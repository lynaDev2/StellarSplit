import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, LessThan, MoreThan, In } from "typeorm";
import { Cron, CronExpression } from "@nestjs/schedule";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import {
  Payment,
  PaymentSettlementStatus,
  PaymentProcessingStatus,
} from "../entities/payment.entity";
import { Participant } from "../entities/participant.entity";
import { Split } from "../entities/split.entity";
import { StellarService } from "../stellar/stellar.service";
import { EventsGateway } from "../gateway/events.gateway";

/**
 * Configuration for reconciliation
 */
export interface ReconciliationConfig {
  staleThresholdMinutes: number;
  maxReconciliationAttempts: number;
  reconciliationBatchSize: number;
  retryDelayMs: number;
}

const DEFAULT_CONFIG: ReconciliationConfig = {
  staleThresholdMinutes: 60,
  maxReconciliationAttempts: 5,
  reconciliationBatchSize: 50,
  retryDelayMs: 5000,
};

/**
 * Result of a reconciliation operation
 */
export interface ReconciliationResult {
  paymentId: string;
  txHash: string;
  previousStatus: PaymentSettlementStatus;
  newStatus: PaymentSettlementStatus;
  onChainValid: boolean;
  onChainAmount?: number;
  error?: string;
}

/**
 * Service for reconciling payment states with on-chain data
 * Handles background reconciliation, stale payment detection, and settlement state management
 */
@Injectable()
export class PaymentReconciliationService implements OnModuleInit {
  private readonly logger = new Logger(PaymentReconciliationService.name);
  private readonly config: ReconciliationConfig;

  constructor(
    @InjectRepository(Payment) private paymentRepository: Repository<Payment>,
    @InjectRepository(Participant)
    private participantRepository: Repository<Participant>,
    @InjectRepository(Split) private splitRepository: Repository<Split>,
    private readonly stellarService: StellarService,
    private readonly eventsGateway: EventsGateway,
    @InjectQueue("payment-reconciliation")
    private readonly reconciliationQueue: Queue,
    @InjectQueue("payment-settlement")
    private readonly settlementQueue: Queue,
    private readonly customConfig?: Partial<ReconciliationConfig>,
  ) {
    this.config = { ...DEFAULT_CONFIG, ...customConfig };
  }

  async onModuleInit() {
    this.logger.log(
      `PaymentReconciliationService initialized with config: ${JSON.stringify(this.config)}`,
    );
  }

  /**
   * Cron job to reconcile pending payments
   * Runs every 5 minutes to check on-chain status of submitted payments
   */
  @Cron(CronExpression.EVERY_5_MINUTES)
  async reconcilePendingPayments(): Promise<void> {
    this.logger.log("Starting scheduled payment reconciliation");

    try {
      // Find payments that need reconciliation
      const paymentsToReconcile = await this.paymentRepository.find({
        where: [
          {
            settlementStatus: PaymentSettlementStatus.SUBMITTED,
          },
          {
            settlementStatus: PaymentSettlementStatus.RECONCILING,
          },
        ],
        take: this.config.reconciliationBatchSize,
        order: { createdAt: "ASC" },
      });

      this.logger.log(
        `Found ${paymentsToReconcile.length} payments to reconcile`,
      );

      for (const payment of paymentsToReconcile) {
        await this.reconciliationQueue.add(
          "reconcile-payment",
          { paymentId: payment.id },
          {
            attempts: 3,
            backoff: {
              type: "exponential",
              delay: this.config.retryDelayMs,
            },
          },
        );
      }
    } catch (error) {
      this.logger.error(
        "Error in scheduled payment reconciliation",
        error,
      );
    }
  }

  /**
   * Cron job to detect and mark stale payments
   * Runs every 15 minutes to find payments that have been pending too long
   */
  @Cron('0 */15 * * * *')
  async detectStalePayments(): Promise<void> {
    this.logger.log("Starting stale payment detection");

    try {
      const staleThreshold = new Date(
        Date.now() - this.config.staleThresholdMinutes * 60 * 1000,
      );

      // Find payments that are still in SUBMITTED status after threshold
      const stalePayments = await this.paymentRepository.find({
        where: {
          settlementStatus: PaymentSettlementStatus.SUBMITTED,
          lastSettlementCheck: LessThan(staleThreshold),
        },
        take: this.config.reconciliationBatchSize,
      });

      this.logger.log(`Found ${stalePayments.length} stale payments`);

      for (const payment of stalePayments) {
        await this.markPaymentForReview(payment.id, "Payment stale - no confirmation after threshold");
      }
    } catch (error) {
      this.logger.error("Error in stale payment detection", error);
    }
  }

  /**
   * Reconcile a single payment with on-chain data
   * @param paymentId The payment ID to reconcile
   */
  async reconcilePayment(paymentId: string): Promise<ReconciliationResult> {
    this.logger.log(`Reconciling payment: ${paymentId}`);

    const payment = await this.paymentRepository.findOne({
      where: { id: paymentId },
    });

    if (!payment) {
      throw new Error(`Payment not found: ${paymentId}`);
    }

    const previousStatus = payment.settlementStatus;

    // Skip if already confirmed, failed, or in review
    if (
      payment.settlementStatus === PaymentSettlementStatus.CONFIRMED ||
      payment.settlementStatus === PaymentSettlementStatus.FAILED ||
      payment.settlementStatus === PaymentSettlementStatus.REVIEW_REQUIRED
    ) {
      this.logger.warn(
        `Payment ${paymentId} already in final state: ${payment.settlementStatus}`,
      );
      return {
        paymentId,
        txHash: payment.txHash,
        previousStatus,
        newStatus: payment.settlementStatus,
        onChainValid: false,
      };
    }

    // Mark as reconciling
    await this.paymentRepository.update(paymentId, {
      settlementStatus: PaymentSettlementStatus.RECONCILING,
      reconciliationAttempts: payment.reconciliationAttempts + 1,
      lastSettlementCheck: new Date(),
    });

    try {
      // Verify transaction on Stellar
      const verification = await this.stellarService.verifyTransaction(
        payment.txHash,
      );

      if (!verification) {
        // Transaction not found on chain yet - could be pending
        this.logger.warn(
          `Transaction not found on chain: ${payment.txHash}`,
        );
        return {
          paymentId,
          txHash: payment.txHash,
          previousStatus,
          newStatus: PaymentSettlementStatus.SUBMITTED,
          onChainValid: false,
          error: "Transaction not found on chain",
        };
      }

      if (!verification.valid) {
        // Transaction failed on chain
        await this.handleFailedPayment(payment, verification);
        return {
          paymentId,
          txHash: payment.txHash,
          previousStatus,
          newStatus: PaymentSettlementStatus.FAILED,
          onChainValid: false,
          error: "Transaction failed on chain",
        };
      }

      // Transaction successful - update payment status
      await this.handleConfirmedPayment(payment, verification);

      return {
        paymentId,
        txHash: payment.txHash,
        previousStatus,
        newStatus: PaymentSettlementStatus.CONFIRMED,
        onChainValid: true,
        onChainAmount: verification.amount,
      };
    } catch (error: any) {
      this.logger.error(
        `Error reconciling payment ${paymentId}: ${error.message}`,
      );

      // Check if we should mark for review
      const attempts = payment.reconciliationAttempts + 1;
      if (attempts >= this.config.maxReconciliationAttempts) {
        await this.markPaymentForReview(
          paymentId,
          `Max reconciliation attempts reached: ${error.message}`,
        );
        return {
          paymentId,
          txHash: payment.txHash,
          previousStatus,
          newStatus: PaymentSettlementStatus.REVIEW_REQUIRED,
          onChainValid: false,
          error: error.message,
        };
      }

      // Reset to submitted for retry
      await this.paymentRepository.update(paymentId, {
        settlementStatus: PaymentSettlementStatus.SUBMITTED,
      });

      return {
        paymentId,
        txHash: payment.txHash,
        previousStatus,
        newStatus: PaymentSettlementStatus.SUBMITTED,
        onChainValid: false,
        error: error.message,
      };
    }
  }

  /**
   * Handle a confirmed payment
   */
  private async handleConfirmedPayment(
    payment: Payment,
    verification: any,
  ): Promise<void> {
    this.logger.log(
      `Payment confirmed on chain: ${payment.txHash}, amount: ${verification.amount}`,
    );

    // Update payment settlement status
    await this.paymentRepository.update(payment.id, {
      settlementStatus: PaymentSettlementStatus.CONFIRMED,
      amount: verification.amount, // Update with actual on-chain amount
      lastSettlementCheck: new Date(),
    });

    // Emit settlement event
    this.emitSettlementEvent(payment, "confirmed", {
      amount: verification.amount,
      asset: verification.asset,
      onChainTimestamp: verification.timestamp,
    });

    // Queue settlement processing
    await this.settlementQueue.add(
      "process-settlement",
      {
        paymentId: payment.id,
        splitId: payment.splitId,
        participantId: payment.participantId,
      },
      {
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 5000,
        },
      },
    );
  }

  /**
   * Handle a failed payment
   */
  private async handleFailedPayment(
    payment: Payment,
    verification: any,
  ): Promise<void> {
    this.logger.log(`Payment failed on chain: ${payment.txHash}`);

    // Update payment settlement status
    await this.paymentRepository.update(payment.id, {
      settlementStatus: PaymentSettlementStatus.FAILED,
      settlementError: "Transaction failed on Stellar network",
      lastSettlementCheck: new Date(),
    });

    // Update participant status if needed
    await this.participantRepository.update(payment.participantId, {
      status: "pending",
    });

    // Emit settlement event
    this.emitSettlementEvent(payment, "failed", {
      error: "Transaction failed on Stellar network",
    });
  }

  /**
   * Mark a payment for manual review
   */
  async markPaymentForReview(
    paymentId: string,
    reason: string,
  ): Promise<void> {
    this.logger.warn(`Marking payment ${paymentId} for review: ${reason}`);

    await this.paymentRepository.update(paymentId, {
      settlementStatus: PaymentSettlementStatus.REVIEW_REQUIRED,
      settlementError: reason,
      lastSettlementCheck: new Date(),
    });

    const payment = await this.paymentRepository.findOne({
      where: { id: paymentId },
    });

    if (payment) {
      this.emitSettlementEvent(payment, "review_required", {
        reason,
      });
    }
  }

  /**
   * Emit settlement state change event
   */
  private emitSettlementEvent(
    payment: Payment,
    eventType: string,
    additionalData: Record<string, any>,
  ): void {
    const eventData = {
      paymentId: payment.id,
      splitId: payment.splitId,
      txHash: payment.txHash,
      eventType,
      settlementStatus: payment.settlementStatus,
      timestamp: new Date().toISOString(),
      ...additionalData,
    };

    // Emit to split room
    this.eventsGateway.emitSplitUpdated(payment.splitId, {
      type: "settlement_update",
      ...eventData,
    });

    // Emit to participant room (without duplicate participantId in eventData)
    this.eventsGateway.emitPaymentReceived(payment.splitId, {
      type: eventType,
      ...eventData,
    });

    this.logger.log(
      `Emitted settlement event: ${eventType} for payment ${payment.id}`,
    );
  }

  /**
   * Get payments requiring review
   */
  async getPaymentsForReview(): Promise<Payment[]> {
    return this.paymentRepository.find({
      where: {
        settlementStatus: PaymentSettlementStatus.REVIEW_REQUIRED,
      },
      order: { updatedAt: "DESC" },
    });
  }

  /**
   * Manually resolve a payment in review
   */
  async resolvePaymentFromReview(
    paymentId: string,
    resolution: "confirm" | "fail" | "retry",
    adminNote?: string,
  ): Promise<void> {
    const payment = await this.paymentRepository.findOne({
      where: { id: paymentId },
    });

    if (!payment) {
      throw new Error(`Payment not found: ${paymentId}`);
    }

    if (payment.settlementStatus !== PaymentSettlementStatus.REVIEW_REQUIRED) {
      throw new Error("Payment is not in review required status");
    }

    switch (resolution) {
      case "confirm":
        await this.paymentRepository.update(paymentId, {
          settlementStatus: PaymentSettlementStatus.CONFIRMED,
          settlementError: `Manually confirmed by admin: ${adminNote || "No note"}`,
        });
        break;

      case "fail":
        await this.paymentRepository.update(paymentId, {
          settlementStatus: PaymentSettlementStatus.FAILED,
          settlementError: `Manually failed by admin: ${adminNote || "No note"}`,
        });
        break;

      case "retry":
        await this.paymentRepository.update(paymentId, {
          settlementStatus: PaymentSettlementStatus.SUBMITTED,
          settlementError: `Manually queued for retry: ${adminNote || "No note"}`,
          reconciliationAttempts: 0,
        });
        // Queue for reconciliation
        await this.reconciliationQueue.add(
          "reconcile-payment",
          { paymentId },
          { attempts: 3 },
        );
        break;
    }

    this.logger.log(
      `Payment ${paymentId} resolved from review: ${resolution}`,
    );
  }

  /**
   * Get reconciliation statistics
   */
  async getReconciliationStats(): Promise<{
    total: number;
    submitted: number;
    confirmed: number;
    failed: number;
    reviewRequired: number;
    stale: number;
  }> {
    const staleThreshold = new Date(
      Date.now() - this.config.staleThresholdMinutes * 60 * 1000,
    );

    const [total, submitted, confirmed, failed, reviewRequired, stale] =
      await Promise.all([
        this.paymentRepository.count(),
        this.paymentRepository.count({
          where: { settlementStatus: PaymentSettlementStatus.SUBMITTED },
        }),
        this.paymentRepository.count({
          where: { settlementStatus: PaymentSettlementStatus.CONFIRMED },
        }),
        this.paymentRepository.count({
          where: { settlementStatus: PaymentSettlementStatus.FAILED },
        }),
        this.paymentRepository.count({
          where: { settlementStatus: PaymentSettlementStatus.REVIEW_REQUIRED },
        }),
        this.paymentRepository.count({
          where: {
            settlementStatus: PaymentSettlementStatus.SUBMITTED,
            lastSettlementCheck: LessThan(staleThreshold),
          },
        }),
      ]);

    return {
      total,
      submitted,
      confirmed,
      failed,
      reviewRequired,
      stale,
    };
  }
}
