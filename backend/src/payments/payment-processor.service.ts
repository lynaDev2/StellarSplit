import {
  Injectable,
  Logger,
  BadRequestException,
  NotFoundException,
  ConflictException,
  Optional,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, DataSource } from "typeorm";
import { StellarService } from "../stellar/stellar.service";
import { PaymentGateway } from "../websocket/payment.gateway";
import {
  Payment,
  PaymentProcessingStatus,
  PaymentSettlementStatus,
} from "../entities/payment.entity";
import { Participant } from "../entities/participant.entity";
import { Split } from "../entities/split.entity";
import { EmailService } from "../email/email.service";
import { MultiCurrencyService } from "../multi-currency/multi-currency.service";
import { EventsGateway } from "../gateway/events.gateway";
import { AnalyticsService } from "@/analytics/analytics.service";
import * as crypto from "crypto";

/**
 * Result of processing a payment submission
 */
export interface PaymentSubmissionResult {
  success: boolean;
  message: string;
  paymentId?: string;
  isDuplicate?: boolean;
  idempotencyKey?: string;
}

/**
 * Options for processing a payment
 */
export interface ProcessPaymentOptions {
  splitId: string;
  participantId: string;
  txHash: string;
  idempotencyKey?: string;
  externalReference?: string;
  isRetry?: boolean;
  maxRetries?: number;
}

/**
 * Configuration for payment processing
 */
export interface PaymentProcessorConfig {
  maxReconciliationAttempts: number;
  reconciliationTimeoutMinutes: number;
  stalePaymentThresholdMinutes: number;
  enableIdempotencyChecks: boolean;
}

const DEFAULT_CONFIG: PaymentProcessorConfig = {
  maxReconciliationAttempts: 5,
  reconciliationTimeoutMinutes: 30,
  stalePaymentThresholdMinutes: 60,
  enableIdempotencyChecks: true,
};

@Injectable()
export class PaymentProcessorService {
  private readonly logger = new Logger(PaymentProcessorService.name);
  private readonly config: PaymentProcessorConfig;

  constructor(
    private readonly stellarService: StellarService,
    private readonly paymentGateway: PaymentGateway,
    private readonly eventsGateway: EventsGateway,
    @InjectRepository(Payment) private paymentRepository: Repository<Payment>,
    @InjectRepository(Participant)
    private participantRepository: Repository<Participant>,
    @InjectRepository(Split) private splitRepository: Repository<Split>,
    private readonly emailService: EmailService,
    private readonly multiCurrencyService: MultiCurrencyService,
    private readonly dataSource: DataSource,
    @Optional() private readonly analyticsService?: AnalyticsService,
    @Optional() private readonly customConfig?: Partial<PaymentProcessorConfig>,
  ) {
    this.config = { ...DEFAULT_CONFIG, ...customConfig };
  }

  /**
   * Generate an idempotency key for a payment
   */
  generateIdempotencyKey(
    splitId: string,
    participantId: string,
    txHash: string,
  ): string {
    const payload = `${splitId}:${participantId}:${txHash}`;
    return crypto.createHash("sha256").update(payload).digest("hex");
  }

  /**
   * Process a payment submission with idempotency and transaction support
   * @param options Payment processing options
   */
  async processPaymentSubmission(
    options: ProcessPaymentOptions,
  ): Promise<PaymentSubmissionResult> {
    const { splitId, participantId, txHash, idempotencyKey, externalReference, isRetry } =
      options;

    this.logger.log(
      `Processing payment submission for split ${splitId}, participant ${participantId}, tx ${txHash}`,
    );

    // Generate idempotency key if not provided
    const key = idempotencyKey || this.generateIdempotencyKey(splitId, participantId, txHash);

    // Start a database transaction for atomic operations
    const queryRunner = this.dataSource.createQueryRunner();
    await queryRunner.connect();
    await queryRunner.startTransaction();

    try {
      // Check for existing payment with idempotency key
      const existingByKey = await queryRunner.manager.findOne(Payment, {
        where: { idempotencyKey: key },
      });

      if (existingByKey) {
        this.logger.warn(`Duplicate payment detected via idempotency key: ${key}`);
        await queryRunner.rollbackTransaction();
        return {
          success: true,
          message: "Payment already processed",
          paymentId: existingByKey.id,
          isDuplicate: true,
          idempotencyKey: key,
        };
      }

      // Check for duplicate txHash (legacy check)
      const existingByTxHash = await queryRunner.manager.findOne(Payment, {
        where: { txHash },
      });

      if (existingByTxHash) {
        this.logger.warn(`Duplicate payment detected via txHash: ${txHash}`);
        await queryRunner.rollbackTransaction();
        return {
          success: true,
          message: "Payment with this transaction hash already exists",
          paymentId: existingByTxHash.id,
          isDuplicate: true,
          idempotencyKey: key,
        };
      }

      // Verify the transaction on Stellar network
      const verificationResult = await this.stellarService.verifyTransaction(txHash);

      if (!verificationResult || !verificationResult.valid) {
        await queryRunner.rollbackTransaction();
        throw new BadRequestException(
          "Invalid or unsuccessful Stellar transaction",
        );
      }

      // Get the participant record with lock
      const participant = await queryRunner.manager.findOne(Participant, {
        where: { id: participantId, splitId },
        lock: { mode: "pessimistic_write" },
      });

      if (!participant) {
        await queryRunner.rollbackTransaction();
        throw new NotFoundException(
          `Participant ${participantId} not found for split ${splitId}`,
        );
      }

      // Get split to check preferred currency
      const split = await queryRunner.manager.findOne(Split, {
        where: { id: splitId },
        lock: { mode: "pessimistic_write" },
      });

      if (!split) {
        await queryRunner.rollbackTransaction();
        throw new NotFoundException(`Split ${splitId} not found`);
      }

      // Check if split is frozen
      if (split.isFrozen) {
        await queryRunner.rollbackTransaction();
        throw new ConflictException(
          "Split is frozen due to an active dispute",
        );
      }

      // Determine the paid asset (from path payment source or regular payment)
      const paidAsset =
        verificationResult.isPathPayment && verificationResult.sourceAsset
          ? verificationResult.sourceAsset
          : verificationResult.asset;

      const paidAmount =
        verificationResult.isPathPayment && verificationResult.sourceAmount
          ? verificationResult.sourceAmount
          : verificationResult.amount;

      // Process multi-currency payment if needed
      let receivedAmount = verificationResult.amount;
      let receivedAsset = verificationResult.asset;
      let multiCurrencyResult = null;

      // Check if conversion is needed (paid asset differs from received asset)
      if (verificationResult.isPathPayment || paidAsset !== receivedAsset) {
        try {
          multiCurrencyResult =
            await this.multiCurrencyService.processMultiCurrencyPayment({
              splitId,
              participantId,
              txHash,
              paidAsset,
              paidAmount,
              receivedAsset: (split as any).preferredCurrency || receivedAsset,
              slippageTolerance: 0.01,
            });

          receivedAmount = multiCurrencyResult.receivedAmount;
          receivedAsset = multiCurrencyResult.receivedAsset;
        } catch (error: any) {
          this.logger.warn(
            `Multi-currency processing failed, using direct payment: ${error.message}`,
          );
        }
      }

      // Determine payment status based on amount
      let paymentStatus: PaymentProcessingStatus;
      let settlementStatus: PaymentSettlementStatus;

      if (receivedAmount < participant.amountOwed) {
        paymentStatus = PaymentProcessingStatus.PARTIAL;
        settlementStatus = PaymentSettlementStatus.CONFIRMED;
      } else if (receivedAmount > participant.amountOwed) {
        paymentStatus = PaymentProcessingStatus.CONFIRMED;
        settlementStatus = PaymentSettlementStatus.CONFIRMED;
      } else {
        paymentStatus = PaymentProcessingStatus.CONFIRMED;
        settlementStatus = PaymentSettlementStatus.CONFIRMED;
      }

      // Create payment record with idempotency key
      const payment = queryRunner.manager.create(Payment, {
        idempotencyKey: key,
        splitId,
        participantId,
        txHash,
        amount: receivedAmount,
        asset: receivedAsset,
        status: paymentStatus,
        settlementStatus,
        lastSettlementCheck: new Date(),
        reconciliationAttempts: 0,
        maxReconciliationAttempts: this.config.maxReconciliationAttempts,
        notificationsSent: false,
        processedAt: new Date(),
        externalReference,
      });

      const savedPayment = await queryRunner.manager.save(Payment, payment);

      // Update participant's paid amount and status
      const newAmountPaid = participant.amountPaid + receivedAmount;
      let participantStatus: "pending" | "paid" | "partial" = "partial";

      if (newAmountPaid >= participant.amountOwed) {
        participantStatus = "paid";
      } else if (newAmountPaid === 0) {
        participantStatus = "pending";
      }

      await queryRunner.manager.update(Participant, { id: participantId }, {
        amountPaid: newAmountPaid,
        status: participantStatus,
      });

      // Update split's total paid amount
      await this.updateSplitAmountPaidTransactional(queryRunner, splitId);

      // Commit the transaction
      await queryRunner.commitTransaction();

      // Send notifications (outside transaction)
      await this.sendPaymentNotifications(
        participantId,
        splitId,
        paymentStatus,
        {
          txHash,
          amount: receivedAmount,
          asset: receivedAsset,
        },
      );

      // Invalidate analytics cache
      await this.invalidateAnalyticsCache(participant.userId);

      const statusMessage =
        paymentStatus === PaymentProcessingStatus.PARTIAL
          ? `Partial payment received. Amount: ${paidAmount} ${paidAsset}${multiCurrencyResult?.requiresConversion ? ` (converted to ${receivedAmount} ${receivedAsset})` : ""}. Expected: ${participant.amountOwed}`
          : paymentStatus === PaymentProcessingStatus.CONFIRMED && receivedAmount > participant.amountOwed
          ? `Payment received with overpayment. Amount: ${paidAmount} ${paidAsset}${multiCurrencyResult?.requiresConversion ? ` (converted to ${receivedAmount} ${receivedAsset})` : ""}. Expected: ${participant.amountOwed}`
          : `Payment confirmed. Amount: ${paidAmount} ${paidAsset}${multiCurrencyResult?.requiresConversion ? ` (converted to ${receivedAmount} ${receivedAsset})` : ""}`;

      return {
        success: true,
        message: statusMessage,
        paymentId: savedPayment.id,
        idempotencyKey: key,
      };
    } catch (error: any) {
      await queryRunner.rollbackTransaction();
      this.logger.error(
        `Error processing payment submission: ${error.message}`,
        error.stack,
      );
      throw error;
    } finally {
      await queryRunner.release();
    }
  }

  /**
   * Handle webhook replay of payment events
   * This ensures idempotent processing of webhook events
   */
  async handleWebhookReplay(
    txHash: string,
    externalReference: string,
  ): Promise<PaymentSubmissionResult> {
    this.logger.log(`Handling webhook replay for tx: ${txHash}, ref: ${externalReference}`);

    // Check if we already processed this
    const existingPayment = await this.paymentRepository.findOne({
      where: { txHash },
    });

    if (existingPayment) {
      // If already processed and notifications sent, return success
      if (existingPayment.notificationsSent) {
        return {
          success: true,
          message: "Payment already processed and notifications sent",
          paymentId: existingPayment.id,
          isDuplicate: true,
        };
      }

      // If not notifications sent, resend them
      if (!existingPayment.notificationsSent) {
        await this.resendNotifications(existingPayment);
        return {
          success: true,
          message: "Notifications resent for existing payment",
          paymentId: existingPayment.id,
        };
      }
    }

    // For webhook replays without existing payment, we need to find the split/participant
    // This requires additional context - return error with guidance
    throw new BadRequestException(
      "Cannot process webhook replay without existing payment record. Provide idempotency key for original submission.",
    );
  }

  /**
   * Retry a failed payment
   */
  async retryPayment(
    paymentId: string,
    newTxHash: string,
  ): Promise<PaymentSubmissionResult> {
    const payment = await this.paymentRepository.findOne({
      where: { id: paymentId },
      relations: ["participant", "participant.split"],
    });

    if (!payment) {
      throw new NotFoundException(`Payment ${paymentId} not found`);
    }

    if (payment.status !== PaymentProcessingStatus.FAILED) {
      throw new ConflictException(
        "Only failed payments can be retried",
      );
    }

    // Generate new idempotency key for the retry
    const newIdempotencyKey = this.generateIdempotencyKey(
      payment.splitId,
      payment.participantId,
      newTxHash,
    );

    return this.processPaymentSubmission({
      splitId: payment.splitId,
      participantId: payment.participantId,
      txHash: newTxHash,
      idempotencyKey: newIdempotencyKey,
      isRetry: true,
    });
  }

  /**
   * Update split amount paid within a transaction
   */
  private async updateSplitAmountPaidTransactional(
    queryRunner: any,
    splitId: string,
  ): Promise<void> {
    // Calculate total amount paid by summing all participants' paid amounts
    const participants = await queryRunner.manager.find(Participant, {
      where: { splitId },
    });

    const totalPaid = participants.reduce(
      (sum: number, participant: Participant) =>
        sum + Number(participant.amountPaid || 0),
      0,
    );

    // Get the split to update
    const split = await queryRunner.manager.findOne(Split, {
      where: { id: splitId },
    });

    if (!split) {
      throw new NotFoundException(`Split ${splitId} not found`);
    }

    // Determine split status based on total paid vs total amount
    let status: "active" | "completed" | "partial" = "active";
    if (Number(totalPaid) >= Number(split.totalAmount)) {
      status = "completed";
    } else if (Number(totalPaid) > 0) {
      status = "partial";
    }

    await queryRunner.manager.update(
      Split,
      { id: splitId },
      {
        amountPaid: totalPaid,
        status,
      },
    );

    // Send split update notification
    this.eventsGateway.emitSplitUpdated(splitId, {
      splitId,
      status,
      amountPaid: totalPaid,
      timestamp: new Date().toISOString(),
    });

    if (status === "completed") {
      this.sendSplitCompletedNotification(splitId);
    }
  }

  /**
   * Send payment notifications
   */
  private async sendPaymentNotifications(
    participantId: string,
    splitId: string,
    status: PaymentProcessingStatus,
    data: { txHash: string; amount: number; asset: string },
  ): Promise<void> {
    const notificationType =
      status === PaymentProcessingStatus.PARTIAL
        ? "partial_payment_received"
        : "payment_confirmed";

    // Emit to WebSocket gateway
    const roomId = `participant_${participantId}`;
    this.paymentGateway.emitPaymentNotification(roomId, {
      type: notificationType,
      data,
      timestamp: new Date(),
    });
    this.eventsGateway.emitPaymentReceived(splitId, {
      participantId,
      type: notificationType,
      ...data,
      timestamp: new Date().toISOString(),
    });

    this.logger.log(
      `Sending payment notification for participant ${participantId}: ${notificationType}`,
    );

    // Trigger Email Notification
    await this.triggerPaymentConfirmationEmail(participantId, {
      amount: data.amount,
      splitId,
      txHash: data.txHash,
    });
  }

  /**
   * Resend notifications for an existing payment
   */
  private async resendNotifications(payment: Payment): Promise<void> {
    const notificationType =
      payment.status === PaymentProcessingStatus.PARTIAL
        ? "partial_payment_received"
        : "payment_confirmed";

    this.paymentGateway.emitPaymentNotification(`participant_${payment.participantId}`, {
      type: notificationType,
      data: {
        txHash: payment.txHash,
        amount: payment.amount,
        asset: payment.asset,
      },
      timestamp: new Date(),
    });

    this.eventsGateway.emitPaymentReceived(payment.splitId, {
      participantId: payment.participantId,
      type: notificationType,
      txHash: payment.txHash,
      amount: payment.amount,
      asset: payment.asset,
      timestamp: new Date().toISOString(),
    });

    // Update notification sent flag
    await this.paymentRepository.update(payment.id, {
      notificationsSent: true,
    });
  }

  /**
   * Send split completion notification
   */
  private sendSplitCompletedNotification(splitId: string): void {
    const roomId = `split_${splitId}`;
    this.paymentGateway.emitSplitCompletion(roomId, {
      splitId,
      status: "completed",
      timestamp: new Date(),
    });
    this.eventsGateway.emitSplitUpdated(splitId, {
      splitId,
      status: "completed",
      timestamp: new Date().toISOString(),
    });

    this.logger.log(`Sending split completed notification for split ${splitId}`);

    this.triggerSplitCompletedEmail(splitId);
  }

  /**
   * Trigger payment confirmation email
   */
  private async triggerPaymentConfirmationEmail(
    participantId: string,
    data: { amount: number; splitId: string; txHash: string },
  ) {
    try {
      const participant = await this.participantRepository.findOne({
        where: { id: participantId },
      });

      const split = await this.splitRepository.findOne({
        where: { id: data.splitId },
      });

      if (participant) {
        const user = await this.emailService.getUser(participant.userId);
        if (user) {
          await this.emailService.sendPaymentConfirmation(user.email, {
            amount: data.amount,
            splitDescription: split?.description || "Payment",
            txHash: data.txHash,
          });
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to send payment confirmation email: ${error}`);
    }
  }

  /**
   * Trigger split completed email
   */
  private triggerSplitCompletedEmail(splitId: string): void {
    // Implementation would send emails to all participants
    this.logger.log(`Triggering split completed email for split ${splitId}`);
  }

  /**
   * Invalidate analytics cache
   */
  private async invalidateAnalyticsCache(userId: string): Promise<void> {
    try {
      if (this.analyticsService) {
        await this.analyticsService.invalidateUserCache(userId);
        this.analyticsService.refreshMaterializedViewsNow();
      }
    } catch (err) {
      this.logger.warn(
        "Failed to notify analytics service about payment",
        err,
      );
    }
  }

  /**
   * Generate a unique ID
   */
  private generateId(): string {
    return crypto.randomUUID();
  }
}
