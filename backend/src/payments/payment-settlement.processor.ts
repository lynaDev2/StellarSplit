import { Injectable, Logger } from "@nestjs/common";
import { Processor, Process } from "@nestjs/bull";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Payment, PaymentSettlementStatus } from "../entities/payment.entity";
import { Participant } from "../entities/participant.entity";
import { Split } from "../entities/split.entity";
import { EmailService } from "../email/email.service";
import { EventsGateway } from "../gateway/events.gateway";

/**
 * Job data for settlement processing
 */
interface SettlementJobData {
  paymentId: string;
  splitId: string;
  participantId: string;
}

/**
 * Queue processor for payment settlement jobs
 */
@Processor("payment-settlement")
@Injectable()
export class PaymentSettlementProcessor {
  private readonly logger = new Logger(PaymentSettlementProcessor.name);

  constructor(
    @InjectRepository(Payment) private paymentRepository: Repository<Payment>,
    @InjectRepository(Participant)
    private participantRepository: Repository<Participant>,
    @InjectRepository(Split) private splitRepository: Repository<Split>,
    private readonly emailService: EmailService,
    private readonly eventsGateway: EventsGateway,
  ) {}

  /**
   * Process a settlement after on-chain confirmation
   */
  @Process("process-settlement")
  async handleSettlement(job: Job<SettlementJobData>): Promise<void> {
    const { paymentId, splitId, participantId } = job.data;
    this.logger.log(`Processing settlement for payment: ${paymentId}`);

    try {
      // Get payment details
      const payment = await this.paymentRepository.findOne({
        where: { id: paymentId },
      });

      if (!payment) {
        this.logger.error(`Payment not found: ${paymentId}`);
        return;
      }

      // Verify payment is confirmed
      if (payment.settlementStatus !== PaymentSettlementStatus.CONFIRMED) {
        this.logger.warn(
          `Payment ${paymentId} is not confirmed, skipping settlement`,
        );
        return;
      }

      // Get participant details
      const participant = await this.participantRepository.findOne({
        where: { id: participantId },
      });

      if (!participant) {
        this.logger.error(`Participant not found: ${participantId}`);
        return;
      }

      // Get split details
      const split = await this.splitRepository.findOne({
        where: { id: splitId },
        relations: ["participants"],
      });

      if (!split) {
        this.logger.error(`Split not found: ${splitId}`);
        return;
      }

      // Check if split is now complete
      await this.checkAndUpdateSplitCompletion(splitId);

      // Send settlement confirmation
      await this.sendSettlementConfirmation(payment, participant, split);

      // Emit settlement event
      this.eventsGateway.emitSplitUpdated(splitId, {
        type: "settlement_completed",
        paymentId,
        participantId,
        amount: payment.amount,
        timestamp: new Date().toISOString(),
      });

      this.logger.log(
        `Settlement processed for payment: ${paymentId}`,
      );
    } catch (error: any) {
      this.logger.error(
        `Failed to process settlement for ${paymentId}: ${error.message}`,
      );
      throw error;
    }
  }

  /**
   * Check if split is complete after payment and update status
   */
  private async checkAndUpdateSplitCompletion(splitId: string): Promise<void> {
    const split = await this.splitRepository.findOne({
      where: { id: splitId },
      relations: ["participants"],
    });

    if (!split) return;

    // Check if all participants are paid
    const allPaid = split.participants.every(
      (p: Participant) => p.status === "paid" || Number(p.amountPaid) >= Number(p.amountOwed),
    );

    if (allPaid && split.status !== "completed") {
      await this.splitRepository.update(splitId, {
        status: "completed",
      });

      // Emit split completed event
      this.eventsGateway.emitSplitUpdated(splitId, {
        type: "split_completed",
        splitId,
        timestamp: new Date().toISOString(),
      });

      this.logger.log(`Split ${splitId} marked as completed`);
    }
  }

  /**
   * Send settlement confirmation email
   */
  private async sendSettlementConfirmation(
    payment: Payment,
    participant: Participant,
    split: Split,
  ): Promise<void> {
    try {
      const user = await this.emailService.getUser(participant.userId);
      if (user) {
        // In a real implementation, we'd send a settlement-specific email
        // For now, we'll just log it
        this.logger.log(
          `Would send settlement confirmation to ${user.email} for payment ${payment.id}`,
        );
      }
    } catch (error) {
      this.logger.warn(
        `Failed to send settlement confirmation: ${error}`,
      );
    }
  }
}