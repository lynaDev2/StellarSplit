import { Injectable, Logger, BadRequestException, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Payment } from '../entities/payment.entity';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { StellarService } from '../stellar/stellar.service';
import { PaymentProcessorService } from './payment-processor.service';
import { PaymentGateway } from '../websocket/payment.gateway';

@Injectable()
export class PaymentsService {
  private readonly logger = new Logger(PaymentsService.name);

  constructor(
    @InjectRepository(Payment) private paymentRepository: Repository<Payment>,
    @InjectRepository(Participant) private participantRepository: Repository<Participant>,
    @InjectRepository(Split) private splitRepository: Repository<Split>,
    private readonly stellarService: StellarService,
    private readonly paymentProcessorService: PaymentProcessorService,
    private readonly paymentGateway: PaymentGateway,
  ) { }

  /**
   * Submit a payment with Stellar transaction hash
   */
  async submitPayment(splitId: string, participantId: string, stellarTxHash: string, idempotencyKey?: string, externalReference?: string) {
    return await this.paymentProcessorService.processPaymentSubmission({
      splitId,
      participantId,
      txHash: stellarTxHash,
      idempotencyKey,
      externalReference,
    });
  }

  /**
   * Verify a Stellar transaction
   */
  async verifyTransaction(txHash: string) {
    return await this.stellarService.verifyTransaction(txHash);
  }

  /**
   * Get payment by transaction hash
   */
  async getPaymentByTxHash(txHash: string) {
    const payment = await this.paymentRepository.findOne({
      where: { txHash },
      relations: ['participant'],
    });

    if (!payment) {
      throw new NotFoundException(`Payment with transaction hash ${txHash} not found`);
    }

    return payment;
  }

  /**
   * Get payments for a specific split
   */
  async getPaymentsBySplitId(splitId: string) {
    return await this.paymentRepository.find({
      where: { splitId },
      order: { createdAt: 'DESC' },
    });
  }

  /**
   * Get payments for a specific participant
   */
  async getPaymentsByParticipantId(participantId: string) {
    return await this.paymentRepository.find({
      where: { participantId },
      order: { createdAt: 'DESC' },
    });
  }

  /**
   * Get payment statistics for a split
   */
  async getPaymentStatsForSplit(splitId: string) {
    const payments = await this.paymentRepository.find({ where: { splitId } });
    const totalPaid = payments.reduce((sum, payment) => sum + Number(payment.amount), 0);

    const split = await this.splitRepository.findOne({ where: { id: splitId } });

    if (!split) {
      throw new NotFoundException(`Split ${splitId} not found`);
    }

    return {
      splitId,
      totalAmount: split.totalAmount,
      totalPaid,
      remainingAmount: Number(split.totalAmount) - totalPaid,
      paymentCount: payments.length,
      status: split.status,
    };
  }

  /**
   * Auto-confirm a payment detected by Horizon polling
   */
  async autoConfirmPayment(txHash: string, memo: string) {
    this.logger.log(`Auto-confirming payment for tx: ${txHash}, memo: ${memo}`);

    // Parse the memo to find split and participant (format "splitId:participantId")
    const [splitId, participantId] = memo.split(':');

    if (!splitId || !participantId) {
      this.logger.warn(`Invalid memo format: ${memo}`);
      return;
    }

    const participant = await this.participantRepository.findOne({
      where: { splitId, id: participantId },
    });

    if (!participant) {
      this.logger.warn(`Participant ${participantId} not found in split ${splitId}`);
      return;
    }

    const result = await this.paymentProcessorService.processPaymentSubmission({
      splitId,
      participantId,
      txHash,
    });

    this.paymentGateway.emitPaymentStatusUpdate(splitId, {
      type: 'PAYMENT_CONFIRMED',
      participantId,
      txHash,
      status: 'paid',
    });

    return result;
  }
}