import {
  Injectable,
  Logger,
  BadRequestException,
  NotFoundException,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Asset } from '@stellar/stellar-sdk';
import { ExchangeRateTrackerService } from './exchange-rate-tracker.service';
import { PathPaymentService } from './path-payment.service';
import { MultiCurrencyPayment } from './entities/multi-currency-payment.entity';
import { Payment, PaymentProcessingStatus } from '../entities/payment.entity';
import { Split } from '../entities/split.entity';
import { Participant } from '../entities/participant.entity';

export interface ProcessMultiCurrencyPaymentParams {
  splitId: string;
  participantId: string;
  txHash: string;
  paidAsset: string;
  paidAmount: number;
  receivedAsset?: string; // If not provided, uses split's preferred currency
  slippageTolerance?: number; // Default 1%
}

export interface MultiCurrencyPaymentResult {
  success: boolean;
  paymentId: string;
  multiCurrencyPaymentId?: string;
  paidAsset: string;
  paidAmount: number;
  receivedAsset: string;
  receivedAmount: number;
  exchangeRate: number;
  slippage?: number;
  pathPaymentTxHash?: string;
  requiresConversion: boolean;
  message: string;
}

@Injectable()
export class MultiCurrencyService {
  private readonly logger = new Logger(MultiCurrencyService.name);

  constructor(
    @InjectRepository(Payment)
    private paymentRepository: Repository<Payment>,
    @InjectRepository(Split)
    private splitRepository: Repository<Split>,
    @InjectRepository(Participant)
    private participantRepository: Repository<Participant>,
    @InjectRepository(MultiCurrencyPayment)
    private multiCurrencyPaymentRepository: Repository<MultiCurrencyPayment>,
    private readonly exchangeRateTracker: ExchangeRateTrackerService,
    private readonly pathPaymentService: PathPaymentService,
  ) {}

  /**
   * Process a multi-currency payment
   * Handles conversion if needed and tracks exchange rates
   */
  async processMultiCurrencyPayment(
    params: ProcessMultiCurrencyPaymentParams,
  ): Promise<MultiCurrencyPaymentResult> {
    try {
      const {
        splitId,
        participantId,
        txHash,
        paidAsset,
        paidAmount,
        receivedAsset,
        slippageTolerance = 0.01, // 1% default
      } = params;

      this.logger.log(
        `Processing multi-currency payment: ${paidAmount} ${paidAsset} for split ${splitId}`,
      );

      // Get split to determine preferred currency
      const split = await this.splitRepository.findOne({
        where: { id: splitId },
      });

      if (!split) {
        throw new NotFoundException(`Split ${splitId} not found`);
      }

      // Get participant
      const participant = await this.participantRepository.findOne({
        where: { id: participantId, splitId },
      });

      if (!participant) {
        throw new NotFoundException(
          `Participant ${participantId} not found for split ${splitId}`,
        );
      }

      // Determine target asset (preferred currency or provided receivedAsset)
      const targetAsset =
        receivedAsset || (split as any).preferredCurrency || 'XLM';

      // Parse assets
      const paidAssetObj = this.exchangeRateTracker.parseAsset(paidAsset);
      const targetAssetObj = this.exchangeRateTracker.parseAsset(targetAsset);

      // Check if conversion is needed
      const requiresConversion = !this.areAssetsEqual(
        paidAssetObj,
        targetAssetObj,
      );

      let receivedAmount = paidAmount;
      let exchangeRate = 1.0;
      let pathPaymentTxHash: string | null = null;
      let slippage: number | undefined;

      if (requiresConversion) {
        this.logger.log(
          `Conversion required: ${paidAsset} -> ${targetAsset}`,
        );

        // Get exchange rate
        const rateInfo = await this.exchangeRateTracker.getBestExchangeRate(
          paidAssetObj,
          targetAssetObj,
          paidAmount,
          slippageTolerance,
        );

        exchangeRate = rateInfo.rate;
        receivedAmount = rateInfo.destinationAmount;

        // Note: In a real implementation, the path payment would be executed
        // by the participant's wallet. Here we're tracking what happened.
        // The txHash might be the path payment transaction hash if conversion was done on-chain
        pathPaymentTxHash = txHash; // Assuming the provided txHash is the path payment

        // Calculate slippage if we have expected vs actual
        if (rateInfo.destinationAmount) {
          const expectedAmount = paidAmount * rateInfo.rate;
          slippage = this.pathPaymentService.calculateSlippage(
            expectedAmount,
            rateInfo.destinationAmount,
          );
        }

        this.logger.log(
          `Conversion rate: ${exchangeRate} (${paidAmount} ${paidAsset} = ${receivedAmount} ${targetAsset})`,
        );
      } else {
        this.logger.log(
          `No conversion needed: payment already in ${targetAsset}`,
        );
      }

      // Find or create payment record
      let payment = await this.paymentRepository.findOne({
        where: { txHash },
      });

      if (!payment) {
        // Payment record should be created by payment processor
        // But if it doesn't exist, we'll need to create it
        this.logger.warn(
          `Payment record not found for txHash ${txHash}, creating one`,
        );
        payment = this.paymentRepository.create({
          splitId,
          participantId,
          txHash,
          amount: receivedAmount, // Use converted amount
          asset: targetAsset,
          status: PaymentProcessingStatus.CONFIRMED,
        });
        payment = await this.paymentRepository.save(payment);
      }

      // Track exchange rate
      const multiCurrencyPayment =
        await this.exchangeRateTracker.trackExchangeRate(
          payment.id,
          paidAsset,
          paidAmount,
          targetAsset,
          receivedAmount,
          exchangeRate,
          pathPaymentTxHash || undefined,
          requiresConversion ? paidAmount * exchangeRate : undefined,
        );

      this.logger.log(
        `Multi-currency payment processed: ${paidAmount} ${paidAsset} -> ${receivedAmount} ${targetAsset}`,
      );

      return {
        success: true,
        paymentId: payment.id,
        multiCurrencyPaymentId: multiCurrencyPayment.id,
        paidAsset,
        paidAmount,
        receivedAsset: targetAsset,
        receivedAmount,
        exchangeRate,
        slippage,
        pathPaymentTxHash: pathPaymentTxHash || undefined,
        requiresConversion,
        message: `Payment processed: ${paidAmount} ${paidAsset} ${requiresConversion ? `converted to ${receivedAmount} ${targetAsset}` : ''}`,
      };
    } catch (error: any) {
      this.logger.error(
        `Error processing multi-currency payment: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  /**
   * Get path payment transaction for client to sign
   * This builds a path payment transaction that the participant can sign and submit
   */
  async getPathPaymentTransaction(
    splitId: string,
    participantId: string,
    sourceAsset: string,
    destinationAmount: number,
    slippageTolerance: number = 0.01,
  ): Promise<{
    transactionXDR: string;
    sourceAmount: number;
    destinationAmount: number;
    path: string[];
    exchangeRate: number;
    maxSourceAmount: number;
  }> {
    try {
      // Get split to determine preferred currency
      const split = await this.splitRepository.findOne({
        where: { id: splitId },
      });

      if (!split) {
        throw new NotFoundException(`Split ${splitId} not found`);
      }

      // Get participant
      const participant = await this.participantRepository.findOne({
        where: { id: participantId, splitId },
      });

      if (!participant) {
        throw new NotFoundException(
          `Participant ${participantId} not found for split ${splitId}`,
        );
      }

      if (!participant.walletAddress) {
        throw new BadRequestException(
          `Participant ${participantId} does not have a wallet address`,
        );
      }

      // Determine destination asset
      const destinationAsset =
        (split as any).preferredCurrency || 'XLM';

      // Parse assets
      const sourceAssetObj = this.exchangeRateTracker.parseAsset(sourceAsset);
      const destinationAssetObj =
        this.exchangeRateTracker.parseAsset(destinationAsset);

      // Get split creator's wallet address (destination)
      // This should be stored in the split or retrieved from user
      // For now, we'll need to get it from somewhere - assuming it's in split or user entity
      const destinationAccount = (split as any).creatorWalletAddress;
      if (!destinationAccount) {
        // Fallback: try to get from the first participant or use a default
        // In production, this should be properly stored in the split or user entity
        throw new BadRequestException(
          'Split creator wallet address not found. Please set creatorWalletAddress on the split.',
        );
      }

      // Build path payment transaction
      const transactionInfo =
        await this.pathPaymentService.buildPathPaymentTransaction({
          sourceAccount: participant.walletAddress,
          destinationAccount,
          sourceAsset: sourceAssetObj,
          destinationAsset: destinationAssetObj,
          destinationAmount,
          slippageTolerance,
        });

      return {
        transactionXDR: transactionInfo.transactionXDR,
        sourceAmount: transactionInfo.sourceAmount,
        destinationAmount: transactionInfo.destinationAmount,
        path: transactionInfo.path.map((asset) =>
          this.exchangeRateTracker.formatAsset(asset),
        ),
        exchangeRate: transactionInfo.exchangeRate,
        maxSourceAmount: transactionInfo.maxSourceAmount,
      };
    } catch (error: any) {
      this.logger.error(
        `Error getting path payment transaction: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  /**
   * Get multi-currency payment details
   */
  async getMultiCurrencyPayment(
    paymentId: string,
  ): Promise<MultiCurrencyPayment | null> {
    return await this.multiCurrencyPaymentRepository.findOne({
      where: { paymentId },
      relations: ['payment'],
    });
  }

  /**
   * Get supported assets
   */
  getSupportedAssets(): string[] {
    return this.exchangeRateTracker.getSupportedAssets();
  }

  /**
   * Validate asset format
   */
  validateAsset(asset: string): boolean {
    return this.exchangeRateTracker.validateAssetFormat(asset);
  }

  /**
   * Check if two assets are equal
   */
  private areAssetsEqual(asset1: Asset, asset2: Asset): boolean {
    if (asset1.isNative() && asset2.isNative()) {
      return true;
    }

    if (asset1.isNative() || asset2.isNative()) {
      return false;
    }

    return (
      asset1.getCode() === asset2.getCode() &&
      asset1.getIssuer() === asset2.getIssuer()
    );
  }

  /**
   * Generate UUID
   */
  private generateId(): string {
    return (
      Math.random().toString(36).substring(2, 15) +
      Math.random().toString(36).substring(2, 15)
    );
  }
}
