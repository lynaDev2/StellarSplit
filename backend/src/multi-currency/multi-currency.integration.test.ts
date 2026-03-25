/**
 * Multi-Currency Integration Tests
 * Tests for multi-currency payment processing with path payments and exchange rate tracking
 */

import { Test, TestingModule } from '@nestjs/testing';
import { Repository } from 'typeorm';
import { getRepositoryToken } from '@nestjs/typeorm';
import { MultiCurrencyService } from './multi-currency.service';
import { ExchangeRateTrackerService } from './exchange-rate-tracker.service';
import { PathPaymentService } from './path-payment.service';
import { MultiCurrencyPayment } from './entities/multi-currency-payment.entity';
import { Payment, PaymentProcessingStatus } from '../entities/payment.entity';
import { Split } from '../entities/split.entity';
import { Participant } from '../entities/participant.entity';
import { StellarService } from '../stellar/stellar.service';

describe('Multi-Currency Integration Tests', () => {
  let multiCurrencyService: MultiCurrencyService;
  let exchangeRateTracker: ExchangeRateTrackerService;
  let pathPaymentService: PathPaymentService;
  let paymentRepository: Repository<Payment>;
  let splitRepository: Repository<Split>;
  let participantRepository: Repository<Participant>;
  let multiCurrencyPaymentRepository: Repository<MultiCurrencyPayment>;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        MultiCurrencyService,
        ExchangeRateTrackerService,
        PathPaymentService,
        {
          provide: StellarService,
          useValue: {
            verifyTransaction: jest.fn(),
            getAccountDetails: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Payment),
          useClass: Repository,
        },
        {
          provide: getRepositoryToken(Split),
          useClass: Repository,
        },
        {
          provide: getRepositoryToken(Participant),
          useClass: Repository,
        },
        {
          provide: getRepositoryToken(MultiCurrencyPayment),
          useClass: Repository,
        },
      ],
    }).compile();

    multiCurrencyService = module.get<MultiCurrencyService>(MultiCurrencyService);
    exchangeRateTracker = module.get<ExchangeRateTrackerService>(ExchangeRateTrackerService);
    pathPaymentService = module.get<PathPaymentService>(PathPaymentService);
    paymentRepository = module.get<Repository<Payment>>(getRepositoryToken(Payment));
    splitRepository = module.get<Repository<Split>>(getRepositoryToken(Split));
    participantRepository = module.get<Repository<Participant>>(getRepositoryToken(Participant));
    multiCurrencyPaymentRepository = module.get<Repository<MultiCurrencyPayment>>(
      getRepositoryToken(MultiCurrencyPayment),
    );
  });

  describe('ExchangeRateTrackerService', () => {
    it('should parse XLM asset correctly', () => {
      const asset = exchangeRateTracker.parseAsset('XLM');
      expect(asset.isNative()).toBe(true);
    });

    it('should parse asset with issuer correctly', () => {
      const asset = exchangeRateTracker.parseAsset('USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN');
      expect(asset.isNative()).toBe(false);
      expect(asset.getCode()).toBe('USDC');
    });

    it('should format asset correctly', () => {
      const asset = exchangeRateTracker.parseAsset('XLM');
      const formatted = exchangeRateTracker.formatAsset(asset);
      expect(formatted).toBe('XLM');
    });

    it('should validate asset format', () => {
      expect(exchangeRateTracker.validateAssetFormat('XLM')).toBe(true);
      expect(exchangeRateTracker.validateAssetFormat('USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN')).toBe(true);
      expect(exchangeRateTracker.validateAssetFormat('INVALID')).toBe(false);
      expect(exchangeRateTracker.validateAssetFormat('USDC')).toBe(false);
    });

    it('should get supported assets', () => {
      const assets = exchangeRateTracker.getSupportedAssets();
      expect(assets).toContain('XLM');
      expect(assets.length).toBeGreaterThan(0);
    });
  });

  describe('PathPaymentService', () => {
    it('should calculate slippage correctly', () => {
      const slippage = pathPaymentService.calculateSlippage(100, 99);
      expect(slippage).toBe(1); // 1% slippage
    });

    it('should check if slippage is acceptable', () => {
      expect(pathPaymentService.isSlippageAcceptable(100, 99, 0.02)).toBe(true); // 1% slippage, 2% tolerance
      expect(pathPaymentService.isSlippageAcceptable(100, 95, 0.02)).toBe(false); // 5% slippage, 2% tolerance
    });
  });

  describe('MultiCurrencyService', () => {
    const mockSplit: Partial<Split> = {
      id: 'split-1',
      totalAmount: 100,
      preferredCurrency: 'XLM',
      creatorWalletAddress: 'GCREATOR123456789012345678901234567890123456789012345678',
    };

    const mockParticipant: Partial<Participant> = {
      id: 'participant-1',
      splitId: 'split-1',
      amountOwed: 50,
      walletAddress: 'GPARTICIPANT1234567890123456789012345678901234567890123456',
    };

    const mockPayment: Partial<Payment> = {
      id: 'payment-1',
      splitId: 'split-1',
      participantId: 'participant-1',
      txHash: 'tx-hash-123',
      amount: 50,
      asset: 'XLM',
      status: PaymentProcessingStatus.CONFIRMED,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    beforeEach(() => {
      jest.spyOn(splitRepository, 'findOne').mockResolvedValue(mockSplit as Split);
      jest.spyOn(participantRepository, 'findOne').mockResolvedValue(mockParticipant as Participant);
      jest.spyOn(paymentRepository, 'findOne').mockResolvedValue(mockPayment as Payment);
      jest.spyOn(paymentRepository, 'create').mockReturnValue(mockPayment as Payment);
      jest.spyOn(paymentRepository, 'save').mockResolvedValue(mockPayment as Payment);
      jest.spyOn(multiCurrencyPaymentRepository, 'create').mockReturnValue({
        id: 'multi-currency-1',
        paymentId: 'payment-1',
        paidAsset: 'USDC:GA5Z...',
        paidAmount: 50,
        receivedAsset: 'XLM',
        receivedAmount: 100,
        exchangeRate: 2.0,
        pathPaymentTxHash: 'tx-hash-123',
        createdAt: new Date(),
      } as MultiCurrencyPayment);
      jest.spyOn(multiCurrencyPaymentRepository, 'save').mockResolvedValue({
        id: 'multi-currency-1',
        paymentId: 'payment-1',
        paidAsset: 'USDC:GA5Z...',
        paidAmount: 50,
        receivedAsset: 'XLM',
        receivedAmount: 100,
        exchangeRate: 2.0,
        pathPaymentTxHash: 'tx-hash-123',
        createdAt: new Date(),
      } as MultiCurrencyPayment);
    });

    it('should process multi-currency payment with conversion', async () => {
      jest.spyOn(exchangeRateTracker, 'getBestExchangeRate').mockResolvedValue({
        rate: 2.0,
        sourceAmount: 50,
        destinationAmount: 100,
        path: [],
        timestamp: new Date(),
      });

      const result = await multiCurrencyService.processMultiCurrencyPayment({
        splitId: 'split-1',
        participantId: 'participant-1',
        txHash: 'tx-hash-123',
        paidAsset: 'USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
        paidAmount: 50,
      });

      expect(result.success).toBe(true);
      expect(result.requiresConversion).toBe(true);
      expect(result.exchangeRate).toBe(2.0);
      expect(result.receivedAmount).toBe(100);
    });

    it('should process payment without conversion when same asset', async () => {
      jest.spyOn(exchangeRateTracker, 'getBestExchangeRate').mockResolvedValue({
        rate: 1.0,
        sourceAmount: 50,
        destinationAmount: 50,
        path: [],
        timestamp: new Date(),
      });

      const result = await multiCurrencyService.processMultiCurrencyPayment({
        splitId: 'split-1',
        participantId: 'participant-1',
        txHash: 'tx-hash-123',
        paidAsset: 'XLM',
        paidAmount: 50,
      });

      expect(result.success).toBe(true);
      expect(result.requiresConversion).toBe(false);
      expect(result.exchangeRate).toBe(1.0);
    });

    it('should get supported assets', () => {
      const assets = multiCurrencyService.getSupportedAssets();
      expect(assets).toContain('XLM');
      expect(assets.length).toBeGreaterThan(0);
    });

    it('should validate asset format', () => {
      expect(multiCurrencyService.validateAsset('XLM')).toBe(true);
      expect(multiCurrencyService.validateAsset('USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN')).toBe(true);
      expect(multiCurrencyService.validateAsset('INVALID')).toBe(false);
    });

    it('should throw error when split not found', async () => {
      jest.spyOn(splitRepository, 'findOne').mockResolvedValue(null);

      await expect(
        multiCurrencyService.processMultiCurrencyPayment({
          splitId: 'non-existent',
          participantId: 'participant-1',
          txHash: 'tx-hash-123',
          paidAsset: 'XLM',
          paidAmount: 50,
        }),
      ).rejects.toThrow('Split non-existent not found');
    });

    it('should throw error when participant not found', async () => {
      jest.spyOn(participantRepository, 'findOne').mockResolvedValue(null);

      await expect(
        multiCurrencyService.processMultiCurrencyPayment({
          splitId: 'split-1',
          participantId: 'non-existent',
          txHash: 'tx-hash-123',
          paidAsset: 'XLM',
          paidAmount: 50,
        }),
      ).rejects.toThrow('Participant non-existent not found');
    });
  });

  describe('Multi-Currency Payment Flow', () => {
    it('should handle complete flow: payment -> conversion -> tracking', async () => {
      const mockSplit: Partial<Split> = {
        id: 'split-1',
        totalAmount: 100,
        preferredCurrency: 'XLM',
      };

      const mockParticipant: Partial<Participant> = {
        id: 'participant-1',
        splitId: 'split-1',
        amountOwed: 50,
      };

      jest.spyOn(splitRepository, 'findOne').mockResolvedValue(mockSplit as Split);
      jest.spyOn(participantRepository, 'findOne').mockResolvedValue(mockParticipant as Participant);
      jest.spyOn(paymentRepository, 'findOne').mockResolvedValue(null);
      jest.spyOn(paymentRepository, 'create').mockReturnValue({
        id: 'payment-1',
        splitId: 'split-1',
        participantId: 'participant-1',
        txHash: 'tx-hash-123',
        amount: 100,
        asset: 'XLM',
        status: 'confirmed',
        createdAt: new Date(),
        updatedAt: new Date(),
      } as Payment);
      jest.spyOn(paymentRepository, 'save').mockResolvedValue({
        id: 'payment-1',
        splitId: 'split-1',
        participantId: 'participant-1',
        txHash: 'tx-hash-123',
        amount: 100,
        asset: 'XLM',
        status: 'confirmed',
        createdAt: new Date(),
        updatedAt: new Date(),
      } as Payment);

      jest.spyOn(exchangeRateTracker, 'getBestExchangeRate').mockResolvedValue({
        rate: 2.0,
        sourceAmount: 50,
        destinationAmount: 100,
        path: [],
        timestamp: new Date(),
      });

      jest.spyOn(exchangeRateTracker, 'trackExchangeRate').mockResolvedValue({
        id: 'multi-currency-1',
        paymentId: 'payment-1',
        paidAsset: 'USDC:GA5Z...',
        paidAmount: 50,
        receivedAsset: 'XLM',
        receivedAmount: 100,
        exchangeRate: 2.0,
        pathPaymentTxHash: 'tx-hash-123',
        createdAt: new Date(),
      } as MultiCurrencyPayment);

      const result = await multiCurrencyService.processMultiCurrencyPayment({
        splitId: 'split-1',
        participantId: 'participant-1',
        txHash: 'tx-hash-123',
        paidAsset: 'USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN',
        paidAmount: 50,
      });

      expect(result.success).toBe(true);
      expect(result.paymentId).toBe('payment-1');
      expect(result.multiCurrencyPaymentId).toBe('multi-currency-1');
      expect(exchangeRateTracker.trackExchangeRate).toHaveBeenCalled();
    });
  });
});
