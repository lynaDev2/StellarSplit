import { Test, TestingModule } from "@nestjs/testing";
import { DataSource, Repository } from "typeorm";
import { getRepositoryToken } from "@nestjs/typeorm";
import { PaymentProcessorService } from "./payment-processor.service";
import { PaymentReconciliationService } from "./payment-reconciliation.service";
import { Payment, PaymentSettlementStatus, PaymentProcessingStatus } from "../entities/payment.entity";
import { Participant } from "../entities/participant.entity";
import { Split } from "../entities/split.entity";
import { StellarService } from "../stellar/stellar.service";
import { PaymentGateway } from "../websocket/payment.gateway";
import { EventsGateway } from "../gateway/events.gateway";
import { EmailService } from "../email/email.service";
import { MultiCurrencyService } from "../multi-currency/multi-currency.service";
import { ConflictException } from "@nestjs/common";

/**
 * Mock Stellar service for testing
 */
const mockStellarService = {
  verifyTransaction: jest.fn(),
};

/**
 * Mock Payment Gateway
 */
const mockPaymentGateway = {
  emitPaymentNotification: jest.fn(),
  emitSplitCompletion: jest.fn(),
};

/**
 * Mock Events Gateway
 */
const mockEventsGateway = {
  emitPaymentReceived: jest.fn(),
  emitSplitUpdated: jest.fn(),
};

/**
 * Mock Email Service
 */
const mockEmailService = {
  getUser: jest.fn(),
  sendPaymentConfirmation: jest.fn(),
};

/**
 * Mock MultiCurrency Service
 */
const mockMultiCurrencyService = {
  processMultiCurrencyPayment: jest.fn().mockResolvedValue({
    receivedAmount: 100,
    receivedAsset: "XLM",
    requiresConversion: false,
  }),
};

/**
 * Mock DataSource with transaction support
 */
const mockDataSource = {
  createQueryRunner: jest.fn(),
};

describe("PaymentProcessorService Concurrency Tests", () => {
  let service: PaymentProcessorService;
  let paymentRepository: Repository<Payment>;
  let participantRepository: Repository<Participant>;
  let splitRepository: Repository<Split>;
  let mockQueryRunner: any;

  beforeEach(async () => {
    // Setup mock query runner with transaction support
    mockQueryRunner = {
      connect: jest.fn(),
      startTransaction: jest.fn(),
      commitTransaction: jest.fn().mockResolvedValue(undefined),
      rollbackTransaction: jest.fn().mockResolvedValue(undefined),
      release: jest.fn().mockResolvedValue(undefined),
      manager: {
        findOne: jest.fn(),
        find: jest.fn(),
        create: jest.fn().mockImplementation((entity, data) => ({ ...data, id: "test-id" })),
        save: jest.fn().mockImplementation((entity, data) => ({ ...data, id: "test-id" })),
        update: jest.fn().mockResolvedValue({ affected: 1 }),
      },
    };

    mockDataSource.createQueryRunner.mockReturnValue(mockQueryRunner);

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PaymentProcessorService,
        {
          provide: getRepositoryToken(Payment),
          useValue: {
            findOne: jest.fn(),
            save: jest.fn(),
            update: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Participant),
          useValue: {
            findOne: jest.fn(),
            find: jest.fn(),
            update: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Split),
          useValue: {
            findOne: jest.fn(),
            find: jest.fn(),
            update: jest.fn(),
          },
        },
        {
          provide: DataSource,
          useValue: mockDataSource,
        },
        {
          provide: StellarService,
          useValue: mockStellarService,
        },
        {
          provide: PaymentGateway,
          useValue: mockPaymentGateway,
        },
        {
          provide: EventsGateway,
          useValue: mockEventsGateway,
        },
        {
          provide: EmailService,
          useValue: mockEmailService,
        },
        {
          provide: MultiCurrencyService,
          useValue: mockMultiCurrencyService,
        },
      ],
    }).compile();

    service = module.get<PaymentProcessorService>(PaymentProcessorService);
    paymentRepository = module.get<Repository<Payment>>(getRepositoryToken(Payment));
    participantRepository = module.get<Repository<Participant>>(getRepositoryToken(Participant));
    splitRepository = module.get<Repository<Split>>(getRepositoryToken(Split));
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("Idempotency Tests", () => {
    it("should detect duplicate via idempotency key and return existing payment", async () => {
      // Setup
      const splitId = "split-123";
      const participantId = "participant-456";
      const txHash = "tx-789";
      const idempotencyKey = service.generateIdempotencyKey(splitId, participantId, txHash);

      // Mock existing payment found by idempotency key
      mockQueryRunner.manager.findOne.mockResolvedValueOnce({
        id: "existing-payment-id",
        idempotencyKey,
        txHash,
        status: "confirmed",
      });

      // Execute
      const result = await service.processPaymentSubmission({
        splitId,
        participantId,
        txHash,
        idempotencyKey,
      });

      // Verify
      expect(result.isDuplicate).toBe(true);
      expect(result.paymentId).toBe("existing-payment-id");
      expect(mockQueryRunner.rollbackTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.commitTransaction).not.toHaveBeenCalled();
    });

    it("should detect duplicate via txHash and return existing payment", async () => {
      // Setup
      const splitId = "split-123";
      const participantId = "participant-456";
      const txHash = "tx-789";
      const idempotencyKey = service.generateIdempotencyKey(splitId, participantId, txHash);

      // No existing payment with idempotency key
      mockQueryRunner.manager.findOne
        .mockResolvedValueOnce(null) // idempotency key check
        .mockResolvedValueOnce({
          // txHash check
          id: "existing-payment-id",
          txHash,
          status: "confirmed",
        });

      // Execute
      const result = await service.processPaymentSubmission({
        splitId,
        participantId,
        txHash,
        idempotencyKey,
      });

      // Verify
      expect(result.isDuplicate).toBe(true);
      expect(result.paymentId).toBe("existing-payment-id");
    });

    it("should generate consistent idempotency key", () => {
      const key1 = service.generateIdempotencyKey("split-1", "participant-1", "tx-1");
      const key2 = service.generateIdempotencyKey("split-1", "participant-1", "tx-1");
      expect(key1).toBe(key2);

      const key3 = service.generateIdempotencyKey("split-2", "participant-1", "tx-1");
      expect(key1).not.toBe(key3);
    });
  });

  describe("Transaction Tests", () => {
    it("should rollback transaction on verification failure", async () => {
      // Setup
      mockStellarService.verifyTransaction.mockResolvedValue(null);

      // Execute
      await expect(
        service.processPaymentSubmission({
          splitId: "split-123",
          participantId: "participant-456",
          txHash: "tx-789",
        }),
      ).rejects.toThrow();

      // Verify
      expect(mockQueryRunner.rollbackTransaction).toHaveBeenCalled();
      expect(mockQueryRunner.commitTransaction).not.toHaveBeenCalled();
    });

    it("should commit transaction on successful payment", async () => {
      // Setup
      mockStellarService.verifyTransaction.mockResolvedValue({
        valid: true,
        amount: 100,
        asset: "XLM",
        sender: "sender123",
        receiver: "receiver456",
        timestamp: "2024-01-01T00:00:00Z",
      });

      mockQueryRunner.manager.findOne
        .mockResolvedValueOnce(null) // idempotency key
        .mockResolvedValueOnce(null) // txHash
        .mockResolvedValueOnce({ id: "participant-456", splitId: "split-123", amountOwed: 100, amountPaid: 0, userId: "user-1", status: "pending" }) // participant
        .mockResolvedValueOnce({ id: "split-123", totalAmount: 100, isFrozen: false }); // split

      mockQueryRunner.manager.find
        .mockResolvedValueOnce([{ id: "participant-456", amountPaid: 0, amountOwed: 100 }]); // participants for split update

      // Execute
      const result = await service.processPaymentSubmission({
        splitId: "split-123",
        participantId: "participant-456",
        txHash: "tx-789",
      });

      // Verify
      expect(result.success).toBe(true);
      expect(mockQueryRunner.commitTransaction).toHaveBeenCalled();
    });
  });

  describe("Retry Tests", () => {
    it("should reject retry of non-failed payment", async () => {
      // Setup
      const payment = {
        id: "payment-123",
        splitId: "split-123",
        participantId: "participant-456",
        txHash: "tx-789",
        status: PaymentProcessingStatus.CONFIRMED,
        settlementStatus: PaymentSettlementStatus.CONFIRMED,
      };

      jest.spyOn(paymentRepository, "findOne").mockResolvedValue(payment as any);

      // Execute
      await expect(service.retryPayment("payment-123", "new-tx-hash")).rejects.toThrow(
        ConflictException,
      );
    });
  });
});

describe("PaymentReconciliationService Tests", () => {
  let service: PaymentReconciliationService;
  let paymentRepository: Repository<Payment>;
  let stellarService: StellarService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        PaymentReconciliationService,
        {
          provide: getRepositoryToken(Payment),
          useValue: {
            find: jest.fn(),
            findOne: jest.fn(),
            update: jest.fn(),
            count: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Participant),
          useValue: {
            update: jest.fn(),
          },
        },
        {
          provide: getRepositoryToken(Split),
          useValue: {
            findOne: jest.fn(),
          },
        },
        {
          provide: StellarService,
          useValue: mockStellarService,
        },
        {
          provide: EventsGateway,
          useValue: mockEventsGateway,
        },
      ],
    }).compile();

    service = module.get<PaymentReconciliationService>(PaymentReconciliationService);
    paymentRepository = module.get<Repository<Payment>>(getRepositoryToken(Payment));
    stellarService = module.get<StellarService>(StellarService);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("Reconciliation Tests", () => {
    it("should update payment to confirmed when on-chain verification succeeds", async () => {
      // Setup
      const payment = {
        id: "payment-123",
        txHash: "tx-789",
        settlementStatus: PaymentSettlementStatus.SUBMITTED,
        reconciliationAttempts: 0,
        splitId: "split-123",
        participantId: "participant-456",
      };

      mockStellarService.verifyTransaction.mockResolvedValue({
        valid: true,
        amount: 100,
        asset: "XLM",
        sender: "sender",
        receiver: "receiver",
        timestamp: "2024-01-01T00:00:00Z",
      });

      jest.spyOn(paymentRepository, "findOne").mockResolvedValue(payment as any);
      jest.spyOn(paymentRepository, "update").mockResolvedValue({ affected: 1 } as any);

      // Execute
      const result = await service.reconcilePayment("payment-123");

      // Verify
      expect(result.newStatus).toBe(PaymentSettlementStatus.CONFIRMED);
      expect(result.onChainValid).toBe(true);
    });

    it("should update payment to failed when on-chain verification fails", async () => {
      // Setup
      const payment = {
        id: "payment-123",
        txHash: "tx-789",
        settlementStatus: PaymentSettlementStatus.SUBMITTED,
        reconciliationAttempts: 0,
        splitId: "split-123",
        participantId: "participant-456",
      };

      mockStellarService.verifyTransaction.mockResolvedValue({
        valid: false,
        amount: 0,
        asset: "",
        sender: "",
        receiver: "",
        timestamp: "2024-01-01T00:00:00Z",
      });

      jest.spyOn(paymentRepository, "findOne").mockResolvedValue(payment as any);
      jest.spyOn(paymentRepository, "update").mockResolvedValue({ affected: 1 } as any);

      // Execute
      const result = await service.reconcilePayment("payment-123");

      // Verify
      expect(result.newStatus).toBe(PaymentSettlementStatus.FAILED);
    });

    it("should mark payment for review after max attempts", async () => {
      // Setup
      const payment = {
        id: "payment-123",
        txHash: "tx-789",
        settlementStatus: PaymentSettlementStatus.SUBMITTED,
        reconciliationAttempts: 4, // Will reach max (5) after increment
        splitId: "split-123",
        participantId: "participant-456",
      };

      mockStellarService.verifyTransaction.mockRejectedValue(new Error("Network error"));

      jest.spyOn(paymentRepository, "findOne").mockResolvedValue(payment as any);
      jest.spyOn(paymentRepository, "update").mockResolvedValue({ affected: 1 } as any);

      // Execute
      const result = await service.reconcilePayment("payment-123");

      // Verify
      expect(result.newStatus).toBe(PaymentSettlementStatus.REVIEW_REQUIRED);
      expect(result.error).toBe("Network error");
    });
  });

  describe("Stale Payment Detection", () => {
    it("should mark old pending payments as review required", async () => {
      // Setup
      const stalePayment = {
        id: "payment-stale",
        txHash: "tx-stale",
        settlementStatus: PaymentSettlementStatus.SUBMITTED,
        lastSettlementCheck: new Date(Date.now() - 90 * 60 * 1000), // 90 minutes ago
      };

      jest.spyOn(paymentRepository, "find").mockResolvedValue([stalePayment] as any);
      jest.spyOn(paymentRepository, "update").mockResolvedValue({ affected: 1 } as any);

      // Execute - manual call to detectStalePayments
      // In a real test we'd use TestScheduler or manual invocation
      const result = await service.markPaymentForReview(
        "payment-stale",
        "Payment stale - no confirmation after threshold",
      );

      // Verify
      expect(result).toBeUndefined();
      expect(paymentRepository.update).toHaveBeenCalledWith(
        "payment-stale",
        expect.objectContaining({
          settlementStatus: PaymentSettlementStatus.REVIEW_REQUIRED,
        }),
      );
    });
  });

  describe("Statistics Tests", () => {
    it("should return correct reconciliation stats", async () => {
      // Setup
      jest.spyOn(paymentRepository, "count")
        .mockResolvedValueOnce(100) // total
        .mockResolvedValueOnce(20) // submitted
        .mockResolvedValueOnce(70) // confirmed
        .mockResolvedValueOnce(5) // failed
        .mockResolvedValueOnce(3) // reviewRequired
        .mockResolvedValueOnce(2); // stale

      // Execute
      const stats = await service.getReconciliationStats();

      // Verify
      expect(stats.total).toBe(100);
      expect(stats.submitted).toBe(20);
      expect(stats.confirmed).toBe(70);
      expect(stats.failed).toBe(5);
      expect(stats.reviewRequired).toBe(3);
      expect(stats.stale).toBe(2);
    });
  });
});