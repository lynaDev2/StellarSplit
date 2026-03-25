import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { DebtSimplificationService } from './debt-simplification.service';
import { DebtGraphService, RawDebt } from './debt-graph.service';
import { SimplifiedDebt } from './entities/simplified-debt.entity';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';

// ─── Mock factories ───────────────────────────────────────────────────────────

const buildQbMock = () => {
  const qb: any = {
    where: jest.fn().mockReturnThis(),
    andWhere: jest.fn().mockReturnThis(),
    orderBy: jest.fn().mockReturnThis(),
    delete: jest.fn().mockReturnThis(),
    from: jest.fn().mockReturnThis(),
    execute: jest.fn().mockResolvedValue({ affected: 1 }),
    getOne: jest.fn().mockResolvedValue(null),
    getMany: jest.fn().mockResolvedValue([]),
  };
  return qb;
};

const buildSimplifiedDebtRepoMock = () => ({
  createQueryBuilder: jest.fn(() => buildQbMock()),
  findOne: jest.fn().mockResolvedValue(null),
  find: jest.fn().mockResolvedValue([]),
  create: jest.fn((data) => ({ ...data, id: 'test-uuid', calculatedAt: new Date() })),
  save: jest.fn().mockImplementation((entity) => Promise.resolve({ ...entity, id: 'test-uuid' })),
  delete: jest.fn().mockResolvedValue({ affected: 1 }),
});

const buildParticipantRepoMock = () => ({
  createQueryBuilder: jest.fn(() => {
    const qb = buildQbMock();
    qb.innerJoinAndSelect = jest.fn().mockReturnThis();
    return qb;
  }),
});

const buildSplitRepoMock = () => ({
  findOne: jest.fn(),
});

// ─── Test Suite ───────────────────────────────────────────────────────────────

describe('DebtSimplificationService – integration', () => {
  let module: TestingModule;
  let service: DebtSimplificationService;
  let graphService: DebtGraphService;
  let simplifiedDebtRepo: ReturnType<typeof buildSimplifiedDebtRepoMock>;

  beforeEach(async () => {
    simplifiedDebtRepo = buildSimplifiedDebtRepoMock();

    module = await Test.createTestingModule({
      providers: [
        DebtSimplificationService,
        DebtGraphService,
        { provide: getRepositoryToken(SimplifiedDebt), useValue: simplifiedDebtRepo },
        { provide: getRepositoryToken(Participant), useFactory: buildParticipantRepoMock },
        { provide: getRepositoryToken(Split), useFactory: buildSplitRepoMock },
      ],
    }).compile();

    service = module.get<DebtSimplificationService>(DebtSimplificationService);
    graphService = module.get<DebtGraphService>(DebtGraphService);
  });

  afterEach(() => jest.clearAllMocks());

  // ─── calculate() returns cached result when fresh ─────────────────────────
  it('returns cached result when a fresh record exists', async () => {
    const cachedRecord: Partial<SimplifiedDebt> = {
      id: 'cached-id',
      calculatedForUserIds: ['walletA', 'walletB'],
      debts: [],
      originalTransactionCount: 3,
      simplifiedTransactionCount: 1,
      savingsPercentage: 66.67,
      expiresAt: new Date(Date.now() + 1000 * 3600 * 24),
      calculatedAt: new Date(),
    };

    // The first QBuilder call is the cache lookup
    const cacheQb = buildQbMock();
    cacheQb.getOne.mockResolvedValue(cachedRecord);
    simplifiedDebtRepo.createQueryBuilder.mockReturnValueOnce(cacheQb);

    const result = await service.calculate(['walletB', 'walletA']); // unsorted input

    expect(result.id).toBe('cached-id');
    expect(simplifiedDebtRepo.save).not.toHaveBeenCalled();
  });

  // ─── calculate() recalculates when no cache ──────────────────────────────
  it('calls recalculate when no valid cache entry exists', async () => {
    // All QBs return null / empty
    const loadDebtsSpy = jest
      .spyOn(graphService, 'loadRawDebts')
      .mockResolvedValue([
        { from: 'walletA', to: 'walletB', amount: 10, asset: 'XLM' },
        { from: 'walletC', to: 'walletB', amount: 5, asset: 'XLM' },
        { from: 'walletA', to: 'walletC', amount: 3, asset: 'XLM' },
      ] as RawDebt[]);

    const result = await service.calculate(['walletA', 'walletB', 'walletC']);

    expect(loadDebtsSpy).toHaveBeenCalledWith(
      expect.arrayContaining(['walletA', 'walletB', 'walletC']),
      undefined,
    );
    expect(simplifiedDebtRepo.save).toHaveBeenCalledTimes(1);
    // At most N-1 = 2 transactions
    expect(result.simplifiedTransactionCount).toBeLessThanOrEqual(2);
    expect(result.savingsPercentage).toBeGreaterThan(0);
  });

  // ─── generatePaymentLinks() attaches SEP-7 URIs ──────────────────────────
  it('attaches Stellar SEP-7 payment links to each simplified debt', async () => {
    jest.spyOn(graphService, 'loadRawDebts').mockResolvedValue([
      { from: 'GXXX', to: 'GYYY', amount: 25, asset: 'XLM' },
    ] as RawDebt[]);

    const result = await service.generatePaymentLinks(['GXXX', 'GYYY']);

    expect(result.debts).toHaveLength(1);
    expect(result.debts[0].paymentLink).toMatch(/^stellar:GYYY/);
    expect(result.debts[0].paymentLink).toContain('amount=25.0000000');
    expect(result.debts[0].paymentLink).toContain('memo=StellarSplit');
  });

  // ─── Payment link for non-native asset ───────────────────────────────────
  it('generates correct SEP-7 link for USDC asset', async () => {
    const issuer = 'GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN';
    jest.spyOn(graphService, 'loadRawDebts').mockResolvedValue([
      { from: 'GXXX', to: 'GYYY', amount: 50.5, asset: `USDC:${issuer}` },
    ] as RawDebt[]);

    const result = await service.generatePaymentLinks(['GXXX', 'GYYY']);

    const link = result.debts[0].paymentLink!;
    expect(link).toMatch(/^stellar:GYYY/);
    expect(link).toContain('asset_code=USDC');
    expect(link).toContain(`asset_issuer=${issuer}`);
  });

  // ─── getByGroup() throws NotFoundException for missing group ─────────────
  it('throws NotFoundException when no result exists for group', async () => {
    const qb = buildQbMock();
    qb.getOne.mockResolvedValue(null);
    // findOne (used by getByGroup) returns null
    simplifiedDebtRepo.findOne.mockResolvedValue(null);

    await expect(service.getByGroup('nonexistent-group-id')).rejects.toThrow(
      'No valid debt simplification found',
    );
  });

  // ─── getByWallet() returns matching records ───────────────────────────────
  it('returns records containing the given wallet address', async () => {
    const mockRecords: Partial<SimplifiedDebt>[] = [
      {
        id: 'r1',
        calculatedForUserIds: ['walletA', 'walletB'],
        debts: [],
        expiresAt: new Date(Date.now() + 3600000),
      },
    ];

    const qb = buildQbMock();
    qb.getMany.mockResolvedValue(mockRecords);
    simplifiedDebtRepo.createQueryBuilder.mockReturnValue(qb);

    const results = await service.getByWallet('walletA');
    expect(results).toHaveLength(1);
    expect(results[0].id).toBe('r1');
  });

  // ─── invalidateForWallets() deletes cache entries ─────────────────────────
  it('deletes all cache entries for the given wallets', async () => {
    const qb = buildQbMock();
    simplifiedDebtRepo.createQueryBuilder.mockReturnValue(qb);

    await service.invalidateForWallets(['walletA', 'walletB']);

    expect(simplifiedDebtRepo.createQueryBuilder).toHaveBeenCalledTimes(2);
    expect(qb.execute).toHaveBeenCalledTimes(2);
  });

  // ─── invalidateForGroup() deletes by groupId ─────────────────────────────
  it('deletes cache entries by groupId', async () => {
    await service.invalidateForGroup('group-123');
    expect(simplifiedDebtRepo.delete).toHaveBeenCalledWith({ groupId: 'group-123' });
  });

  // ─── cleanExpiredCache() deletes expired entries ──────────────────────────
  it('cleans expired cache entries', async () => {
    simplifiedDebtRepo.delete.mockResolvedValue({ affected: 5 });

    const count = await service.cleanExpiredCache();

    expect(count).toBe(5);
    expect(simplifiedDebtRepo.delete).toHaveBeenCalledWith(
      expect.objectContaining({ expiresAt: expect.anything() }),
    );
  });

  // ─── Multi-currency split scenario ───────────────────────────────────────
  it('handles multi-currency debt calculations', async () => {
    jest.spyOn(graphService, 'loadRawDebts').mockResolvedValue([
      { from: 'A', to: 'B', amount: 100, asset: 'XLM' },
      { from: 'B', to: 'C', amount: 100, asset: 'XLM' },
      { from: 'A', to: 'B', amount: 200, asset: 'USDC:ISSUER' },
      { from: 'B', to: 'C', amount: 200, asset: 'USDC:ISSUER' },
    ] as RawDebt[]);

    const result = await service.calculate(['A', 'B', 'C']);

    const xlmDebts = result.debts.filter((d) => d.asset === 'XLM');
    const usdcDebts = result.debts.filter((d) => d.asset === 'USDC:ISSUER');

    // XLM: A→B 100, B→C 100 → net: A=-100, B=0, C=+100 → 1 transaction
    expect(xlmDebts).toHaveLength(1);
    // USDC: same pattern
    expect(usdcDebts).toHaveLength(1);
  });
});
