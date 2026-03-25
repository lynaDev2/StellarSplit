import { Test, TestingModule } from '@nestjs/testing';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { DebtGraphService, RawDebt } from './debt-graph.service';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';

/** Build a minimal mock repository */
const mockRepo = () => ({
  createQueryBuilder: jest.fn(),
  findOne: jest.fn(),
  find: jest.fn(),
  save: jest.fn(),
  delete: jest.fn(),
});

describe('DebtGraphService – simplify()', () => {
  let service: DebtGraphService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DebtGraphService,
        { provide: getRepositoryToken(Participant), useFactory: mockRepo },
        { provide: getRepositoryToken(Split), useFactory: mockRepo },
      ],
    }).compile();

    service = module.get<DebtGraphService>(DebtGraphService);
  });

  // ─── Empty input ─────────────────────────────────────────────────────────────
  it('returns empty result for no debts', () => {
    const result = service.simplify([]);
    expect(result.simplifiedDebts).toHaveLength(0);
    expect(result.originalTransactionCount).toBe(0);
    expect(result.simplifiedTransactionCount).toBe(0);
    expect(result.savingsPercentage).toBe(0);
  });

  // ─── Single debt ─────────────────────────────────────────────────────────────
  it('keeps single debt unchanged', () => {
    const raw: RawDebt[] = [{ from: 'A', to: 'B', amount: 10, asset: 'XLM' }];
    const result = service.simplify(raw);
    expect(result.simplifiedDebts).toHaveLength(1);
    expect(result.simplifiedDebts[0]).toMatchObject({ from: 'A', to: 'B', amount: 10, asset: 'XLM' });
    expect(result.savingsPercentage).toBe(0);
  });

  // ─── Triangle (A→B, B→C, A→C) can become at most 2 payments ─────────────────
  it('simplifies a triangle of 3 debts into 2 transactions', () => {
    const raw: RawDebt[] = [
      { from: 'A', to: 'B', amount: 10, asset: 'XLM' },
      { from: 'B', to: 'C', amount: 10, asset: 'XLM' },
      { from: 'A', to: 'C', amount: 10, asset: 'XLM' },
    ];
    const result = service.simplify(raw);
    // Net: A = -20, B = 0, C = +20 → only 1 transaction needed
    expect(result.simplifiedTransactionCount).toBeLessThanOrEqual(2);
    expect(result.originalTransactionCount).toBe(3);
    assertNetBalancesPreserved(raw, result.simplifiedDebts, 'XLM');
  });

  // ─── Classic 4-person example (6 debts → ≤3) ────────────────────────────────
  it('reduces 6 transactions (4 people) to ≤3', () => {
    // A owes B 10, A owes C 15, B owes C 5, B owes D 10, C owes D 20, A owes D 5
    const raw: RawDebt[] = [
      { from: 'A', to: 'B', amount: 10, asset: 'XLM' },
      { from: 'A', to: 'C', amount: 15, asset: 'XLM' },
      { from: 'B', to: 'C', amount: 5, asset: 'XLM' },
      { from: 'B', to: 'D', amount: 10, asset: 'XLM' },
      { from: 'C', to: 'D', amount: 20, asset: 'XLM' },
      { from: 'A', to: 'D', amount: 5, asset: 'XLM' },
    ];
    const result = service.simplify(raw);
    expect(result.originalTransactionCount).toBe(6);
    expect(result.simplifiedTransactionCount).toBeLessThanOrEqual(3); // N-1 = 3
    assertNetBalancesPreserved(raw, result.simplifiedDebts, 'XLM');
    expect(result.savingsPercentage).toBeGreaterThan(0);
  });

  // ─── Multi-currency: separate graphs per asset ───────────────────────────────
  it('handles multi-currency debts in separate graphs', () => {
    const raw: RawDebt[] = [
      { from: 'A', to: 'B', amount: 10, asset: 'XLM' },
      { from: 'B', to: 'C', amount: 10, asset: 'XLM' },
      { from: 'A', to: 'B', amount: 50, asset: 'USDC:ISSUER' },
      { from: 'B', to: 'C', amount: 50, asset: 'USDC:ISSUER' },
    ];
    const result = service.simplify(raw);

    const xlmDebts = result.simplifiedDebts.filter((d) => d.asset === 'XLM');
    const usdcDebts = result.simplifiedDebts.filter((d) => d.asset === 'USDC:ISSUER');

    // Each currency is independently simplified
    assertNetBalancesPreserved(
      raw.filter((d) => d.asset === 'XLM'),
      xlmDebts,
      'XLM',
    );
    assertNetBalancesPreserved(
      raw.filter((d) => d.asset === 'USDC:ISSUER'),
      usdcDebts,
      'USDC:ISSUER',
    );
  });

  // ─── 20-user performance test ────────────────────────────────────────────────
  it('simplifies 20 users with dense debts in under 100ms', () => {
    const users = Array.from({ length: 20 }, (_, i) => `wallet${i}`);
    const raw: RawDebt[] = [];

    // Create a dense debt graph: every user owes the next one
    for (let i = 0; i < users.length - 1; i++) {
      for (let j = i + 1; j < users.length; j++) {
        raw.push({
          from: users[i],
          to: users[j],
          amount: Math.round(Math.random() * 100 * 100) / 100,
          asset: 'XLM',
        });
      }
    }

    const start = Date.now();
    const result = service.simplify(raw);
    const elapsed = Date.now() - start;

    expect(elapsed).toBeLessThan(100);
    expect(result.simplifiedTransactionCount).toBeLessThanOrEqual(users.length - 1);
    assertNetBalancesPreserved(raw, result.simplifiedDebts, 'XLM');
  });

  // ─── Circular debts net to zero ──────────────────────────────────────────────
  it('resolves circular debts to zero transactions when net is zero', () => {
    // A→B 10, B→C 10, C→A 10 — net for all is 0
    const raw: RawDebt[] = [
      { from: 'A', to: 'B', amount: 10, asset: 'XLM' },
      { from: 'B', to: 'C', amount: 10, asset: 'XLM' },
      { from: 'C', to: 'A', amount: 10, asset: 'XLM' },
    ];
    const result = service.simplify(raw);
    expect(result.simplifiedTransactionCount).toBe(0);
    expect(result.savingsPercentage).toBe(100);
  });

  // ─── Known input → known output ──────────────────────────────────────────────
  it('correctly simplifies known input: A owes B 30 → A pays B 30', () => {
    const raw: RawDebt[] = [
      { from: 'A', to: 'B', amount: 20, asset: 'XLM' },
      { from: 'A', to: 'B', amount: 10, asset: 'XLM' },
    ];
    const result = service.simplify(raw);
    // Net: A = -30, B = +30 → 1 transaction
    expect(result.simplifiedTransactionCount).toBe(1);
    expect(result.simplifiedDebts[0].from).toBe('A');
    expect(result.simplifiedDebts[0].to).toBe('B');
    expect(result.simplifiedDebts[0].amount).toBeCloseTo(30, 4);
  });
});

// ─── Helper: verify net balances are preserved ────────────────────────────────
function assertNetBalancesPreserved(
  rawDebts: RawDebt[],
  simplified: Array<{ from: string; to: string; amount: number; asset: string }>,
  asset: string,
): void {
  const rawNet = new Map<string, number>();
  for (const d of rawDebts.filter((r) => r.asset === asset)) {
    rawNet.set(d.from, (rawNet.get(d.from) ?? 0) - d.amount);
    rawNet.set(d.to, (rawNet.get(d.to) ?? 0) + d.amount);
  }

  const simNet = new Map<string, number>();
  for (const d of simplified.filter((r) => r.asset === asset)) {
    simNet.set(d.from, (simNet.get(d.from) ?? 0) - d.amount);
    simNet.set(d.to, (simNet.get(d.to) ?? 0) + d.amount);
  }

  for (const [wallet, net] of rawNet.entries()) {
    const simBalance = simNet.get(wallet) ?? 0;
    expect(simBalance).toBeCloseTo(net, 4);
  }
}
