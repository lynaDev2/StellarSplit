import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { SimplifiedDebtEdge } from './entities/simplified-debt.entity';

export interface RawDebt {
  from: string;   // debtor wallet address
  to: string;     // creditor wallet address
  amount: number;
  asset: string;
}

export interface DebtGraphResult {
  simplifiedDebts: SimplifiedDebtEdge[];
  originalTransactionCount: number;
  simplifiedTransactionCount: number;
  savingsPercentage: number;
}

@Injectable()
export class DebtGraphService {
  private readonly logger = new Logger(DebtGraphService.name);

  constructor(
    @InjectRepository(Participant)
    private readonly participantRepo: Repository<Participant>,
    @InjectRepository(Split)
    private readonly splitRepo: Repository<Split>,
  ) {}

  /**
   * Load raw outstanding debts for a set of wallet addresses across all their active splits.
   * A participant "owes" (amountOwed - amountPaid) to the split creator.
   */
  async loadRawDebts(walletAddresses: string[], groupId?: string): Promise<RawDebt[]> {
    // Build the query: find all non-fully-paid participants whose wallet is in the list
    const qb = this.participantRepo
      .createQueryBuilder('p')
      .innerJoinAndSelect('p.split', 'split')
      .where('p.walletAddress IN (:...wallets)', { wallets: walletAddresses })
      .andWhere("p.status != 'paid'")
      .andWhere('split.status != :completed', { completed: 'completed' })
      .andWhere('split.isFrozen = false')
      .andWhere('p.deleted_at IS NULL')
      .andWhere('split.deleted_at IS NULL');

    const participants = await qb.getMany();

    const rawDebts: RawDebt[] = [];

    for (const participant of participants) {
      const split = participant.split!;
      const balance = Number(participant.amountOwed) - Number(participant.amountPaid);
      if (balance <= 0.0001) continue; // already settled (epsilon guard)

      const creatorWallet = split.creatorWalletAddress;
      if (!creatorWallet) continue;
      if (!walletAddresses.includes(creatorWallet)) continue; // creator not in the requested set

      const asset = split.preferredCurrency ?? 'XLM';

      rawDebts.push({
        from: participant.walletAddress!,
        to: creatorWallet,
        amount: balance,
        asset,
      });
    }

    return rawDebts;
  }

  /**
   * Core greedy debt simplification algorithm.
   *
   * Strategy:
   *  1. Separate debts by asset (currency).
   *  2. For each asset, compute net balance per participant.
   *  3. Apply the min-cash-flow greedy: repeatedly match the largest creditor
   *     against the largest debtor until all balances are zero.
   *
   * This yields at most (N-1) transactions for N participants per asset,
   * which is the theoretical minimum for a connected graph.
   */
  simplify(rawDebts: RawDebt[]): DebtGraphResult {
    const originalTransactionCount = rawDebts.length;

    if (rawDebts.length === 0) {
      return {
        simplifiedDebts: [],
        originalTransactionCount: 0,
        simplifiedTransactionCount: 0,
        savingsPercentage: 0,
      };
    }

    // Group raw debts by asset
    const byAsset = new Map<string, RawDebt[]>();
    for (const debt of rawDebts) {
      const list = byAsset.get(debt.asset) ?? [];
      list.push(debt);
      byAsset.set(debt.asset, list);
    }

    const simplifiedDebts: SimplifiedDebtEdge[] = [];

    for (const [asset, debts] of byAsset.entries()) {
      const assetDebts = this.simplifyAsset(asset, debts);
      simplifiedDebts.push(...assetDebts);
    }

    const simplifiedTransactionCount = simplifiedDebts.length;
    const savingsPercentage =
      originalTransactionCount > 0
        ? Math.max(
            0,
            ((originalTransactionCount - simplifiedTransactionCount) /
              originalTransactionCount) *
              100,
          )
        : 0;

    return {
      simplifiedDebts,
      originalTransactionCount,
      simplifiedTransactionCount,
      savingsPercentage: Math.round(savingsPercentage * 100) / 100,
    };
  }

  /**
   * Simplify debts for a single asset using the min-cash-flow greedy algorithm.
   */
  private simplifyAsset(asset: string, debts: RawDebt[]): SimplifiedDebtEdge[] {
    // Compute net balance per wallet (positive = creditor, negative = debtor)
    const balance = new Map<string, number>();

    for (const debt of debts) {
      balance.set(debt.from, (balance.get(debt.from) ?? 0) - debt.amount);
      balance.set(debt.to, (balance.get(debt.to) ?? 0) + debt.amount);
    }

    // Filter out near-zero balances
    const participants = Array.from(balance.entries())
      .filter(([, v]) => Math.abs(v) > 0.0001)
      .map(([wallet, net]) => ({ wallet, net }));

    const result: SimplifiedDebtEdge[] = [];

    // Work with mutable copies
    const nets = participants.map((p) => ({ ...p }));

    while (true) {
      // Sort: most negative first (biggest debtors), most positive last (biggest creditors)
      nets.sort((a, b) => a.net - b.net);

      const debtor = nets[0];
      const creditor = nets[nets.length - 1];

      if (!debtor || !creditor) break;
      if (Math.abs(debtor.net) < 0.0001 || Math.abs(creditor.net) < 0.0001) break;
      if (debtor.net >= 0) break; // no more debtors

      const settleAmount = Math.min(Math.abs(debtor.net), creditor.net);

      result.push({
        from: debtor.wallet,
        to: creditor.wallet,
        amount: Math.round(settleAmount * 1_000_000) / 1_000_000, // 7 decimal Stellar precision
        asset,
      });

      debtor.net += settleAmount;
      creditor.net -= settleAmount;

      // Remove settled participants
      if (Math.abs(debtor.net) < 0.0001) nets.splice(0, 1);
      if (Math.abs(creditor.net) < 0.0001) nets.splice(nets.length - 1, 1);

      if (nets.length < 2) break;
    }

    return result;
  }
}
