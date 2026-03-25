import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, LessThan, MoreThan } from 'typeorm';
import { SimplifiedDebt, SimplifiedDebtEdge } from './entities/simplified-debt.entity';
import { DebtGraphService } from './debt-graph.service';

const CACHE_TTL_HOURS = 24;
const STELLAR_PAYMENT_BASE_URL = 'https://stellarpay.io/pay'; // Stellar Web Authentication / SEP-7 style

@Injectable()
export class DebtSimplificationService {
  private readonly logger = new Logger(DebtSimplificationService.name);

  constructor(
    @InjectRepository(SimplifiedDebt)
    private readonly simplifiedDebtRepo: Repository<SimplifiedDebt>,
    private readonly debtGraphService: DebtGraphService,
  ) {}

  /**
   * Calculate (or return cached) simplified debts for a given set of user wallet addresses.
   * Recalculates if the cache has expired.
   */
  async calculate(userIds: string[], groupId?: string): Promise<SimplifiedDebt> {
    const sortedIds = [...userIds].sort();

    // Try to load a valid cached result
    const cached = await this.findValidCache(sortedIds, groupId);
    if (cached) {
      this.logger.log(`Returning cached debt simplification for ${sortedIds.length} users`);
      return cached;
    }

    return this.recalculate(sortedIds, groupId);
  }

  /**
   * Force recalculation and persist the new result.
   */
  async recalculate(sortedUserIds: string[], groupId?: string): Promise<SimplifiedDebt> {
    this.logger.log(`Recalculating debts for ${sortedUserIds.length} users`);

    // Load raw debts from DB
    const rawDebts = await this.debtGraphService.loadRawDebts(sortedUserIds, groupId);

    // Run simplification algorithm
    const graphResult = this.debtGraphService.simplify(rawDebts);

    // Generate Stellar payment links for each simplified transaction
    const debtsWithLinks: SimplifiedDebtEdge[] = graphResult.simplifiedDebts.map((debt) => ({
      ...debt,
      paymentLink: this.generateStellarPaymentLink(debt),
    }));

    // Expire old cache entries for the same user set / group
    await this.invalidateCacheForUsers(sortedUserIds, groupId);

    // Persist new result
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + CACHE_TTL_HOURS);

    const record = this.simplifiedDebtRepo.create({
      groupId,
      calculatedForUserIds: sortedUserIds,
      debts: debtsWithLinks,
      originalTransactionCount: graphResult.originalTransactionCount,
      simplifiedTransactionCount: graphResult.simplifiedTransactionCount,
      savingsPercentage: graphResult.savingsPercentage,
      expiresAt,
    });

    const saved = await this.simplifiedDebtRepo.save(record);
    this.logger.log(
      `Saved simplified debts: ${graphResult.simplifiedTransactionCount} transactions ` +
        `(was ${graphResult.originalTransactionCount}), ${graphResult.savingsPercentage}% savings`,
    );
    return saved;
  }

  /**
   * Get the latest valid cached simplification for a group.
   */
  async getByGroup(groupId: string): Promise<SimplifiedDebt> {
    const result = await this.simplifiedDebtRepo.findOne({
      where: {
        groupId,
        expiresAt: MoreThan(new Date()),
      },
      order: { calculatedAt: 'DESC' },
    });

    if (!result) {
      throw new NotFoundException(
        `No valid debt simplification found for group ${groupId}. Please POST /calculate first.`,
      );
    }

    return result;
  }

  /**
   * Get the latest valid cached simplification that involves a given wallet address.
   */
  async getByWallet(walletAddress: string): Promise<SimplifiedDebt[]> {
    // JSONB containment: calculatedForUserIds @> '[walletAddress]'
    const results = await this.simplifiedDebtRepo
      .createQueryBuilder('sd')
      .where('sd."calculatedForUserIds" @> :wallet', {
        wallet: JSON.stringify([walletAddress]),
      })
      .andWhere('sd."expiresAt" > :now', { now: new Date() })
      .orderBy('sd."calculatedAt"', 'DESC')
      .getMany();

    return results;
  }

  /**
   * Generate Stellar payment links for simplified debts, optionally recalculating first.
   */
  async generatePaymentLinks(userIds: string[], groupId?: string): Promise<SimplifiedDebt> {
    const result = await this.calculate(userIds, groupId);

    // Ensure all debts have payment links (they should from calculate(), but guard anyway)
    const updatedDebts = result.debts.map((debt) => ({
      ...debt,
      paymentLink: debt.paymentLink ?? this.generateStellarPaymentLink(debt),
    }));

    if (updatedDebts.some((d, i) => d.paymentLink !== result.debts[i].paymentLink)) {
      result.debts = updatedDebts;
      await this.simplifiedDebtRepo.save(result);
    }

    return result;
  }

  /**
   * Invalidate cached results when a relevant split is settled.
   * Call this from payment/settlement event handlers.
   */
  async invalidateForWallets(walletAddresses: string[]): Promise<void> {
    // Find all cached records that include any of the given wallets
    for (const wallet of walletAddresses) {
      await this.simplifiedDebtRepo
        .createQueryBuilder()
        .delete()
        .from(SimplifiedDebt)
        .where('"calculatedForUserIds" @> :wallet', {
          wallet: JSON.stringify([wallet]),
        })
        .execute();
    }
    this.logger.log(
      `Invalidated debt simplification cache for wallets: ${walletAddresses.join(', ')}`,
    );
  }

  /**
   * Invalidate cache for a group.
   */
  async invalidateForGroup(groupId: string): Promise<void> {
    await this.simplifiedDebtRepo.delete({ groupId });
    this.logger.log(`Invalidated debt simplification cache for group: ${groupId}`);
  }

  /**
   * Clean up expired cache entries (can be called by a scheduler).
   */
  async cleanExpiredCache(): Promise<number> {
    const result = await this.simplifiedDebtRepo.delete({
      expiresAt: LessThan(new Date()),
    });
    const affected = result.affected ?? 0;
    if (affected > 0) {
      this.logger.log(`Cleaned ${affected} expired debt simplification cache entries`);
    }
    return affected;
  }

  // ─── Private Helpers ────────────────────────────────────────────────────────

  private async findValidCache(
    sortedUserIds: string[],
    groupId?: string,
  ): Promise<SimplifiedDebt | null> {
    // Match by exact user set (JSONB equality) and unexpired
    const qb = this.simplifiedDebtRepo
      .createQueryBuilder('sd')
      .where('sd."calculatedForUserIds" = :ids', {
        ids: JSON.stringify(sortedUserIds),
      })
      .andWhere('sd."expiresAt" > :now', { now: new Date() });

    if (groupId) {
      qb.andWhere('sd."groupId" = :groupId', { groupId });
    } else {
      qb.andWhere('sd."groupId" IS NULL');
    }

    return qb.orderBy('sd."calculatedAt"', 'DESC').getOne();
  }

  private async invalidateCacheForUsers(
    sortedUserIds: string[],
    groupId?: string,
  ): Promise<void> {
    const qb = this.simplifiedDebtRepo
      .createQueryBuilder()
      .delete()
      .from(SimplifiedDebt)
      .where('"calculatedForUserIds" = :ids', {
        ids: JSON.stringify(sortedUserIds),
      });

    if (groupId) {
      qb.andWhere('"groupId" = :groupId', { groupId });
    } else {
      qb.andWhere('"groupId" IS NULL');
    }

    await qb.execute();
  }

  /**
   * Generate a SEP-7 / Stellar Web Authentication style payment URL.
   *
   * Format: stellar:<destination>?amount=<amount>&asset_code=<code>&asset_issuer=<issuer>&memo=StellarSplit
   *
   * Falls back to a web-based payment URL for non-native assets.
   */
  private generateStellarPaymentLink(debt: SimplifiedDebtEdge): string {
    const { to, amount, asset } = debt;
    const amountStr = amount.toFixed(7);

    // SEP-7 URI scheme: stellar:<destination>?...
    if (asset === 'XLM' || asset === 'native') {
      return `stellar:${to}?amount=${amountStr}&memo=StellarSplit&memo_type=text`;
    }

    // asset format: 'CODE:ISSUER'
    const [assetCode, assetIssuer] = asset.split(':');
    if (assetCode && assetIssuer) {
      return (
        `stellar:${to}?amount=${amountStr}` +
        `&asset_code=${assetCode}&asset_issuer=${assetIssuer}` +
        `&memo=StellarSplit&memo_type=text`
      );
    }

    // Fallback web link
    return `${STELLAR_PAYMENT_BASE_URL}?destination=${to}&amount=${amountStr}&asset=${encodeURIComponent(asset)}`;
  }
}
