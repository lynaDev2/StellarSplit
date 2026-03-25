import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { SplitHistory, SplitRole } from './entities/split-history.entity';
import { SplitArchive } from '../modules/archiving/entities/split-archive.entity';
import { HistoryQueryDto, HistoryStatusFilter } from './dto/history-query.dto';
import { HistoryResponseDto, HistoryItemDto, HistorySummaryDto } from './dto/history-response.dto';
import { ExportFormat } from '../export/entities/export-job.entity';

@Injectable()
export class SplitHistoryService {
  constructor(
    @InjectRepository(SplitHistory)
    private readonly repo: Repository<SplitHistory>,
    @InjectRepository(SplitArchive)
    private readonly archiveRepo: Repository<SplitArchive>,
  ) {}

  async getUserHistory(wallet: string) {
    const history = await this.repo.find({
      where: { userId: wallet },
      relations: ['split'],
      order: { completionTime: 'DESC' },
    });

    // Fetch archived splits involving this user
    // Using JSONB containment operator @> to find if user is a participant
    // or checking creatorWalletAddress in splitData
    const archives = await this.archiveRepo.createQueryBuilder('archive')
      .where(`archive.splitData ->> 'creatorWalletAddress' = :wallet`, { wallet })
      .orWhere(`archive.participantData @> :participant`, { participant: JSON.stringify([{ walletAddress: wallet }]) })
      .orderBy('archive.archivedAt', 'DESC')
      .getMany();

    const mappedArchives = archives.map(archive => {
      const isCreator = archive.splitData.creatorWalletAddress === wallet;
      const participant = archive.participantData.find((p: any) => p.walletAddress === wallet);
      
      // Calculate final amount similar to SplitHistory logic
      // For creators: usually total - paid? Or received? 
      // SplitHistory defines finalAmount as: paid (-) or received (+)
      // We'll estimate based on role.
      let finalAmount = '0';
      if (participant) {
        // As participant: amountPaid (negative if we consider cost) or simply amount involved?
        // SplitHistory says "paid (-) or received (+)"
        // If I paid 50, it's -50? 
        // Let's stick to simple amount for now or 0 if unclear.
        // Actually, if it's archived (expired/unpaid), maybe just show 0 or amountOwed?
        finalAmount = participant.amountOwed ? `-${participant.amountOwed}` : '0';
      } else if (isCreator) {
        finalAmount = archive.splitData.totalAmount;
      }

      return {
        id: archive.id,
        userId: wallet,
        split: { ...archive.splitData, status: 'archived' },
        role: isCreator ? SplitRole.CREATOR : SplitRole.PARTICIPANT,
        comment: archive.archiveReason,
        splitId: archive.originalSplitId,
        finalAmount: finalAmount,
        completionTime: archive.archivedAt,
        isArchived: true
      };
    });

    // Combine and sort by date (newest first)
    const combined = [...history, ...mappedArchives].sort((a, b) => 
      new Date(b.completionTime).getTime() - new Date(a.completionTime).getTime()
    );

    return combined;
  }

  async getUserStats(wallet: string) {
    const qb = this.repo.createQueryBuilder('sh');

    const [created, participated] = await Promise.all([
      qb.clone()
        .where('sh.userId = :wallet', { wallet })
        .andWhere('sh.role = :role', { role: SplitRole.CREATOR })
        .getCount(),

      qb.clone()
        .where('sh.userId = :wallet', { wallet })
        .andWhere('sh.role = :role', { role: SplitRole.PARTICIPANT })
        .getCount(),
    ]);

    const avgAmount = await qb
      .clone()
      .select('AVG(sh.finalAmount)', 'avg')
      .where('sh.userId = :wallet', { wallet })
      .getRawOne();

    const totalAmount = await qb
      .clone()
      .select('SUM(sh.finalAmount)', 'total')
      .where('sh.userId = :wallet', { wallet })
      .getRawOne();

    const frequentPartners = await qb
      .clone()
      .select('other.userId', 'partner')
      .addSelect('COUNT(*)', 'count')
      .innerJoin(
        SplitHistory,
        'other',
        'other.splitId = sh.splitId AND other.userId != sh.userId',
      )
      .where('sh.userId = :wallet', { wallet })
      .groupBy('other.userId')
      .orderBy('count', 'DESC')
      .getRawMany();

    // Fetch archives for stats
    const archives = await this.archiveRepo.createQueryBuilder('archive')
      .where(`archive.splitData ->> 'creatorWalletAddress' = :wallet`, { wallet })
      .orWhere(`archive.participantData @> :participant`, { participant: JSON.stringify([{ walletAddress: wallet }]) })
      .getMany();

    let archivedCreated = 0;
    let archivedParticipated = 0;
    let archivedTotalAmount = 0;

    const partnerMap = new Map<string, number>();
    frequentPartners.forEach(p => partnerMap.set(p.partner, Number(p.count)));

    for (const archive of archives) {
      const isCreator = archive.splitData.creatorWalletAddress === wallet;
      
      if (isCreator) {
        archivedCreated++;
        archivedTotalAmount += Number(archive.splitData.totalAmount) || 0;
      } else {
        const p = archive.participantData.find((p: any) => p.walletAddress === wallet);
        if (p) {
          archivedParticipated++;
          archivedTotalAmount += -(Number(p.amountOwed) || 0);
        }
      }

      // Merge partners
      const creator = archive.splitData.creatorWalletAddress;
      const participants = archive.participantData
        .map((p: any) => p.walletAddress)
        .filter((w: string) => w);
      
      const allInvolved = new Set<string>();
      if (creator) allInvolved.add(creator);
      participants.forEach((p: string) => allInvolved.add(p));
      
      allInvolved.delete(wallet); // Exclude self
      
      allInvolved.forEach(partner => {
        partnerMap.set(partner, (partnerMap.get(partner) || 0) + 1);
      });
    }

    const totalCreated = created + archivedCreated;
    const totalParticipated = participated + archivedParticipated;
    const dbTotal = Number(totalAmount?.total) || 0;
    const finalTotal = dbTotal + archivedTotalAmount;
    
    const dbCount = created + participated;
    const totalCount = dbCount + archives.length;
    
    const finalAvg = totalCount > 0 ? finalTotal / totalCount : 0;

    const mergedPartners = Array.from(partnerMap.entries())
      .map(([partner, count]) => ({ partner, count }))
      .sort((a, b) => b.count - a.count)
      .slice(0, 5);

    return {
      totalSplitsCreated: totalCreated,
      totalSplitsParticipated: totalParticipated,
      averageSplitAmount: finalAvg,
      totalAmount: finalTotal,
      mostFrequentPartners: mergedPartners,
    };
  }

  /**
   * Paginated, filtered history endpoint — the intentional frontend contract.
   * Merges SplitHistory records (completed splits) with SplitArchive records,
   * applies role/status/search/date filters, then paginates in-memory after
   * fetching only the relevant DB rows.
   */
  async getHistory(wallet: string, query: HistoryQueryDto): Promise<HistoryResponseDto> {
    const { role, status, search, dateFrom, dateTo, page, limit } = query;

    // ── 1. Query SplitHistory (completed/settled records) ──────────────────
    const qb = this.repo.createQueryBuilder('sh')
      .leftJoinAndSelect('sh.split', 'split')
      .where('sh.userId = :wallet', { wallet });

    if (role) {
      qb.andWhere('sh.role = :role', { role });
    }

    if (status && status !== HistoryStatusFilter.ALL && status !== HistoryStatusFilter.ARCHIVED) {
      qb.andWhere('split.status = :status', { status });
    }

    if (search) {
      qb.andWhere(
        '(split.description ILIKE :search OR CAST(sh.splitId AS text) ILIKE :search)',
        { search: `%${search}%` },
      );
    }

    if (dateFrom) {
      qb.andWhere('sh.completionTime >= :dateFrom', { dateFrom: new Date(dateFrom) });
    }

    if (dateTo) {
      qb.andWhere('sh.completionTime <= :dateTo', { dateTo: new Date(dateTo) });
    }

    const dbRecords = await qb.orderBy('sh.completionTime', 'DESC').getMany();

    // ── 2. Query SplitArchive when status is ALL or ARCHIVED ───────────────
    let archiveItems: HistoryItemDto[] = [];
    const includeArchived = !status || status === HistoryStatusFilter.ALL || status === HistoryStatusFilter.ARCHIVED;

    if (includeArchived) {
      let archiveQb = this.archiveRepo.createQueryBuilder('archive')
        .where(`archive.splitData ->> 'creatorWalletAddress' = :wallet`, { wallet })
        .orWhere(`archive.participantData @> :participant`, {
          participant: JSON.stringify([{ walletAddress: wallet }]),
        });

      if (dateFrom) {
        archiveQb = archiveQb.andWhere('archive.archivedAt >= :dateFrom', { dateFrom: new Date(dateFrom) });
      }
      if (dateTo) {
        archiveQb = archiveQb.andWhere('archive.archivedAt <= :dateTo', { dateTo: new Date(dateTo) });
      }

      const archives = await archiveQb.orderBy('archive.archivedAt', 'DESC').getMany();

      archiveItems = archives
        .filter((archive) => {
          if (!search) return true;
          const desc: string = archive.splitData?.description ?? '';
          return (
            desc.toLowerCase().includes(search.toLowerCase()) ||
            archive.originalSplitId.includes(search)
          );
        })
        .map((archive): HistoryItemDto => {
          const isCreator = archive.splitData.creatorWalletAddress === wallet;
          const participant = archive.participantData.find((p: any) => p.walletAddress === wallet);
          const itemRole = isCreator ? SplitRole.CREATOR : SplitRole.PARTICIPANT;

          // Skip if role filter doesn't match
          if (role && itemRole !== role) return null as any;

          const rawAmount = isCreator
            ? Number(archive.splitData.totalAmount ?? 0)
            : -(Number(participant?.amountOwed ?? 0));

          return {
            id: archive.id,
            splitId: archive.originalSplitId,
            role: itemRole,
            finalAmount: rawAmount,
            status: 'archived',
            description: archive.splitData?.description,
            preferredCurrency: archive.splitData?.preferredCurrency,
            totalAmount: Number(archive.splitData?.totalAmount ?? 0),
            completionTime: archive.archivedAt,
            comment: archive.archiveReason,
            isArchived: true,
          };
        })
        .filter(Boolean);
    }

    // ── 3. Map DB records to HistoryItemDto ────────────────────────────────
    const dbItems: HistoryItemDto[] = dbRecords.map((sh): HistoryItemDto => ({
      id: sh.id,
      splitId: sh.splitId,
      role: sh.role,
      finalAmount: Number(sh.finalAmount),
      status: sh.split?.status ?? 'completed',
      description: sh.split?.description,
      preferredCurrency: sh.split?.preferredCurrency,
      totalAmount: Number(sh.split?.totalAmount ?? 0),
      completionTime: sh.completionTime,
      comment: sh.comment,
      isArchived: false,
    }));

    // ── 4. Merge, sort, paginate ───────────────────────────────────────────
    const all = [...dbItems, ...archiveItems].sort(
      (a, b) => new Date(b.completionTime).getTime() - new Date(a.completionTime).getTime(),
    );

    const total = all.length;
    const offset = (page - 1) * limit;
    const pageData = all.slice(offset, offset + limit);

    // ── 5. Summary over the full (unfiltered-by-page) result set ──────────
    const summary: HistorySummaryDto = {
      totalSplitsCreated: all.filter((i) => i.role === SplitRole.CREATOR).length,
      totalSplitsParticipated: all.filter((i) => i.role === SplitRole.PARTICIPANT).length,
      totalAmountPaid: all.filter((i) => i.finalAmount < 0).reduce((s, i) => s + Math.abs(i.finalAmount), 0),
      totalAmountReceived: all.filter((i) => i.finalAmount > 0).reduce((s, i) => s + i.finalAmount, 0),
      netAmount: all.reduce((s, i) => s + i.finalAmount, 0),
    };

    return {
      data: pageData,
      total,
      page,
      limit,
      hasMore: offset + limit < total,
      summary,
      exportHint: {
        endpoint: '/api/export',
        supportedFormats: Object.values(ExportFormat),
      },
    };
  }
}
