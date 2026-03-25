import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { Activity } from '../entities/activity.entity';
import { DashboardSummaryDto, DashboardActivityDto, QuickAction } from './dto/dashboard.dto';

@Injectable()
export class DashboardService {
  constructor(
    @InjectRepository(Participant)
    private readonly participantRepo: Repository<Participant>,
    @InjectRepository(Split)
    private readonly splitRepo: Repository<Split>,
    @InjectRepository(Activity)
    private readonly activityRepo: Repository<Activity>,
  ) {}

  async getSummary(userId: string): Promise<DashboardSummaryDto> {
    // Run all aggregation queries in parallel for efficiency
    const [owedResult, owedToUserResult, activeSplitsCount, splitsCreatedCount, unreadCount] =
      await Promise.all([
        // Total the user owes (amountOwed - amountPaid) across non-completed splits
        this.participantRepo
          .createQueryBuilder('p')
          .select('COALESCE(SUM((p.amountOwed - p.amountPaid)::numeric), 0)', 'total')
          .innerJoin(Split, 's', 's.id = p.splitId')
          .where('p.userId = :userId', { userId })
          .andWhere("p.status != 'paid'")
          .andWhere("s.status != 'completed'")
          .andWhere('s.deletedAt IS NULL')
          .getRawOne<{ total: string }>(),

        // Total owed to the user: sum of what others owe on splits the user created
        this.participantRepo
          .createQueryBuilder('p')
          .select('COALESCE(SUM((p.amountOwed - p.amountPaid)::numeric), 0)', 'total')
          .innerJoin(Split, 's', 's.id = p.splitId')
          .where('s.creatorWalletAddress = :userId', { userId })
          .andWhere('p.userId != :userId', { userId })
          .andWhere("p.status != 'paid'")
          .andWhere("s.status != 'completed'")
          .andWhere('s.deletedAt IS NULL')
          .getRawOne<{ total: string }>(),

        // Active splits the user participates in
        this.participantRepo
          .createQueryBuilder('p')
          .innerJoin(Split, 's', 's.id = p.splitId')
          .where('p.userId = :userId', { userId })
          .andWhere("s.status != 'completed'")
          .andWhere('s.deletedAt IS NULL')
          .getCount(),

        // Splits the user created that are still active
        this.splitRepo
          .createQueryBuilder('s')
          .where('s.creatorWalletAddress = :userId', { userId })
          .andWhere("s.status != 'completed'")
          .andWhere('s.deletedAt IS NULL')
          .getCount(),

        // Unread activity count
        this.activityRepo.count({ where: { userId, isRead: false } }),
      ]);

    const totalOwed = parseFloat(owedResult?.total ?? '0');
    const totalOwedToUser = parseFloat(owedToUserResult?.total ?? '0');

    const quickActions: QuickAction[] = [
      { id: 'new-split', label: 'New Split', route: '/splits/new' },
      { id: 'my-splits', label: 'My Splits', route: '/splits', badge: activeSplitsCount },
      { id: 'activity', label: 'Activity', route: '/activity', badge: unreadCount || undefined },
      { id: 'analytics', label: 'Analytics', route: '/analytics' },
    ];

    return {
      totalOwed,
      totalOwedToUser,
      activeSplits: activeSplitsCount,
      splitsCreated: splitsCreatedCount,
      unreadNotifications: unreadCount,
      quickActions,
    };
  }

  async getActivity(
    userId: string,
    page: number,
    limit: number,
  ): Promise<DashboardActivityDto> {
    const [data, total] = await this.activityRepo.findAndCount({
      where: { userId },
      order: { createdAt: 'DESC' },
      skip: (page - 1) * limit,
      take: limit,
    });

    const unreadCount = await this.activityRepo.count({
      where: { userId, isRead: false },
    });

    return {
      data: data.map((a) => ({
        id: a.id,
        activityType: a.activityType,
        splitId: a.splitId,
        metadata: a.metadata,
        isRead: a.isRead,
        createdAt: a.createdAt,
      })),
      total,
      page,
      limit,
      hasMore: page * limit < total,
      unreadCount,
    };
  }
}
