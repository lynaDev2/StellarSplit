export class DashboardSummaryDto {
  /** Total amount the user owes across all active splits */
  totalOwed: number;
  /** Total amount owed to the user across all active splits */
  totalOwedToUser: number;
  /** Number of active (non-completed) splits the user participates in */
  activeSplits: number;
  /** Number of splits the user created that are still active */
  splitsCreated: number;
  /** Number of unread activity notifications */
  unreadNotifications: number;
  /** Quick-action metadata for the frontend */
  quickActions: QuickAction[];
}

export class QuickAction {
  id: string;
  label: string;
  route: string;
  badge?: number;
}

export class DashboardActivityItem {
  id: string;
  activityType: string;
  splitId?: string;
  metadata: Record<string, any>;
  isRead: boolean;
  createdAt: Date;
}

export class DashboardActivityDto {
  data: DashboardActivityItem[];
  total: number;
  page: number;
  limit: number;
  hasMore: boolean;
  unreadCount: number;
}
