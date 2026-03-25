import { SplitRole } from '../entities/split-history.entity';

export class HistoryItemDto {
  id!: string;
  splitId!: string;
  role!: SplitRole;
  /** Positive = received, negative = paid out */
  finalAmount!: number;
  status!: string;
  description?: string;
  preferredCurrency?: string;
  totalAmount!: number;
  completionTime!: Date;
  comment?: string;
  isArchived!: boolean;
}

export class HistorySummaryDto {
  totalSplitsCreated!: number;
  totalSplitsParticipated!: number;
  totalAmountPaid!: number;
  totalAmountReceived!: number;
  netAmount!: number;
}

export class HistoryResponseDto {
  data!: HistoryItemDto[];
  total!: number;
  page!: number;
  limit!: number;
  hasMore!: boolean;
  summary!: HistorySummaryDto;
  /** Opaque token for triggering an export of this result set */
  exportHint!: {
    endpoint: string;
    supportedFormats: string[];
  };
}
