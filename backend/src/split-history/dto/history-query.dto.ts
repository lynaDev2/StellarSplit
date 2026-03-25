import {
  IsOptional,
  IsEnum,
  IsString,
  IsDateString,
  IsInt,
  Min,
  Max,
} from 'class-validator';
import { Type } from 'class-transformer';
import { ApiPropertyOptional } from '@nestjs/swagger';
import { SplitRole } from '../entities/split-history.entity';

export enum HistoryStatusFilter {
  ACTIVE = 'active',
  COMPLETED = 'completed',
  PARTIAL = 'partial',
  ARCHIVED = 'archived',
  ALL = 'all',
}

export class HistoryQueryDto {
  @ApiPropertyOptional({ enum: SplitRole, description: 'Filter by user role in the split' })
  @IsOptional()
  @IsEnum(SplitRole)
  role?: SplitRole;

  @ApiPropertyOptional({ enum: HistoryStatusFilter, default: HistoryStatusFilter.ALL })
  @IsOptional()
  @IsEnum(HistoryStatusFilter)
  status?: HistoryStatusFilter = HistoryStatusFilter.ALL;

  @ApiPropertyOptional({ description: 'Search by split description or ID' })
  @IsOptional()
  @IsString()
  search?: string;

  @ApiPropertyOptional({ description: 'Filter from date (ISO 8601)', example: '2024-01-01' })
  @IsOptional()
  @IsDateString()
  dateFrom?: string;

  @ApiPropertyOptional({ description: 'Filter to date (ISO 8601)', example: '2024-12-31' })
  @IsOptional()
  @IsDateString()
  dateTo?: string;

  @ApiPropertyOptional({ default: 1, minimum: 1 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  page: number = 1;

  @ApiPropertyOptional({ default: 20, minimum: 1, maximum: 100 })
  @IsOptional()
  @Type(() => Number)
  @IsInt()
  @Min(1)
  @Max(100)
  limit: number = 20;
}
