// Analytics read endpoints for dashboard consumers
import { Controller, Get, Post, Query, Body, Param, HttpStatus, HttpCode } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiQuery, ApiBearerAuth } from '@nestjs/swagger';
import { IsOptional, IsDateString, IsString, IsArray, Min, Max } from 'class-validator';
import { AnalyticsIngestService } from './analytics-ingest.service';
import { AnalyticsEventType } from './analytics-events';

class QueryMetricsDto {
  @IsOptional()
  @IsDateString()
  dateFrom?: string;

  @IsOptional()
  @IsDateString()
  dateTo?: string;

  @IsOptional()
  @IsString()
  eventType?: string;
}

class QueryFunnelDto {
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  eventTypes?: string[];

  @IsOptional()
  @IsDateString()
  dateFrom?: string;

  @IsOptional()
  @IsDateString()
  dateTo?: string;
}

class QueryRetentionDto {
  @IsOptional()
  @IsString()
  eventType?: string;

  @IsOptional()
  @IsDateString()
  startDate?: string;

  @IsOptional()
  @Min(1)
  @Max(90)
  periods?: number;
}

class QueryTrendsDto {
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  eventTypes?: string[];

  @IsOptional()
  @IsDateString()
  dateFrom?: string;

  @IsOptional()
  @IsDateString()
  dateTo?: string;

  @IsOptional()
  @IsString()
  interval?: 'hour' | 'day' | 'week' | 'month';
}

class HealthStatusDto {
  operational: boolean;
  features: Record<string, boolean>;
  timestamp: string;
}

@ApiTags('Analytics')
@Controller('analytics')
export class AnalyticsController {
  constructor(private readonly analyticsIngest: AnalyticsIngestService) {}

  /**
   * Health check for analytics service
   * Returns operational status and feature availability
   */
  @Get('health')
  @ApiOperation({ summary: 'Check analytics service health' })
  @ApiResponse({ status: HttpStatus.OK, description: 'Analytics service status', type: HealthStatusDto })
  getHealth(): HealthStatusDto {
    return {
      operational: this.analyticsIngest.isOperational(),
      features: this.analyticsIngest.getFeatures(),
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Query metrics with filters
   */
  @Get('metrics')
  @ApiOperation({ summary: 'Query analytics metrics' })
  @ApiQuery({ name: 'dateFrom', required: false, type: String })
  @ApiQuery({ name: 'dateTo', required: false, type: String })
  @ApiQuery({ name: 'eventType', required: false, type: String })
  @ApiResponse({ status: HttpStatus.OK, description: 'Metrics data' })
  async queryMetrics(@Query() query: QueryMetricsDto): Promise<Record<string, unknown>> {
    const dateFrom = query.dateFrom ? new Date(query.dateFrom) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const dateTo = query.dateTo ? new Date(query.dateTo) : new Date();

    return this.analyticsIngest.queryAnalytics(this.buildMetricsQuery(dateFrom, dateTo, query.eventType));
  }

  /**
   * Query funnel analysis
   */
  @Post('funnel')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Query funnel analysis' })
  @ApiResponse({ status: HttpStatus.OK, description: 'Funnel conversion data' })
  async queryFunnel(@Body() query: QueryFunnelDto): Promise<Record<string, unknown>> {
    const eventTypes = query.eventTypes || [
      AnalyticsEventType.USER_REGISTERED,
      AnalyticsEventType.SPLIT_CREATED,
      AnalyticsEventType.PAYMENT_INITIATED,
      AnalyticsEventType.PAYMENT_COMPLETED,
    ];

    const dateFrom = query.dateFrom ? new Date(query.dateFrom) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const dateTo = query.dateTo ? new Date(query.dateTo) : new Date();

    return this.analyticsIngest.queryAnalytics(this.buildFunnelQuery(eventTypes, dateFrom, dateTo));
  }

  /**
   * Query retention analysis
   */
  @Get('retention')
  @ApiOperation({ summary: 'Query retention analysis' })
  @ApiQuery({ name: 'eventType', required: false, type: String })
  @ApiQuery({ name: 'startDate', required: false, type: String })
  @ApiQuery({ name: 'periods', required: false, type: Number })
  @ApiResponse({ status: HttpStatus.OK, description: 'Retention cohort data' })
  async queryRetention(@Query() query: QueryRetentionDto): Promise<Record<string, unknown>> {
    const eventType = query.eventType || AnalyticsEventType.USER_REGISTERED;
    const startDate = query.startDate ? new Date(query.startDate) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const periods = query.periods || 7;

    return this.analyticsIngest.queryAnalytics(this.buildRetentionQuery(eventType, startDate, periods));
  }

  /**
   * Query trends over time
   */
  @Get('trends')
  @ApiOperation({ summary: 'Query event trends over time' })
  @ApiQuery({ name: 'eventTypes', required: false, type: [String] })
  @ApiQuery({ name: 'dateFrom', required: false, type: String })
  @ApiQuery({ name: 'dateTo', required: false, type: String })
  @ApiQuery({ name: 'interval', required: false, enum: ['hour', 'day', 'week', 'month'] })
  @ApiResponse({ status: HttpStatus.OK, description: 'Time series data' })
  async queryTrends(@Query() query: QueryTrendsDto): Promise<Record<string, unknown>> {
    const eventTypes = query.eventTypes || [AnalyticsEventType.SPLIT_CREATED];
    const dateFrom = query.dateFrom ? new Date(query.dateFrom) : new Date(Date.now() - 7 * 24 * 60 * 60 * 1000);
    const dateTo = query.dateTo ? new Date(query.dateTo) : new Date();
    const interval = query.interval || 'day';

    return this.analyticsIngest.queryAnalytics(this.buildTrendsQuery(eventTypes, dateFrom, dateTo, interval));
  }

  /**
   * Get daily summary for dashboard
   */
  @Get('dashboard/daily')
  @ApiOperation({ summary: 'Get daily summary for dashboard' })
  @ApiQuery({ name: 'date', required: false, type: String })
  @ApiResponse({ status: HttpStatus.OK, description: 'Daily summary' })
  async getDailySummary(@Query('date') date?: string): Promise<Record<string, unknown>> {
    const targetDate = date ? new Date(date) : new Date();
    return this.analyticsIngest.getDailyMetrics(targetDate);
  }

  private buildMetricsQuery(dateFrom: Date, dateTo: Date, eventType?: string): string {
    const dateFromStr = dateFrom.toISOString().slice(0, 10);
    const dateToStr = dateTo.toISOString().slice(0, 10);
    let sql = `
      SELECT
        toDate(timestamp) as date,
        type,
        count() as count,
        uniq(actor_user_id) as unique_users
      FROM analytics_events
      WHERE timestamp >= '${dateFromStr}' AND timestamp < '${dateToStr}'
    `;

    if (eventType) {
      sql += ` AND type = '${eventType}'`;
    }

    sql += `
      GROUP BY date, type
      ORDER BY date DESC, count DESC
    `;

    return sql;
  }

  private buildFunnelQuery(eventTypes: string[], dateFrom: Date, dateTo: Date): string {
    const dateFromStr = dateFrom.toISOString().slice(0, 10);
    const dateToStr = dateTo.toISOString().slice(0, 10);
    const eventTypeList = eventTypes.map((t) => `'${t}'`).join(', ');

    return `
      SELECT
        type,
        count() as count,
        uniq(actor_user_id) as unique_users,
        count() / lagInFrame(count()) over (ORDER BY count() DESC) as conversion_rate
      FROM analytics_events
      WHERE timestamp >= '${dateFromStr}' AND timestamp < '${dateToStr}'
        AND type IN (${eventTypeList})
      GROUP BY type
      ORDER BY count DESC
    `;
  }

  private buildRetentionQuery(eventType: string, startDate: Date, periods: number): string {
    const startDateStr = startDate.toISOString().slice(0, 10);

    return `
      SELECT
        toStartOfInterval(timestamp, INTERVAL 1 DAY) as cohort_date,
        formatDateTime(timestamp, '%Y-%m-%d') as return_date,
        uniq(actor_user_id) as retained_users
      FROM analytics_events
      WHERE timestamp >= '${startDateStr}'
        AND type = '${eventType}'
      GROUP BY cohort_date, return_date
      ORDER BY cohort_date, return_date
      LIMIT ${periods}
    `;
  }

  private buildTrendsQuery(eventTypes: string[], dateFrom: Date, dateTo: Date, interval: string): string {
    const dateFromStr = dateFrom.toISOString().slice(0, 10);
    const dateToStr = dateTo.toISOString().slice(0, 10);
    const eventTypeList = eventTypes.map((t) => `'${t}'`).join(', ');
    const intervalMap: Record<string, string> = {
      hour: 'INTERVAL 1 HOUR',
      day: 'INTERVAL 1 DAY',
      week: 'INTERVAL 1 WEEK',
      month: 'INTERVAL 1 MONTH',
    };

    return `
      SELECT
        toStartOfInterval(timestamp, ${intervalMap[interval]}) as period,
        type,
        count() as count,
        uniq(actor_user_id) as unique_users
      FROM analytics_events
      WHERE timestamp >= '${dateFromStr}' AND timestamp < '${dateToStr}'
        AND type IN (${eventTypeList})
      GROUP BY period, type
      ORDER BY period DESC, count DESC
    `;
  }
}
