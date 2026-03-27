import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AnalyticsIngestService } from './analytics-ingest.service';
import { AnalyticsController } from './analytics.controller';

@Module({
  imports: [ConfigModule],
  controllers: [AnalyticsController],
  providers: [AnalyticsIngestService],
  exports: [AnalyticsIngestService],
})
export class RealtimeAnalyticsModule {}
