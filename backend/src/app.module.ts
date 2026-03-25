// app.module.ts
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { BullModule } from '@nestjs/bull';
import { EventEmitterModule } from '@nestjs/event-emitter';

import * as dotenv from 'dotenv';
import * as path from 'path';

import databaseConfig from './config/database.config';
import appConfig from './config/app.config';
import { getRedisConnectionOptions } from './config/redis.config';

import { HealthModule } from './modules/health/health.module';
import { StellarModule } from './stellar/stellar.module';
import { PaymentsModule } from './payments/payments.module';
import { CurrencyModule } from './modules/currency/currency.module';
import { SplitsModule } from './modules/splits/splits.module';
import { ItemsModule } from './modules/items/items.module';
import { EmailModule } from './email/email.module';
import { RecurringSplitsModule } from './recurring-splits/recurring-splits.module';
import { ReceiptsModule } from './receipts/receipts.module';
import { SplitHistoryModule } from './split-history/split-history.module';
import { ActivitiesModule } from './modules/activities/activities.module';
import { SearchModule } from './search/search.module';
import { FriendshipModule } from './friendship/friendship.module';
import { MentionsModule } from './mentions/mentions.module';
import { SplitCommentsModule } from './split-comments/split-comments.module';
import { AnalyticsModule } from "./analytics/analytics.module";
import { ExportModule } from './export/export.module';
import { WebhooksModule } from "./webhooks/webhooks.module";
import { DisputesModule } from './disputes/disputes.module';
import { GovernanceModule } from './governance/governance.module';
import { ComplianceModule } from './compliance/compliance.module';
import { SettlementModule } from "./settlement/settlement.module";
import { TemplatesModule } from "./templates/templates.module";
import { PushNotificationsModule } from './push-notifications/push-notifications.module';
import { ArchivingModule } from './modules/archiving/archiving.module';
import { GatewayModule } from "./gateway/gateway.module";
import { SchedulerModule } from "./modules/scheduler/scheduler.module";
import { UploadModule } from "./uploads/upload.module";
import { ProfileModule } from "./profile/profile.module";
import { InvitationsModule } from "./invitations/invitations.module";
import { CommonModule } from "./common/common.module";
import { DebtSimplificationModule } from "./debt-simplification/debt-simplification.module";
import { OcrModule } from "./ocr/ocr.module";
import { DashboardModule } from "./dashboard/dashboard.module";
// Load environment variables
dotenv.config({
  path: path.resolve(__dirname, '../.env'),
});

@Module({
  imports: [
    // ✅ Config
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: [".env", ".env.local"],
      load: [appConfig, databaseConfig],
    }),

    // ✅ Event system (mentions, activity feed, etc.)
    EventEmitterModule.forRoot(),

    // ✅ Database
    TypeOrmModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => {
        const dbConfig = configService.get("database");
        return {
          type: "postgres",
          host: dbConfig.host,
          port: dbConfig.port,
          username: dbConfig.username,
          password: dbConfig.password,
          database: dbConfig.name,
          entities: [path.join(__dirname, "**/*.entity{.ts,.js}")],
          synchronize: dbConfig.synchronize,
          logging: dbConfig.logging,
        };
      },
    }),

    // ✅ Queue / background jobs
    BullModule.forRootAsync({
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        redis: getRedisConnectionOptions(configService),
      }),
    }),

    // ✅ Feature modules
    HealthModule,
    StellarModule,
    PaymentsModule,
    CurrencyModule,
    SplitsModule,
    ItemsModule,
    EmailModule,
    RecurringSplitsModule,
    ReceiptsModule,
    SplitHistoryModule,
    ActivitiesModule,
    SearchModule,
    FriendshipModule,
    MentionsModule,
    SplitCommentsModule,
    // Analytics module for user spending & reports
    AnalyticsModule,
    ExportModule,
    // Webhooks module for external event notifications
    WebhooksModule,
    // Dispute resolution system for split conflicts
    DisputesModule,
    // DAO Governance system for platform decisions
    GovernanceModule,
    // Compliance module for tax reporting and exports
    ComplianceModule,
    SettlementModule,
    TemplatesModule,
    PushNotificationsModule,
    ArchivingModule,
    GatewayModule,
    SchedulerModule,
    UploadModule,
    ProfileModule,
    InvitationsModule,
    OcrModule,
    CommonModule,
    DebtSimplificationModule,
    DashboardModule,
  ],
})
export class AppModule { }
