import { Module } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { CacheModule } from "@nestjs/cache-manager";
import { BullModule } from "@nestjs/bull";
import { ConfigModule, ConfigService } from "@nestjs/config";
import { AnalyticsService } from "./analytics.service";
import { AnalyticsScheduler } from "./analytics.scheduler";
// import { AnalyticsProcessor } from "./analytics.processor";
import { Payment } from "../entities/payment.entity";
import { Participant } from "../entities/participant.entity";
import { User } from "../entities/user.entity";
import { AnalyticsReport } from "./reports.entity";
import { AnalyticsController } from "./analytics.controller";
import { AnalyticsProcessor } from "./analytics.processor";
import { getRedisConnectionOptions } from "../config/redis.config";

@Module({
  imports: [
    TypeOrmModule.forFeature([Payment, Participant, AnalyticsReport, User]),
    CacheModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        ttl: configService.get<number>("ANALYTICS_CACHE_TTL", 300),
        isGlobal: false,
        store: require("cache-manager-redis-store"),
        ...getRedisConnectionOptions(configService),
      }),
    }),
    BullModule.registerQueue({ name: "analytics-export" }),
  ],
  controllers: [AnalyticsController],
  providers: [AnalyticsService, AnalyticsScheduler, AnalyticsProcessor],
  exports: [AnalyticsService],
})
export class AnalyticsModule {}
