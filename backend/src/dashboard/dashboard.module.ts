import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { Activity } from '../entities/activity.entity';
import { DashboardService } from './dashboard.service';
import { DashboardController } from './dashboard.controller';

@Module({
  imports: [TypeOrmModule.forFeature([Participant, Split, Activity])],
  controllers: [DashboardController],
  providers: [DashboardService],
})
export class DashboardModule {}
