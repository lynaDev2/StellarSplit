import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SimplifiedDebt } from './entities/simplified-debt.entity';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { DebtGraphService } from './debt-graph.service';
import { DebtSimplificationService } from './debt-simplification.service';
import { DebtSimplificationController } from './debt-simplification.controller';

@Module({
  imports: [
    TypeOrmModule.forFeature([SimplifiedDebt, Participant, Split]),
  ],
  providers: [DebtGraphService, DebtSimplificationService],
  controllers: [DebtSimplificationController],
  exports: [DebtSimplificationService],
})
export class DebtSimplificationModule {}
