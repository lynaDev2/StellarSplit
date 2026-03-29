import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository, LessThanOrEqual } from "typeorm";

import { BatchJob, BatchJobType, BatchJobStatus } from "../entities/batch-job.entity";
import { BatchOperation, BatchOperationStatus } from "../entities/batch-operation.entity";
import { BatchProgressService } from "../batch-progress.service";

interface ScheduledJobData {
  batchId: string;
  taskType: string;
  params?: Record<string, any>;
}

@Processor("batch_scheduled")
export class ScheduledBatchProcessor {
  private readonly logger = new Logger(ScheduledBatchProcessor.name);

  constructor(
    @InjectRepository(BatchJob)
    private batchJobRepository: Repository<BatchJob>,
    @InjectRepository(BatchOperation)
    private batchOperationRepository: Repository<BatchOperation>,
    private batchProgressService: BatchProgressService,
  ) {}

  @Process("daily_reconciliation")
  async handleDailyReconciliation(job: Job<ScheduledJobData>): Promise<void> {
    const { batchId } = job.data;
    this.logger.log(`Starting daily reconciliation for batch ${batchId}`);

    try {
      await this.executeScheduledTask(batchId, "daily_reconciliation", async () => {
        const [pending, failed, completed] = await Promise.all([
          this.batchOperationRepository.count({ where: { status: BatchOperationStatus.PENDING } }),
          this.batchOperationRepository.count({ where: { status: BatchOperationStatus.FAILED } }),
          this.batchOperationRepository.count({ where: { status: BatchOperationStatus.COMPLETED } }),
        ]);

        return {
          pendingOperations: pending,
          failedOperations: failed,
          completedOperations: completed,
          reconciledAt: new Date().toISOString(),
        };
      });

      this.logger.log(`Completed daily reconciliation for batch ${batchId}`);
    } catch (error: any) {
      this.logger.error(`Daily reconciliation failed: ${error.message}`);
      throw error;
    }
  }

  @Process("weekly_summary")
  async handleWeeklySummary(job: Job<ScheduledJobData>): Promise<void> {
    const { batchId } = job.data;
    this.logger.log(`Starting weekly summary for batch ${batchId}`);

    try {
      await this.executeScheduledTask(batchId, "weekly_summary", async () => {
        const oneWeekAgo = new Date();
        oneWeekAgo.setDate(oneWeekAgo.getDate() - 7);

        const [totalBatches, completedBatches, failedBatches] = await Promise.all([
          this.batchJobRepository.count(),
          this.batchJobRepository.count({ where: { status: BatchJobStatus.COMPLETED } }),
          this.batchJobRepository.count({ where: { status: BatchJobStatus.FAILED } }),
        ]);

        return {
          totalBatches,
          completedBatches,
          failedBatches,
          periodStart: oneWeekAgo.toISOString(),
          periodEnd: new Date().toISOString(),
          reportGenerated: true,
        };
      });

      this.logger.log(`Completed weekly summary for batch ${batchId}`);
    } catch (error: any) {
      this.logger.error(`Weekly summary failed: ${error.message}`);
      throw error;
    }
  }

  @Process("monthly_analytics")
  async handleMonthlyAnalytics(job: Job<ScheduledJobData>): Promise<void> {
    const { batchId } = job.data;
    this.logger.log(`Starting monthly analytics for batch ${batchId}`);

    try {
      await this.executeScheduledTask(batchId, "monthly_analytics", async () => {
        const thirtyDaysAgo = new Date();
        thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);

        const [totalOps, completedOps, failedOps] = await Promise.all([
          this.batchOperationRepository.count(),
          this.batchOperationRepository.count({ where: { status: BatchOperationStatus.COMPLETED } }),
          this.batchOperationRepository.count({ where: { status: BatchOperationStatus.FAILED } }),
        ]);

        const successRate = totalOps > 0 ? completedOps / totalOps : 0;

        return {
          totalOperations: totalOps,
          completedOperations: completedOps,
          failedOperations: failedOps,
          successRate: Math.round(successRate * 100) / 100,
          periodStart: thirtyDaysAgo.toISOString(),
          periodEnd: new Date().toISOString(),
          reportGenerated: true,
        };
      });

      this.logger.log(`Completed monthly analytics for batch ${batchId}`);
    } catch (error: any) {
      this.logger.error(`Monthly analytics failed: ${error.message}`);
      throw error;
    }
  }

  @Process("cleanup_old_batches")
  async handleCleanup(job: Job<ScheduledJobData>): Promise<void> {
    const { batchId, params } = job.data;
    const retentionDays = params?.retentionDays || 30;

    this.logger.log(`Starting cleanup for batches older than ${retentionDays} days`);

    try {
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - retentionDays);

      // Find old completed batches using proper LessThanOrEqual comparison
      const oldBatches = await this.batchJobRepository.find({
        where: {
          status: BatchJobStatus.COMPLETED,
          completed_at: LessThanOrEqual(cutoffDate),
        },
      });

      // Delete associated operations first
      for (const batch of oldBatches) {
        await this.batchOperationRepository.delete({ batch_id: batch.id });
      }

      // Delete the batches themselves
      if (oldBatches.length > 0) {
        const ids = oldBatches.map((b) => b.id);
        await this.batchJobRepository.delete(ids);
      }

      this.logger.log(`Cleaned up ${oldBatches.length} old batches`);

      await this.executeScheduledTask(batchId, "cleanup_old_batches", async () => ({
        deletedBatches: oldBatches.length,
        retentionDays,
        cutoffDate: cutoffDate.toISOString(),
      }));
    } catch (error: any) {
      this.logger.error(`Cleanup failed: ${error.message}`);
      throw error;
    }
  }

  /**
   * Execute a scheduled task with proper tracking
   */
  private async executeScheduledTask(
    batchId: string,
    taskType: string,
    taskFn: () => Promise<Record<string, any>>,
  ): Promise<void> {
    await this.batchJobRepository.update(batchId, {
      status: BatchJobStatus.PROCESSING,
      started_at: new Date(),
    });

    const operation = this.batchOperationRepository.create({
      batch_id: batchId,
      operation_index: 0,
      status: BatchOperationStatus.PROCESSING,
      payload: { taskType },
    });

    await this.batchOperationRepository.save(operation);
    await this.batchProgressService.markOperationStarted(operation.id);

    try {
      const result = await taskFn();

      await this.batchProgressService.markOperationCompleted(operation.id, result);

      await this.batchJobRepository.update(batchId, {
        status: BatchJobStatus.COMPLETED,
        completed_at: new Date(),
      });
    } catch (error: any) {
      await this.batchProgressService.markOperationFailed(
        operation.id,
        error.message,
        "SCHEDULED_TASK_ERROR",
      );

      await this.batchJobRepository.update(batchId, {
        status: BatchJobStatus.FAILED,
        error_message: error.message,
        completed_at: new Date(),
      });

      throw error;
    }
  }

  /**
   * Schedule a new recurring batch job
   */
  async scheduleRecurringJob(
    queue: any,
    taskType: string,
    cronExpression: string,
    params?: Record<string, any>,
  ): Promise<string> {
    const batch = this.batchJobRepository.create({
      type: BatchJobType.SCHEDULED_TASK,
      status: BatchJobStatus.PENDING,
      total_operations: 1,
      options: { taskType, cronExpression, params },
    });

    const savedBatch = await this.batchJobRepository.save(batch);

    await queue.add(
      taskType,
      { batchId: savedBatch.id, taskType, params },
      {
        repeat: { cron: cronExpression },
        jobId: `${taskType}_${savedBatch.id}`,
      },
    );

    this.logger.log(`Scheduled ${taskType} job with cron: ${cronExpression}`);

    return savedBatch.id;
  }
}
