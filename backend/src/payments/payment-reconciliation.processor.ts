import { Injectable, Logger } from "@nestjs/common";
import { Processor, Process } from "@nestjs/bull";
import { Job } from "bull";
import { PaymentReconciliationService } from "./payment-reconciliation.service";

/**
 * Queue processor for payment reconciliation jobs
 */
@Processor("payment-reconciliation")
@Injectable()
export class PaymentReconciliationProcessor {
  private readonly logger = new Logger(PaymentReconciliationProcessor.name);

  constructor(
    private readonly reconciliationService: PaymentReconciliationService,
  ) {}

  /**
   * Process reconciliation job for a single payment
   */
  @Process("reconcile-payment")
  async handleReconcilePayment(job: Job<{ paymentId: string }>): Promise<void> {
    const { paymentId } = job.data;
    this.logger.log(`Processing reconciliation job for payment: ${paymentId}`);

    try {
      const result = await this.reconciliationService.reconcilePayment(paymentId);
      this.logger.log(
        `Reconciliation result for ${paymentId}: ${result.newStatus}`,
      );

      // If the payment was confirmed or failed, emit final events
      if (
        result.newStatus === "confirmed" ||
        result.newStatus === "failed"
      ) {
        this.logger.log(
          `Payment ${paymentId} reached final state: ${result.newStatus}`,
        );
      }
    } catch (error: unknown) {
      this.logger.error(
        `Failed to reconcile payment ${paymentId}: ${(error as Error).message}`,
      );
      throw error;
    }
  }

  /**
   * Process batch reconciliation job
   */
  @Process("reconcile-batch")
  async handleReconcileBatch(
    job: Job<{ paymentIds: string[] }>,
  ): Promise<void> {
    const { paymentIds } = job.data;
    this.logger.log(
      `Processing batch reconciliation for ${paymentIds.length} payments`,
    );

    const results = await Promise.allSettled(
      paymentIds.map((id: string) => this.reconciliationService.reconcilePayment(id)),
    );

    const successful = results.filter(
      (r: PromiseSettledResult<any>) => r.status === "fulfilled",
    ).length;
    const failed = results.filter(
      (r: PromiseSettledResult<any>) => r.status === "rejected",
    ).length;

    this.logger.log(
      `Batch reconciliation complete: ${successful} successful, ${failed} failed`,
    );

    // If there are failures, we could implement retry logic here
    if (failed > 0) {
      const failedIds = results
        .map((r: PromiseSettledResult<any>, i: number) =>
          r.status === "rejected" ? paymentIds[i] : null,
        )
        .filter(Boolean) as string[];

      this.logger.warn(
        `Failed payment IDs: ${JSON.stringify(failedIds)}`,
      );
    }
  }
}