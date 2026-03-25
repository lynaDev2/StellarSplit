import { Process, Processor } from "@nestjs/bull";
import { Job } from "bull";
import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { OcrJob, OcrJobStatus } from "../entities/ocr-job.entity";
import { OcrService } from "./ocr.service";

/**
 * OCR Queue Job Data Interface
 */
export interface OcrJobData {
  jobId: string;
  imageBuffer?: string; // Base64 encoded image (for smaller images)
  imageUrl?: string;
  itemId?: string;
  splitId?: string;
  uploadedBy?: string;
  originalFilename?: string;
}

/**
 * OCR Queue Processor using Bull
 * Handles async OCR job processing with retries
 */
@Processor('ocr')
@Injectable()
export class OcrProcessor {
  private readonly logger = new Logger(OcrProcessor.name);

  constructor(
    @InjectRepository(OcrJob)
    private readonly ocrJobRepository: Repository<OcrJob>,
    private readonly ocrService: OcrService,
  ) {}

  /**
   * Process an OCR job from the queue
   */
  @Process({
    concurrency: 3, // Process up to 3 jobs concurrently
  })
  async processOcrJob(job: Job<OcrJobData>): Promise<void> {
    const { jobId, imageUrl, imageBuffer, itemId, splitId, uploadedBy, originalFilename } = job.data;

    this.logger.log(`Processing OCR job ${jobId}`);

    try {
      // Update job status to processing
      const ocrJob = await this.ocrJobRepository.findOne({ where: { id: jobId } });
      if (!ocrJob) {
        this.logger.error(`OCR Job ${jobId} not found in database`);
        return;
      }

      ocrJob.status = OcrJobStatus.PROCESSING;
      ocrJob.progress = 10;
      ocrJob.startedAt = new Date();
      ocrJob.queueJobId = job.id;
      await this.ocrJobRepository.save(ocrJob);

      // Get the image buffer
      let imageBufferData: Buffer;
      if (imageBuffer) {
        imageBufferData = Buffer.from(imageBuffer, 'base64');
      } else if (imageUrl) {
        // Fetch image from URL
        const response = await fetch(imageUrl);
        if (!response.ok) {
          throw new Error(`Failed to fetch image from URL: ${response.statusText}`);
        }
        const arrayBuffer = await response.arrayBuffer();
        imageBufferData = Buffer.from(arrayBuffer);
      } else {
        throw new Error('No image data provided');
      }

      // Update progress
      ocrJob.progress = 30;
      await this.ocrJobRepository.save(ocrJob);

      // Ensure OCR service is initialized
      await this.ocrService.initialize();

      ocrJob.progress = 50;
      await this.ocrJobRepository.save(ocrJob);

      // Perform OCR
      const parsedReceipt = await this.ocrService.scanReceipt(imageBufferData);

      ocrJob.progress = 90;
      
      // Store results
      ocrJob.status = OcrJobStatus.COMPLETED;
      ocrJob.progress = 100;
      ocrJob.confidence = parsedReceipt.confidence;
      ocrJob.totalAmount = parsedReceipt.total;
      ocrJob.subtotal = parsedReceipt.subtotal;
      ocrJob.taxAmount = parsedReceipt.tax;
      ocrJob.tipAmount = parsedReceipt.tip;
      ocrJob.extractedItems = parsedReceipt.items;
      ocrJob.completedAt = new Date();

      // Determine if manual review is needed based on confidence
      if (parsedReceipt.confidence < 0.5) {
        ocrJob.needsManualReview = true;
        ocrJob.status = OcrJobStatus.NEEDS_REVIEW;
      }

      await this.ocrJobRepository.save(ocrJob);

      this.logger.log(`OCR job ${jobId} completed with confidence ${parsedReceipt.confidence.toFixed(2)}`);
    } catch (error) {
      this.logger.error(`OCR job ${jobId} failed:`, error);

      // Update job with error
      const ocrJob = await this.ocrJobRepository.findOne({ where: { id: jobId } });
      if (ocrJob) {
        ocrJob.status = OcrJobStatus.FAILED;
        ocrJob.errorMessage = error instanceof Error ? error.message : 'Unknown error';
        ocrJob.retryCount += 1;
        
        // If retries are still available, the job will be retried by Bull
        // If max retries reached, mark as failed permanently
        if (ocrJob.retryCount >= ocrJob.maxRetries) {
          this.logger.error(`OCR job ${jobId} exceeded max retries`);
        }
        
        await this.ocrJobRepository.save(ocrJob);
      }

      // Re-throw to let Bull handle retry
      throw error;
    }
  }
}