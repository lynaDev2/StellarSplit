import { Controller, Post, Get, Body, Param, Query, ValidationPipe, UseGuards } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam, ApiQuery } from '@nestjs/swagger';
import { OcrQueueService } from './ocr-queue.service';
import { CreateOcrJobDto } from './dto/ocr-job.dto';
import { OcrJob, OcrJobStatus } from './entities/ocr-job.entity';

@ApiTags('OCR')
@Controller('ocr')
export class OcrController {
  constructor(private readonly ocrQueueService: OcrQueueService) {}

  /**
   * Create a new OCR job
   * Accepts image data either as URL or file upload
   */
  @Post('jobs')
  @ApiOperation({ summary: 'Create a new OCR job' })
  @ApiResponse({ status: 201, description: 'OCR job created', type: OcrJob })
  async createJob(
    @Body(ValidationPipe) dto: CreateOcrJobDto,
  ): Promise<OcrJob> {
    return this.ocrQueueService.createJob(dto);
  }

  /**
   * Get OCR job details by ID
   */
  @Get('jobs/:id')
  @ApiOperation({ summary: 'Get OCR job details by ID' })
  @ApiParam({ name: 'id', description: 'OCR Job ID (UUID)' })
  @ApiResponse({ status: 200, description: 'Job details', type: OcrJob })
  async getJob(@Param('id') id: string): Promise<OcrJob> {
    return this.ocrQueueService.getJob(id);
  }

  /**
   * Get OCR job progress
   */
  @Get('jobs/:id/progress')
  @ApiOperation({ summary: 'Get OCR job progress' })
  @ApiParam({ name: 'id', description: 'OCR Job ID (UUID)' })
  @ApiResponse({ status: 200, description: 'Job progress' })
  async getJobProgress(@Param('id') id: string): Promise<{ progress: number; status: string }> {
    return this.ocrQueueService.getJobProgress(id);
  }

  /**
   * Get all jobs needing manual review
   */
  @Get('jobs/review/needs')
  @ApiOperation({ summary: 'Get all OCR jobs that need manual review' })
  @ApiResponse({ status: 200, description: 'List of jobs needing review', type: [OcrJob] })
  async getJobsNeedingReview(): Promise<OcrJob[]> {
    return this.ocrQueueService.getJobsNeedingReview();
  }

  /**
   * Retry a failed OCR job
   */
  @Post('jobs/:id/retry')
  @ApiOperation({ summary: 'Retry a failed OCR job' })
  @ApiParam({ name: 'id', description: 'OCR Job ID (UUID)' })
  @ApiResponse({ status: 200, description: 'Job retry initiated', type: OcrJob })
  async retryJob(@Param('id') id: string): Promise<OcrJob> {
    return this.ocrQueueService.retryJob(id);
  }

  /**
   * Cancel a pending OCR job
   */
  @Post('jobs/:id/cancel')
  @ApiOperation({ summary: 'Cancel a pending OCR job' })
  @ApiParam({ name: 'id', description: 'OCR Job ID (UUID)' })
  @ApiResponse({ status: 204, description: 'Job cancelled' })
  async cancelJob(@Param('id') id: string): Promise<void> {
    return this.ocrQueueService.cancelJob(id);
  }

  /**
   * Get queue statistics
   */
  @Get('stats')
  @ApiOperation({ summary: 'Get OCR queue statistics' })
  @ApiResponse({ status: 200, description: 'Queue stats' })
  async getStats(): Promise<{
    pending: number;
    processing: number;
    completed: number;
    failed: number;
    needsReview: number;
  }> {
    return this.ocrQueueService.getQueueStats();
  }

  /**
   * Get failed jobs
   */
  @Get('jobs/failed')
  @ApiOperation({ summary: 'Get all failed OCR jobs' })
  @ApiResponse({ status: 200, description: 'List of failed jobs', type: [OcrJob] })
  async getFailedJobs(): Promise<OcrJob[]> {
    return this.ocrQueueService.getFailedJobs();
  }

  /**
   * Manually trigger processing for a job
   */
  @Post('jobs/:id/process')
  @ApiOperation({ summary: 'Manually trigger processing for a job' })
  @ApiParam({ name: 'id', description: 'OCR Job ID (UUID)' })
  @ApiResponse({ status: 200, description: 'Processing triggered', type: OcrJob })
  async processJobManually(@Param('id') id: string): Promise<OcrJob> {
    return this.ocrQueueService.processJobManually(id);
  }
}