import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  UpdateDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';

/**
 * OCR Job status enumeration
 */
export enum OcrJobStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed',
  NEEDS_REVIEW = 'needs_review',
}

/**
 * Entity to persist OCR job status and results
 */
@Entity('ocr_jobs')
export class OcrJob {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  /** ID of the receipt/item this OCR is for */
  @Column({ type: 'uuid', nullable: true, name: 'item_id' })
  itemId?: string;

  /** ID of the split this receipt belongs to */
  @Column({ type: 'uuid', nullable: true, name: 'split_id' })
  splitId?: string;

  /** Current status of the OCR job */
  @Column({
    type: 'varchar',
    default: OcrJobStatus.PENDING,
    name: 'status',
  })
  status!: OcrJobStatus;

  /** Progress percentage (0-100) */
  @Column({ type: 'int', default: 0, name: 'progress' })
  progress!: number;

  /** Error message if job failed */
  @Column({ type: 'text', nullable: true, name: 'error_message' })
  errorMessage?: string;

  /** Number of retry attempts */
  @Column({ type: 'int', default: 0, name: 'retry_count' })
  retryCount!: number;

  /** Maximum retry attempts allowed */
  @Column({ type: 'int', default: 3, name: 'max_retries' })
  maxRetries!: number;

  /** Whether the result needs manual review */
  @Column({ type: 'boolean', default: false, name: 'needs_manual_review' })
  needsManualReview!: boolean;

  /** Confidence score of the OCR result (0-1) */
  @Column({ type: 'decimal', precision: 3, scale: 2, nullable: true, name: 'confidence' })
  confidence?: number;

  /** Extracted total amount */
  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true, name: 'total_amount' })
  totalAmount?: number;

  /** Extracted subtotal */
  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true, name: 'subtotal' })
  subtotal?: number;

  /** Extracted tax amount */
  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true, name: 'tax_amount' })
  taxAmount?: number;

  /** Extracted tip amount */
  @Column({ type: 'decimal', precision: 10, scale: 2, nullable: true, name: 'tip_amount' })
  tipAmount?: number;

  /** Extracted items as JSON */
  @Column({ type: 'jsonb', nullable: true, name: 'extracted_items' })
  extractedItems?: Array<{
    name: string;
    quantity: number;
    price: number;
  }>;

  /** Raw OCR text for manual review */
  @Column({ type: 'text', nullable: true, name: 'raw_ocr_text' })
  rawOcrText?: string;

  /** ID of the user who uploaded the image */
  @Column({ type: 'uuid', nullable: true, name: 'uploaded_by' })
  uploadedBy?: string;

  /** Original filename */
  @Column({ type: 'varchar', nullable: true, name: 'original_filename' })
  originalFilename?: string;

  /** URL to the stored image */
  @Column({ type: 'text', nullable: true, name: 'image_url' })
  imageUrl?: string;

  /** Queue job ID (Bull queue reference) */
  @Column({ type: 'varchar', nullable: true, name: 'queue_job_id' })
  queueJobId?: string;

  /** When the job started processing */
  @Column({ type: 'timestamp', nullable: true, name: 'started_at' })
  startedAt?: Date;

  /** When the job completed */
  @Column({ type: 'timestamp', nullable: true, name: 'completed_at' })
  completedAt?: Date;

  @CreateDateColumn({ name: 'created_at' })
  createdAt!: Date;

  @UpdateDateColumn({ name: 'updated_at' })
  updatedAt!: Date;
}