import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, UpdateDateColumn, ManyToOne, JoinColumn, Index } from 'typeorm';
import { Participant } from './participant.entity';

/**
 * Payment settlement states for tracking on-chain reconciliation
 */
export enum PaymentSettlementStatus {
  /** Payment submitted, awaiting Horizon confirmation */
  SUBMITTED = 'submitted',
  /** Payment confirmed on-chain */
  CONFIRMED = 'confirmed',
  /** Payment failed on-chain */
  FAILED = 'failed',
  /** Payment requires manual review (stale, disputed, etc.) */
  REVIEW_REQUIRED = 'review_required',
  /** Payment is being reconciled */
  RECONCILING = 'reconciling',
}

/**
 * Payment processing states
 */
export enum PaymentProcessingStatus {
  PENDING = 'pending',
  CONFIRMED = 'confirmed',
  FAILED = 'failed',
  PARTIAL = 'partial',
}

@Entity('payments')
export class Payment {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  /**
   * Idempotency key to prevent duplicate submissions
   * Generated from: splitId + participantId + txHash
   */
  @Column({ type: 'varchar', unique: true, nullable: true })
  @Index()
  idempotencyKey?: string;

  @Column({ type: 'uuid' })
  splitId!: string;

  @Column({ type: 'uuid' })
  participantId!: string;

  @Column({ type: 'varchar' })
  @Index()
  txHash!: string;

  @Column({ type: 'decimal', precision: 10, scale: 2 })
  amount!: number;

  @Column({ type: 'varchar' })
  asset!: string;

  /**
   * Processing status (pending, confirmed, failed, partial)
   */
  @Column({ type: 'varchar', default: 'pending' })
  status!: PaymentProcessingStatus;

  /**
   * Settlement status for on-chain reconciliation
   */
  @Column({ type: 'varchar', default: PaymentSettlementStatus.SUBMITTED })
  settlementStatus!: PaymentSettlementStatus;

  /**
   * Last time the settlement status was checked on-chain
   */
  @Column({ type: 'timestamp', nullable: true })
  lastSettlementCheck?: Date;

  /**
   * Number of reconciliation attempts
   */
  @Column({ type: 'int', default: 0 })
  reconciliationAttempts!: number;

  /**
   * Maximum reconciliation attempts before marking for review
   */
  @Column({ type: 'int', default: 5 })
  maxReconciliationAttempts!: number;

  /**
   * Error message if settlement fails
   */
  @Column({ type: 'text', nullable: true })
  settlementError?: string;

  /**
   * Whether notifications have been sent (prevents duplicate notifications)
   */
  @Column({ type: 'boolean', default: false })
  notificationsSent!: boolean;

  /**
   * Timestamp when payment was first processed
   */
  @Column({ type: 'timestamp', nullable: true })
  processedAt?: Date;

  /**
   * External reference for webhook replay support
   */
  @Column({ type: 'varchar', nullable: true })
  externalReference?: string;

  @CreateDateColumn()
  createdAt!: Date;

  @UpdateDateColumn()
  updatedAt!: Date;

  @ManyToOne(() => Participant, participant => participant.id)
  @JoinColumn({ name: 'participantId' })
  participant?: Participant;
}