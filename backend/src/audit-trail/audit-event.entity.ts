// Comprehensive audit trail system for financial operations
// This module provides traceability for payments, exports, disputes, and admin actions

import { Entity, PrimaryGeneratedColumn, Column, CreateDateColumn, Index, ManyToOne, JoinColumn } from 'typeorm';
import { User } from '../entities/user.entity';

/**
 * Audit action types that must be tracked
 * These cover all sensitive operations in the system
 */
export enum AuditAction {
  // Payment actions
  PAYMENT_INITIATED = 'payment.initiated',
  PAYMENT_COMPLETED = 'payment.completed',
  PAYMENT_FAILED = 'payment.failed',
  PAYMENT_REFUNDED = 'payment.refunded',
  PAYMENT_CANCELLED = 'payment.cancelled',
  PAYMENT_DISPUTED = 'payment.disputed',
  
  // Settlement actions
  SETTLEMENT_CREATED = 'settlement.created',
  SETTLEMENT_COMPLETED = 'settlement.completed',
  SETTLEMENT_FAILED = 'settlement.failed',
  
  // Split actions
  SPLIT_CREATED = 'split.created',
  SPLIT_UPDATED = 'split.updated',
  SPLIT_DELETED = 'split.deleted',
  SPLIT_COMPLETED = 'split.completed',
  SPLIT_CANCELLED = 'split.cancelled',
  
  // Dispute actions
  DISPUTE_CREATED = 'dispute.created',
  DISPUTE_UPDATED = 'dispute.updated',
  DISPUTE_RESOLVED = 'dispute.resolved',
  DISPUTE_REJECTED = 'dispute.rejected',
  DISPUTE_EVIDENCE_ADDED = 'dispute.evidence_added',
  
  // Export actions
  EXPORT_CREATED = 'export.created',
  EXPORT_DOWNLOADED = 'export.downloaded',
  EXPORT_FAILED = 'export.failed',
  
  // Receipt actions
  RECEIPT_UPLOADED = 'receipt.uploaded',
  RECEIPT_ACCESSED = 'receipt.accessed',
  RECEIPT_DELETED = 'receipt.deleted',
  
  // User management
  USER_CREATED = 'user.created',
  USER_UPDATED = 'user.updated',
  USER_DELETED = 'user.deleted',
  USER_ROLE_CHANGED = 'user.role_changed',
  
  // Admin actions
  ADMIN_NOTE_ADDED = 'admin.note_added',
  ADMIN_STATUS_CHANGED = 'admin.status_changed',
  ADMIN_SETTINGS_CHANGED = 'admin.settings_changed',
  ADMIN_REFUND_PROCESSED = 'admin.refund_processed',
  
  // Auth actions
  LOGIN_SUCCESS = 'auth.login_success',
  LOGIN_FAILED = 'auth.login_failed',
  LOGOUT = 'auth.logout',
  PASSWORD_CHANGED = 'auth.password_changed',
  TFA_ENABLED = 'auth.tfa_enabled',
  TFA_DISABLED = 'auth.tfa_disabled',
}

/**
 * Resource types that can be audited
 */
export enum AuditResourceType {
  PAYMENT = 'payment',
  SETTLEMENT = 'settlement',
  SPLIT = 'split',
  DISPUTE = 'dispute',
  EXPORT = 'export',
  RECEIPT = 'receipt',
  USER = 'user',
  ADMIN = 'admin',
  SESSION = 'session',
}

/**
 * Severity level for audit events
 */
export enum AuditSeverity {
  INFO = 'info',
  WARNING = 'warning',
  ERROR = 'error',
  CRITICAL = 'critical',
}

/**
 * Audit event entity for storing in database
 */
@Entity('audit_events')
@Index(['timestamp', 'action'])
@Index(['actorId', 'timestamp'])
@Index(['resourceType', 'resourceId'])
@Index(['severity'])
export class AuditEvent {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'enum', enum: AuditAction })
  action!: AuditAction;

  @Column({ type: 'enum', enum: AuditResourceType })
  resourceType!: AuditResourceType;

  @Column({ type: 'uuid' })
  resourceId!: string;

  @Column({ type: 'enum', enum: AuditSeverity, default: AuditSeverity.INFO })
  severity!: AuditSeverity;

  // Actor (who performed the action)
  @Column({ type: 'uuid', nullable: true })
  actorId?: string;

  @Column({ type: 'varchar', nullable: true })
  actorEmail?: string;

  @Column({ type: 'varchar', nullable: true })
  actorIp?: string;

  @Column({ type: 'varchar', nullable: true })
  actorUserAgent?: string;

  // Context
  @Column({ type: 'varchar', nullable: true })
  sessionId?: string;

  @Column({ type: 'jsonb', nullable: true })
  requestMetadata?: {
    method?: string;
    path?: string;
    correlationId?: string;
  };

  // Event data
  @Column({ type: 'jsonb', nullable: true })
  previousState?: Record<string, unknown>;

  @Column({ type: 'jsonb', nullable: true })
  newState?: Record<string, unknown>;

  @Column({ type: 'text', nullable: true })
  description?: string;

  @Column({ type: 'text', nullable: true })
  reason?: string;

  // Timestamp
  @CreateDateColumn({ type: 'timestamptz' })
  timestamp!: Date;

  // Metadata
  @Column({ type: 'jsonb', nullable: true })
  metadata?: Record<string, unknown>;

  // Whether this event has been reviewed
  @Column({ type: 'boolean', default: false })
  reviewed!: boolean;

  @Column({ type: 'uuid', nullable: true })
  reviewedById?: string;

  @Column({ type: 'timestamptz', nullable: true })
  reviewedAt?: Date;

  @Column({ type: 'text', nullable: true })
  reviewNote?: string;

  // ManyToOne relationship for actor
  @ManyToOne(() => User, { nullable: true })
  @JoinColumn({ name: 'actorId' })
  actor?: User;
}

/**
 * Audit query filters
 */
export interface AuditQueryFilters {
  action?: AuditAction | AuditAction[];
  resourceType?: AuditResourceType;
  resourceId?: string;
  actorId?: string;
  severity?: AuditSeverity;
  dateFrom?: Date;
  dateTo?: Date;
  reviewed?: boolean;
  search?: string;
}

/**
 * Paginated audit results
 */
export interface AuditQueryResult {
  data: AuditEvent[];
  total: number;
  page: number;
  limit: number;
  hasMore: boolean;
}
