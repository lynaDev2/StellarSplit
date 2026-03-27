// Audit service for recording and querying audit events
import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, Between, Like, In } from 'typeorm';
import { v4 as uuidv4 } from 'uuid';
import {
  AuditEvent,
  AuditAction,
  AuditResourceType,
  AuditSeverity,
  AuditQueryFilters,
  AuditQueryResult,
} from './audit-event.entity';

@Injectable()
export class AuditService {
  private readonly logger = new Logger(AuditService.name);

  constructor(
    @InjectRepository(AuditEvent)
    private readonly auditRepository: Repository<AuditEvent>,
  ) {}

  /**
   * Log an audit event
   * This is the main method for recording all auditable actions
   */
  async logEvent(params: {
    action: AuditAction;
    resourceType: AuditResourceType;
    resourceId: string;
    actorId?: string;
    actorEmail?: string;
    actorIp?: string;
    actorUserAgent?: string;
    sessionId?: string;
    requestMetadata?: {
      method?: string;
      path?: string;
      correlationId?: string;
    };
    previousState?: Record<string, unknown>;
    newState?: Record<string, unknown>;
    description?: string;
    reason?: string;
    severity?: AuditSeverity;
    metadata?: Record<string, unknown>;
  }): Promise<AuditEvent> {
    const event = this.auditRepository.create({
      id: uuidv4(),
      ...params,
      severity: params.severity || this.getDefaultSeverity(params.action),
    });

    const saved = await this.auditRepository.save(event);
    
    this.logger.debug(`Audit event logged: ${params.action}`, {
      resourceType: params.resourceType,
      resourceId: params.resourceId,
      actorId: params.actorId,
    });

    return saved;
  }

  /**
   * Log a payment-related audit event
   */
  async logPaymentEvent(params: {
    action: Extract<AuditAction, `payment.${string}`>;
    paymentId: string;
    actorId?: string;
    actorEmail?: string;
    actorIp?: string;
    previousState?: Record<string, unknown>;
    newState?: Record<string, unknown>;
    description?: string;
    reason?: string;
  }): Promise<AuditEvent> {
    return this.logEvent({
      ...params,
      resourceId: params.paymentId,
      resourceType: AuditResourceType.PAYMENT,
    });
  }

  /**
   * Log a dispute-related audit event
   */
  async logDisputeEvent(params: {
    action: Extract<AuditAction, `dispute.${string}`>;
    disputeId: string;
    actorId?: string;
    actorEmail?: string;
    actorIp?: string;
    previousState?: Record<string, unknown>;
    newState?: Record<string, unknown>;
    description?: string;
    reason?: string;
    severity?: AuditSeverity;
  }): Promise<AuditEvent> {
    return this.logEvent({
      ...params,
      resourceId: params.disputeId,
      resourceType: AuditResourceType.DISPUTE,
    });
  }

  /**
   * Log an export-related audit event
   */
  async logExportEvent(params: {
    action: Extract<AuditAction, `export.${string}`>;
    exportId: string;
    actorId?: string;
    actorEmail?: string;
    actorIp?: string;
    metadata?: Record<string, unknown>;
    description?: string;
  }): Promise<AuditEvent> {
    return this.logEvent({
      ...params,
      resourceId: params.exportId,
      resourceType: AuditResourceType.EXPORT,
    });
  }

  /**
   * Log a receipt access event
   */
  async logReceiptEvent(params: {
    action: Extract<AuditAction, `receipt.${string}`>;
    receiptId: string;
    actorId?: string;
    actorEmail?: string;
    actorIp?: string;
    actorUserAgent?: string;
    metadata?: Record<string, unknown>;
    description?: string;
  }): Promise<AuditEvent> {
    return this.logEvent({
      ...params,
      resourceId: params.receiptId,
      resourceType: AuditResourceType.RECEIPT,
      severity: params.action === AuditAction.RECEIPT_DELETED 
        ? AuditSeverity.WARNING 
        : AuditSeverity.INFO,
    });
  }

  /**
   * Log an admin action
   */
  async logAdminEvent(params: {
    action: Extract<AuditAction, `admin.${string}`>;
    resourceType: AuditResourceType;
    resourceId: string;
    actorId: string;
    actorEmail: string;
    actorIp?: string;
    previousState?: Record<string, unknown>;
    newState?: Record<string, unknown>;
    reason?: string;
    severity?: AuditSeverity;
    metadata?: Record<string, unknown>;
  }): Promise<AuditEvent> {
    return this.logEvent({
      ...params,
      severity: params.severity || AuditSeverity.WARNING,
    });
  }

  /**
   * Query audit events with filters
   */
  async queryEvents(
    filters: AuditQueryFilters,
    page: number = 1,
    limit: number = 20,
  ): Promise<AuditQueryResult> {
    const queryBuilder = this.auditRepository
      .createQueryBuilder('audit')
      .orderBy('audit.timestamp', 'DESC')
      .skip((page - 1) * limit)
      .take(limit);

    // Apply filters
    if (filters.action) {
      if (Array.isArray(filters.action)) {
        queryBuilder.andWhere('audit.action IN (:...actions)', {
          actions: filters.action,
        });
      } else {
        queryBuilder.andWhere('audit.action = :action', {
          action: filters.action,
        });
      }
    }

    if (filters.resourceType) {
      queryBuilder.andWhere('audit.resourceType = :resourceType', {
        resourceType: filters.resourceType,
      });
    }

    if (filters.resourceId) {
      queryBuilder.andWhere('audit.resourceId = :resourceId', {
        resourceId: filters.resourceId,
      });
    }

    if (filters.actorId) {
      queryBuilder.andWhere('audit.actorId = :actorId', {
        actorId: filters.actorId,
      });
    }

    if (filters.severity) {
      queryBuilder.andWhere('audit.severity = :severity', {
        severity: filters.severity,
      });
    }

    if (filters.dateFrom && filters.dateTo) {
      queryBuilder.andWhere('audit.timestamp BETWEEN :dateFrom AND :dateTo', {
        dateFrom: filters.dateFrom,
        dateTo: filters.dateTo,
      });
    }

    if (filters.reviewed !== undefined) {
      queryBuilder.andWhere('audit.reviewed = :reviewed', {
        reviewed: filters.reviewed,
      });
    }

    if (filters.search) {
      queryBuilder.andWhere(
        '(audit.description ILIKE :search OR audit.resourceId ILIKE :search)',
        { search: `%${filters.search}%` },
      );
    }

    // Get total count
    const total = await queryBuilder.getCount();

    // Get results
    const data = await queryBuilder.getMany();

    return {
      data,
      total,
      page,
      limit,
      hasMore: page * limit < total,
    };
  }

  /**
   * Get audit events for a specific resource
   */
  async getEventsForResource(
    resourceType: AuditResourceType,
    resourceId: string,
  ): Promise<AuditEvent[]> {
    return this.auditRepository.find({
      where: {
        resourceType,
        resourceId,
      },
      order: {
        timestamp: 'DESC',
      },
    });
  }

  /**
   * Mark an event as reviewed
   */
  async markAsReviewed(
    eventId: string,
    reviewedById: string,
    note?: string,
  ): Promise<AuditEvent | null> {
    const event = await this.auditRepository.findOne({
      where: { id: eventId },
    });

    if (!event) {
      return null;
    }

    event.reviewed = true;
    event.reviewedById = reviewedById;
    event.reviewedAt = new Date();
    event.reviewNote = note;

    return this.auditRepository.save(event);
  }

  /**
   * Get unreviewed events (for admin dashboard)
   */
  async getUnreviewedEvents(
    page: number = 1,
    limit: number = 20,
  ): Promise<AuditQueryResult> {
    return this.queryEvents(
      { reviewed: false, severity: AuditSeverity.ERROR },
      page,
      limit,
    );
  }

  /**
   * Get critical events for a time period
   */
  async getCriticalEvents(
    dateFrom: Date,
    dateTo: Date,
  ): Promise<AuditEvent[]> {
    return this.auditRepository.find({
      where: {
        severity: AuditSeverity.CRITICAL,
        timestamp: Between(dateFrom, dateTo),
      },
      order: {
        timestamp: 'DESC',
      },
    });
  }

  /**
   * Determine default severity based on action
   */
  private getDefaultSeverity(action: AuditAction): AuditSeverity {
    const criticalActions = [
      AuditAction.PAYMENT_REFUNDED,
      AuditAction.USER_DELETED,
      AuditAction.USER_ROLE_CHANGED,
      AuditAction.ADMIN_REFUND_PROCESSED,
      AuditAction.ADMIN_SETTINGS_CHANGED,
    ];

    const warningActions = [
      AuditAction.PAYMENT_FAILED,
      AuditAction.SPLIT_DELETED,
      AuditAction.DISPUTE_CREATED,
      AuditAction.LOGIN_FAILED,
    ];

    if (criticalActions.includes(action)) {
      return AuditSeverity.CRITICAL;
    }

    if (warningActions.includes(action)) {
      return AuditSeverity.WARNING;
    }

    return AuditSeverity.INFO;
  }
}
