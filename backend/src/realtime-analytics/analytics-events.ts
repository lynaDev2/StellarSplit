// Analytics event types for product analytics
// These events track user actions and business metrics for product analytics

export enum AnalyticsEventType {
  // Split events
  SPLIT_CREATED = 'split.created',
  SPLIT_UPDATED = 'split.updated',
  SPLIT_COMPLETED = 'split.completed',
  SPLIT_DELETED = 'split.deleted',
  SPLIT_SHARED = 'split.shared',

  // Item events
  ITEM_ADDED = 'item.added',
  ITEM_UPDATED = 'item.updated',
  ITEM_DELETED = 'item.deleted',

  // Payment events
  PAYMENT_INITIATED = 'payment.initiated',
  PAYMENT_COMPLETED = 'payment.completed',
  PAYMENT_FAILED = 'payment.failed',
  PAYMENT_REFUNDED = 'payment.refunded',

  // Participant events
  PARTICIPANT_JOINED = 'participant.joined',
  PARTICIPANT_LEFT = 'participant.left',
  PARTICIPANT_SETTLED = 'participant.settled',

  // User events
  USER_REGISTERED = 'user.registered',
  USER_LOGIN = 'user.login',
  USER_LOGOUT = 'user.logout',
  USER_PROFILE_UPDATED = 'user.profile.updated',

  // Discovery events
  SEARCH_PERFORMED = 'search.performed',
  FILTER_APPLIED = 'filter.applied',

  // Engagement events
  PAGE_VIEWED = 'page.viewed',
  BUTTON_CLICKED = 'button.clicked',
  ERROR_OCCURRED = 'error.occurred',
}

export interface AnalyticsEvent {
  // Event metadata
  id: string;
  type: AnalyticsEventType;
  timestamp: string; // ISO 8601

  // Actor (who performed the action)
  actor: {
    userId?: string;
    sessionId?: string;
    ipAddress?: string;
    userAgent?: string;
  };

  // Context (where the action happened)
  context: {
    source?: string; // 'web', 'mobile', 'api'
    platform?: string;
    version?: string;
    locale?: string;
  };

  // Event-specific payload
  payload: Record<string, unknown>;

  // Resource being acted upon
  resource?: {
    type: 'split' | 'item' | 'payment' | 'user';
    id: string;
  };
}

export interface SplitCreatedEvent extends AnalyticsEvent {
  type: AnalyticsEventType.SPLIT_CREATED;
  payload: {
    splitId: string;
    totalAmount: number;
    currency: string;
    participantCount: number;
    itemCount: number;
    categoryId?: string;
    isRecurring: boolean;
    source: 'api' | 'web' | 'mobile';
  };
}

export interface PaymentCompletedEvent extends AnalyticsEvent {
  type: AnalyticsEventType.PAYMENT_COMPLETED;
  payload: {
    paymentId: string;
    splitId: string;
    amount: number;
    currency: string;
    paymentMethod: string;
    settlementTimeMs: number;
    txHash?: string;
  };
}

export interface SearchPerformedEvent extends AnalyticsEvent {
  type: AnalyticsEventType.SEARCH_PERFORMED;
  payload: {
    query: string;
    resultCount: number;
    filtersApplied: string[];
    searchDurationMs: number;
    sortOrder?: string;
  };
}

// ClickHouse table schema for product analytics events
export const ANALYTICS_EVENTS_TABLE_SCHEMA = `
  CREATE TABLE IF NOT EXISTS analytics_events (
    id String,
    type Enum8(
      'split.created' = 1,
      'split.updated' = 2,
      'split.completed' = 3,
      'split.deleted' = 4,
      'split.shared' = 5,
      'item.added' = 6,
      'item.updated' = 7,
      'item.deleted' = 8,
      'payment.initiated' = 9,
      'payment.completed' = 10,
      'payment.failed' = 11,
      'payment.refunded' = 12,
      'participant.joined' = 13,
      'participant.left' = 14,
      'participant.settled' = 15,
      'user.registered' = 16,
      'user.login' = 17,
      'user.logout' = 18,
      'user.profile.updated' = 19,
      'search.performed' = 20,
      'filter.applied' = 21,
      'page.viewed' = 22,
      'button.clicked' = 23,
      'error.occurred' = 24
    ),
    timestamp DateTime64(3),
    
    -- Actor fields
    actor_user_id Nullable(String),
    actor_session_id Nullable(String),
    actor_ip_address Nullable(String),
    actor_user_agent Nullable(String),
    
    -- Context fields
    context_source Nullable(String),
    context_platform Nullable(String),
    context_version Nullable(String),
    context_locale Nullable(String),
    
    -- Payload (JSON)
    payload String,
    
    -- Resource fields
    resource_type Nullable(String),
    resource_id Nullable(String),
    
    -- Processing metadata
    processed_at Nullable(DateTime64(3))
  ) ENGINE = MergeTree()
  PARTITION BY toYYYYMM(timestamp)
  ORDER BY (type, timestamp, id)
  TTL timestamp + INTERVAL 90 DAY
  SETTINGS index_granularity = 8192;
`;

// Retention configuration
export const ANALYTICS_RETENTION_CONFIG = {
  // Raw events retention (90 days for detailed analysis)
  rawEventsRetentionDays: 90,
  
  // Aggregated metrics retention (1 year for dashboards)
  aggregatedMetricsRetentionDays: 365,
  
  // Daily aggregates retention (3 years for trends)
  dailyAggregatesRetentionDays: 1095,
  
  // ClickHouse materialized views TTL
  materializedViewsTTL: 365,
};

// Aggregation intervals for materialized views
export const ANALYTICS_AGGREGATION_INTERVALS = {
  // Real-time (5-minute windows)
  realtime: '5m',
  
  // Hourly aggregates
  hourly: '1h',
  
  // Daily aggregates
  daily: '1d',
  
  // Weekly aggregates
  weekly: '1w',
  
  // Monthly aggregates
  monthly: '1M',
};
