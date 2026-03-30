import {
  Injectable,
  Logger,
  HttpException,
  HttpStatus,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { ConfigService } from "@nestjs/config";
import axios, { AxiosError } from "axios";

import { FraudAlert, AlertStatus, AlertType } from "./entities/fraud-alert.entity";
import {
  AnalyzeSplitRequestDto,
  AnalyzePaymentRequestDto,
  AnalysisResponseDto,
  FeedbackRequestDto,
  ResolveAlertDto,
  FeedbackType,
} from "./dto/analyze-split.dto";

export interface FraudCheckResult {
  allowed: boolean;
  riskScore: number;
  riskLevel: string;
  flags: string[];
  alertId?: string;
}

@Injectable()
export class FraudDetectionService {
  private readonly logger = new Logger(FraudDetectionService.name);
  private readonly mlServiceUrl: string;
  private readonly enabled: boolean;
  private readonly timeout: number;
  private readonly highRiskThreshold: number;
  private readonly mediumRiskThreshold: number;

  constructor(
    @InjectRepository(FraudAlert)
    private fraudAlertRepository: Repository<FraudAlert>,
    private configService: ConfigService,
  ) {
    this.mlServiceUrl = this.configService.get<string>(
      "ML_SERVICE_URL",
      "http://localhost:8000",
    );
    this.enabled = this.configService.get<boolean>("FRAUD_DETECTION_ENABLED", true);
    this.timeout = this.configService.get<number>("FRAUD_CHECK_TIMEOUT_MS", 5000);
    this.highRiskThreshold = this.configService.get<number>("HIGH_RISK_THRESHOLD", 80);
    this.mediumRiskThreshold = this.configService.get<number>("MEDIUM_RISK_THRESHOLD", 50);
  }

  /**
   * Analyze a split for fraud risk
   */
  async analyzeSplit(
    request: AnalyzeSplitRequestDto,
  ): Promise<AnalysisResponseDto> {
    if (!this.enabled) {
      return {
        risk_score: 0,
        risk_level: "low",
        anomaly_score: 0,
        pattern_match_score: 0,
        flags: [],
        model_version: "disabled",
        processing_time_ms: 0,
      };
    }

    try {
      const response = await axios.post<AnalysisResponseDto>(
        `${this.mlServiceUrl}/api/v1/analyze/split`,
        request,
        { timeout: this.timeout },
      );

      return response.data;
    } catch (error) {
      const err = error as AxiosError;
      this.logger.error(`Failed to analyze split: ${err.message}`);
      throw new HttpException(
        "Fraud detection service unavailable",
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }
  }

  /**
   * Analyze a payment for fraud risk
   */
  async analyzePayment(
    request: AnalyzePaymentRequestDto,
  ): Promise<AnalysisResponseDto> {
    if (!this.enabled) {
      return {
        risk_score: 0,
        risk_level: "low",
        anomaly_score: 0,
        pattern_match_score: 0,
        flags: [],
        model_version: "disabled",
        processing_time_ms: 0,
      };
    }

    try {
      const response = await axios.post<AnalysisResponseDto>(
        `${this.mlServiceUrl}/api/v1/analyze/payment`,
        request,
        { timeout: this.timeout },
      );

      return response.data;
    } catch (error) {
      const err = error as AxiosError;
      this.logger.error(`Failed to analyze payment: ${err.message}`);
      throw new HttpException(
        "Fraud detection service unavailable",
        HttpStatus.SERVICE_UNAVAILABLE,
      );
    }
  }

  /**
   * Check if a split should be allowed (used as guard)
   */
  async checkSplit(
    splitData: AnalyzeSplitRequestDto,
  ): Promise<FraudCheckResult> {
    const analysis = await this.analyzeSplit(splitData);

    const result: FraudCheckResult = {
      allowed: analysis.risk_score < this.highRiskThreshold,
      riskScore: analysis.risk_score,
      riskLevel: analysis.risk_level,
      flags: analysis.flags,
    };

    // Create alert for high/medium risk
    if (analysis.risk_score >= this.mediumRiskThreshold) {
      const alert = await this.createAlert(
        analysis.risk_score >= this.highRiskThreshold
          ? AlertType.HIGH_RISK_SPLIT
          : AlertType.SUSPICIOUS_PATTERN,
        splitData.split_data.split_id,
        undefined,
        analysis,
      );
      result.alertId = alert.id;
    }

    return result;
  }

  /**
   * Check if a payment should be allowed
   */
  async checkPayment(
    paymentData: AnalyzePaymentRequestDto,
  ): Promise<FraudCheckResult> {
    const analysis = await this.analyzePayment(paymentData);

    const result: FraudCheckResult = {
      allowed: analysis.risk_score < this.highRiskThreshold,
      riskScore: analysis.risk_score,
      riskLevel: analysis.risk_level,
      flags: analysis.flags,
    };

    // Create alert for high/medium risk
    if (analysis.risk_score >= this.mediumRiskThreshold) {
      const alert = await this.createAlert(
        analysis.risk_score >= this.highRiskThreshold
          ? AlertType.HIGH_RISK_PAYMENT
          : AlertType.SUSPICIOUS_PATTERN,
        paymentData.payment_data.split_id,
        paymentData.payment_data.participant_id,
        analysis,
      );
      result.alertId = alert.id;
    }

    return result;
  }

  /**
   * Create a fraud alert
   */
  async createAlert(
    alertType: AlertType,
    splitId: string,
    participantId: string | undefined,
    analysis: AnalysisResponseDto,
  ): Promise<FraudAlert> {
    const alert = this.fraudAlertRepository.create({
      split_id: splitId,
      participant_id: participantId,
      alert_type: alertType,
      risk_score: analysis.risk_score,
      anomaly_score: analysis.anomaly_score,
      pattern_score: analysis.pattern_match_score,
      model_version: analysis.model_version,
      flags: analysis.flags,
      status: AlertStatus.OPEN,
    });

    const saved = await this.fraudAlertRepository.save(alert);

    this.logger.warn(
      `Fraud alert created: ${alertType} for split ${splitId} with risk score ${analysis.risk_score}`,
    );

    return saved;
  }

  /**
   * Get all fraud alerts
   */
  async getAlerts(
    status?: AlertStatus,
    page: number = 1,
    limit: number = 50,
  ): Promise<{ alerts: FraudAlert[]; total: number }> {
    const where: any = {};
    if (status) {
      where.status = status;
    }

    const [alerts, total] = await this.fraudAlertRepository.findAndCount({
      where,
      order: { created_at: "DESC" },
      skip: (page - 1) * limit,
      take: limit,
    });

    return { alerts, total };
  }

  /**
   * Get a single alert by ID
   */
  async getAlert(id: string): Promise<FraudAlert | null> {
    return this.fraudAlertRepository.findOne({ where: { id } });
  }

  /**
   * Resolve a fraud alert
   */
  async resolveAlert(
    id: string,
    resolveData: ResolveAlertDto,
    resolvedBy: string,
  ): Promise<FraudAlert> {
    const alert = await this.fraudAlertRepository.findOne({ where: { id } });

    if (!alert) {
      throw new HttpException("Alert not found", HttpStatus.NOT_FOUND);
    }

    alert.status =
      resolveData.resolution === "false_positive"
        ? AlertStatus.FALSE_POSITIVE
        : AlertStatus.RESOLVED;
    alert.resolved_at = new Date();
    alert.resolved_by = resolvedBy;
    alert.resolution_notes = resolveData.notes;
    alert.is_true_positive = resolveData.is_true_positive;

    const saved = await this.fraudAlertRepository.save(alert);

    // Send feedback to ML service
    const feedbackType: FeedbackType = resolveData.is_true_positive
      ? FeedbackType.TRUE_POSITIVE
      : FeedbackType.FALSE_POSITIVE;

    await this.submitFeedback({
      alert_id: id,
      is_fraud: resolveData.is_true_positive || false,
      feedback_type: feedbackType,
      notes: resolveData.notes,
      reviewed_by: resolvedBy,
    });

    return saved;
  }

  /**
   * Submit feedback to ML service
   */
  async submitFeedback(feedback: FeedbackRequestDto): Promise<void> {
    try {
      await axios.post(`${this.mlServiceUrl}/api/v1/feedback`, feedback);
    } catch (error) {
      const err = error as AxiosError;
      this.logger.error(`Failed to submit feedback: ${err.message}`);
      // Don't throw - feedback is best-effort
    }
  }

  /**
   * Get fraud detection statistics
   */
  async getStats(): Promise<{
    totalAlerts: number;
    openAlerts: number;
    resolvedAlerts: number;
    falsePositives: number;
    accuracy: number;
  }> {
    const total = await this.fraudAlertRepository.count();
    const open = await this.fraudAlertRepository.count({
      where: { status: AlertStatus.OPEN },
    });
    const resolved = await this.fraudAlertRepository.count({
      where: { status: AlertStatus.RESOLVED },
    });
    const falsePositives = await this.fraudAlertRepository.count({
      where: { status: AlertStatus.FALSE_POSITIVE },
    });

    // Calculate accuracy as precision: true positives / (true positives + false positives)
    const accuracy = reviewed > 0 ? truePositives / reviewed : 0;

    return {
      totalAlerts: total,
      openAlerts: open,
      resolvedAlerts: resolved,
      falsePositives,
      accuracy: Math.round(accuracy * 100) / 100,
    };
  }

  /**
   * Get analysis for a specific split
   */
  async getSplitAnalysis(splitId: string): Promise<{
    riskScore: number;
    factors: string[];
    recommendations: string[];
  }> {
    const alerts = await this.fraudAlertRepository.find({
      where: { split_id: splitId },
      order: { created_at: "DESC" },
    });

    if (alerts.length === 0) {
      return {
        riskScore: 0,
        factors: [],
        recommendations: ["No fraud alerts for this split"],
      };
    }

    const latestAlert = alerts[0];

    const factors = latestAlert.flags || [];
    const recommendations = this.generateRecommendations(factors);

    return {
      riskScore: latestAlert.risk_score,
      factors,
      recommendations,
    };
  }

  /**
   * Generate recommendations based on flags
   */
  private generateRecommendations(flags: string[]): string[] {
    const recommendations: string[] = [];

    const recommendationMap: Record<string, string> = {
      new_user: "Verify user identity before processing large transactions",
      rapid_split_creation: "Review user's recent activity for suspicious patterns",
      night_time_activity: "Consider time-based risk factors",
      large_amount: "Require additional verification for high-value splits",
      single_participant_split: "Review split purpose and legitimacy",
      anomalous_behavior: "Manual review recommended",
      suspicious_pattern_detected: "Compare against known fraud patterns",
      high_fraud_probability: "Immediate review required",
      immediate_payment: "Verify payment source legitimacy",
      instant_payment_after_creation: "Review for potential automated fraud",
    };

    for (const flag of flags) {
      if (recommendationMap[flag]) {
        recommendations.push(recommendationMap[flag]);
      }
    }

    if (recommendations.length === 0) {
      recommendations.push("Standard verification procedures apply");
    }

    return recommendations;
  }
}
