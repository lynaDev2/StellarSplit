// Health check controller with readiness and liveness probes
import { Controller, Get, HttpStatus, Logger } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiProduces } from '@nestjs/swagger';
import { ConfigService } from '@nestjs/config';
import { DataSource } from 'typeorm';
import Redis from 'ioredis';
import { validateEnvironment, getEnvironmentInfo, Environment } from '../config/env.validation';

interface HealthStatus {
  status: 'healthy' | 'unhealthy' | 'degraded';
  timestamp: string;
  version: string;
  uptime: number;
}

interface ComponentHealth {
  status: 'up' | 'down' | 'degraded';
  latencyMs?: number;
  error?: string;
}

interface ReadinessResponse extends HealthStatus {
  checks: {
    database: ComponentHealth;
    redis: ComponentHealth;
    environment: ComponentHealth;
  };
}

interface LivenessResponse extends HealthStatus {
  environment: ReturnType<typeof getEnvironmentInfo>;
}

@ApiTags('Health')
@Controller('health')
export class HealthController {
  private readonly logger = new Logger(HealthController.name);
  private readonly startTime = Date.now();
  private redis: Redis | null = null;

  constructor(
    private readonly configService: ConfigService,
    private readonly dataSource: DataSource,
  ) {
    // Initialize Redis client for health checks
    const redisUrl = this.configService.get<string>('REDIS_URL');
    if (redisUrl) {
      this.redis = new Redis(redisUrl, {
        connectTimeout: 5000,
        maxRetriesPerRequest: 1,
      });
    }
  }

  @Get()
  @ApiOperation({ summary: 'Basic health check' })
  @ApiResponse({ status: HttpStatus.OK, description: 'Service is healthy' })
  @ApiResponse({ status: HttpStatus.SERVICE_UNAVAILABLE, description: 'Service is unhealthy' })
  check(): HealthStatus {
    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      version: this.configService.get<string>('APP_VERSION') || '1.0.0',
      uptime: Math.floor((Date.now() - this.startTime) / 1000),
    };
  }

  @Get('live')
  @ApiOperation({ summary: 'Liveness probe - is the service running?' })
  @ApiResponse({ status: HttpStatus.OK, description: 'Service is alive' })
  liveness(): LivenessResponse {
    return {
      status: 'healthy',
      timestamp: new Date().toISOString(),
      version: this.configService.get<string>('APP_VERSION') || '1.0.0',
      uptime: Math.floor((Date.now() - this.startTime) / 1000),
      environment: getEnvironmentInfo(),
    };
  }

  @Get('ready')
  @ApiOperation({ summary: 'Readiness probe - can the service handle requests?' })
  @ApiResponse({ status: HttpStatus.OK, description: 'Service is ready to handle requests' })
  @ApiResponse({ status: HttpStatus.SERVICE_UNAVAILABLE, description: 'Service is not ready' })
  async readiness(): Promise<ReadinessResponse> {
    const checks = {
      database: await this.checkDatabase(),
      redis: await this.checkRedis(),
      environment: this.checkEnvironment(),
    };

    // Determine overall status
    let status: 'healthy' | 'unhealthy' | 'degraded' = 'healthy';
    
    if (checks.database.status === 'down' || checks.environment.status === 'down') {
      status = 'unhealthy';
    } else if (checks.database.status === 'degraded' || checks.redis.status === 'degraded') {
      status = 'degraded';
    }

    return {
      status,
      timestamp: new Date().toISOString(),
      version: this.configService.get<string>('APP_VERSION') || '1.0.0',
      uptime: Math.floor((Date.now() - this.startTime) / 1000),
      checks,
    };
  }

  private async checkDatabase(): Promise<ComponentHealth> {
    const start = Date.now();
    try {
      await this.dataSource.query('SELECT 1');
      return {
        status: 'up',
        latencyMs: Date.now() - start,
      };
    } catch (error) {
      this.logger.error('Database health check failed', error);
      return {
        status: 'down',
        latencyMs: Date.now() - start,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  private async checkRedis(): Promise<ComponentHealth> {
    if (!this.redis) {
      return {
        status: 'degraded',
        error: 'Redis client not configured',
      };
    }

    const start = Date.now();
    try {
      await this.redis.ping();
      return {
        status: 'up',
        latencyMs: Date.now() - start,
      };
    } catch (error) {
      this.logger.error('Redis health check failed', error);
      return {
        status: 'down',
        latencyMs: Date.now() - start,
        error: error instanceof Error ? error.message : 'Unknown error',
      };
    }
  }

  private checkEnvironment(): ComponentHealth {
    const envConfig = {
      NODE_ENV: process.env.NODE_ENV,
      DATABASE_HOST: process.env.DATABASE_HOST,
      JWT_SECRET: process.env.JWT_SECRET ? 'set' : 'missing',
      REDIS_URL: process.env.REDIS_URL ? 'set' : 'missing',
    };

    const isProduction = process.env.NODE_ENV === Environment.PRODUCTION;
    
    // Validate environment
    const validation = validateEnvironment(envConfig);
    
    if (!validation.isValid) {
      return {
        status: 'down',
        error: validation.errors.join('; '),
      };
    }

    if (validation.warnings.length > 0 && isProduction) {
      return {
        status: 'degraded',
        error: validation.warnings.join('; '),
      };
    }

    return {
      status: 'up',
    };
  }
}
