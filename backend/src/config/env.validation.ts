// Environment validation and configuration checks
import { plainToInstance, Type } from 'class-transformer';
import { IsString, IsNumber, IsBoolean, IsOptional, validateSync, IsEnum, IsArray, ValidateNested } from 'class-validator';

export enum Environment {
  DEVELOPMENT = 'development',
  STAGING = 'staging',
  PRODUCTION = 'production',
  TEST = 'test',
}

/**
 * Required environment variables for production
 */
class RequiredEnvVars {
  @IsString()
  NODE_ENV!: Environment;

  @IsString()
  DATABASE_HOST!: string;

  @IsNumber()
  DATABASE_PORT!: number;

  @IsString()
  DATABASE_USERNAME!: string;

  @IsString()
  DATABASE_PASSWORD!: string;

  @IsString()
  DATABASE_NAME!: string;

  @IsString()
  JWT_SECRET!: string;

  @IsString()
  REDIS_URL!: string;
}

/**
 * Optional but recommended environment variables
 */
class OptionalEnvVars {
  @IsOptional()
  @IsNumber()
  PORT?: number;

  @IsOptional()
  @IsString()
  KAFKA_BROKERS?: string;

  @IsOptional()
  @IsString()
  CLICKHOUSE_HOST?: string;

  @IsOptional()
  @IsNumber()
  CLICKHOUSE_PORT?: number;

  @IsOptional()
  @IsString()
  CLICKHOUSE_USER?: string;

  @IsOptional()
  @IsString()
  CLICKHOUSE_PASSWORD?: string;

  @IsOptional()
  @IsString()
  STELLAR_NETWORK?: string;

  @IsOptional()
  @IsString()
  STELLAR_SECRET_KEY?: string;

  @IsOptional()
  @IsString()
  SMTP_HOST?: string;

  @IsOptional()
  @IsNumber()
  SMTP_PORT?: number;

  @IsOptional()
  @IsString()
  SMTP_USER?: string;

  @IsOptional()
  @IsString()
  SMTP_PASSWORD?: string;
}

/**
 * Combined environment config
 */
export class EnvironmentConfig {
  @ValidateNested()
  @Type(() => RequiredEnvVars)
  required!: RequiredEnvVars;

  @ValidateNested()
  @Type(() => OptionalEnvVars)
  optional!: OptionalEnvVars;

  @IsOptional()
  @IsArray()
  allowedOrigins?: string[];

  @IsOptional()
  @IsBoolean()
  enableStrictMode?: boolean;
}

interface ValidationResult {
  isValid: boolean;
  errors: string[];
  warnings: string[];
}

/**
 * Validate environment configuration
 * Returns validation result with any errors or warnings
 */
export function validateEnvironment(config: Record<string, unknown>): ValidationResult {
  const errors: string[] = [];
  const warnings: string[] = [];

  // Check required variables
  const requiredVars = [
    'NODE_ENV',
    'DATABASE_HOST',
    'DATABASE_PORT',
    'DATABASE_USERNAME',
    'DATABASE_PASSWORD',
    'DATABASE_NAME',
    'JWT_SECRET',
    'REDIS_URL',
  ];

  for (const varName of requiredVars) {
    if (!config[varName]) {
      errors.push(`Missing required environment variable: ${varName}`);
    }
  }

  // Production-specific checks
  const nodeEnv = config.NODE_ENV as Environment;
  
  if (nodeEnv === Environment.PRODUCTION) {
    // Check for default/dangerous values
    if (config.JWT_SECRET === 'your-secret-key' || (config.JWT_SECRET as string)?.length < 32) {
      errors.push('JWT_SECRET must be at least 32 characters in production');
    }

    // Check for debug mode
    if (config.DEBUG === 'true') {
      warnings.push('DEBUG is enabled - not recommended for production');
    }

    // Check CORS settings
    if (config.CORS_ORIGIN === 'true' || config.CORS_ORIGIN === '*') {
      errors.push('CORS origin cannot be wildcard (*) in production');
    }

    // Check database host (should not be localhost)
    if (config.DATABASE_HOST === 'localhost' || config.DATABASE_HOST === '127.0.0.1') {
      warnings.push('DATABASE_HOST is localhost - ensure this is intentional for production');
    }

    // Check for SSL
    if (!config.DATABASE_SSL || config.DATABASE_SSL === 'false') {
      warnings.push('Database SSL is disabled - consider enabling for production');
    }
  }

  // Check for deprecated variables
  const deprecatedVars = [
    { old: 'REDIS_HOST', new: 'REDIS_URL' },
    { old: 'KAFKA_URL', new: 'KAFKA_BROKERS' },
  ];

  for (const { old, new: newVar } of deprecatedVars) {
    if (config[old]) {
      warnings.push(`Environment variable ${old} is deprecated, use ${newVar} instead`);
    }
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Get environment info for health checks
 */
export function getEnvironmentInfo() {
  return {
    nodeEnv: process.env.NODE_ENV || 'development',
    nodeVersion: process.version,
    platform: process.platform,
    timestamp: new Date().toISOString(),
  };
}
