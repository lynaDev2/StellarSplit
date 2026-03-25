import { IsUUID, IsOptional, IsInt, Min, Max, IsEmail, IsBoolean } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';

const DEFAULT_EXPIRY_HOURS = 72;

export class CreateInvitationDto {
  @ApiProperty({ description: 'Split ID to invite participants to', example: '550e8400-e29b-41d4-a716-446655440000' })
  @IsUUID()
  splitId!: string;

  @ApiPropertyOptional({
    description: 'Token expiry in hours (default 72)',
    default: DEFAULT_EXPIRY_HOURS,
    minimum: 1,
    maximum: 720,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(720)
  @Type(() => Number)
  expiresInHours?: number = DEFAULT_EXPIRY_HOURS;

  @ApiPropertyOptional({
    description: 'Maximum number of times this invitation can be used (default 1)',
    default: 1,
    minimum: 1,
    maximum: 100,
  })
  @IsOptional()
  @IsInt()
  @Min(1)
  @Max(100)
  @Type(() => Number)
  maxUses?: number = 1;

  @ApiPropertyOptional({
    description: 'Email of the invitee (for duplicate detection)',
    example: 'alice@example.com',
  })
  @IsOptional()
  @IsEmail()
  inviteeEmail?: string;

  @ApiPropertyOptional({
    description: 'Whether this invitation can be upgraded to a registered user (default true)',
    default: true,
  })
  @IsOptional()
  @IsBoolean()
  @Type(() => Boolean)
  isUpgradeable?: boolean = true;
}
