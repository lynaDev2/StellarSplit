import { IsUUID, IsEmail, IsString, MaxLength, IsOptional } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';

export class UpgradeGuestDto {
  @ApiProperty({ 
    description: 'ID of the participant to upgrade', 
    example: '550e8400-e29b-41d4-a716-446655440000' 
  })
  @IsUUID()
  participantId!: string;

  @ApiProperty({ 
    description: 'Email of the registered user', 
    example: 'alice@example.com' 
  })
  @IsEmail()
  email!: string;

  @ApiPropertyOptional({ 
    description: 'Display name', 
    example: 'Alice', 
    maxLength: 255 
  })
  @IsOptional()
  @IsString()
  @MaxLength(255)
  displayName?: string;
}
