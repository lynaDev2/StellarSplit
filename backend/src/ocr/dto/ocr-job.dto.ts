import { IsUUID, IsOptional, IsString, IsNumber, Min, Max } from 'class-validator';
import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import { Type } from 'class-transformer';

export class CreateOcrJobDto {
  @ApiPropertyOptional({
    description: 'ID of the item this OCR is for',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsOptional()
  @IsUUID()
  itemId?: string;

  @ApiPropertyOptional({
    description: 'ID of the split this receipt belongs to',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsOptional()
  @IsUUID()
  splitId?: string;

  @ApiPropertyOptional({
    description: 'ID of the user uploading the image',
    example: '550e8400-e29b-41d4-a716-446655440000',
  })
  @IsOptional()
  @IsUUID()
  uploadedBy?: string;

  @ApiPropertyOptional({
    description: 'Original filename of the uploaded image',
    example: 'receipt.jpg',
  })
  @IsOptional()
  @IsString()
  originalFilename?: string;

  @ApiPropertyOptional({
    description: 'URL to the stored image (if already uploaded to storage)',
    example: 'https://storage.example.com/receipts/abc123.jpg',
  })
  @IsOptional()
  @IsString()
  imageUrl?: string;

  @ApiPropertyOptional({
    description: 'Priority of the job (higher = more urgent)',
    default: 1,
    minimum: 1,
    maximum: 10,
  })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(10)
  @Type(() => Number)
  priority?: number = 1;
}

export class UpdateOcrJobDto {
  @ApiPropertyOptional({
    description: 'Whether the result needs manual review',
  })
  @IsOptional()
  needsManualReview?: boolean;

  @ApiPropertyOptional({
    description: 'Confidence score (0-1)',
  })
  @IsOptional()
  @IsNumber()
  confidence?: number;

  @ApiPropertyOptional({
    description: 'Total amount',
  })
  @IsOptional()
  @IsNumber()
  totalAmount?: number;

  @ApiPropertyOptional({
    description: 'Extracted items as JSON',
  })
  @IsOptional()
  extractedItems?: Array<{
    name: string;
    quantity: number;
    price: number;
  }>;
}