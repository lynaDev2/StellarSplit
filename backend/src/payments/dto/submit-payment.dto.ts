import { IsString, IsUUID, IsNotEmpty, IsOptional } from 'class-validator';

export class SubmitPaymentDto {
  @IsUUID()
  @IsNotEmpty()
  splitId!: string;

  @IsUUID()
  @IsNotEmpty()
  participantId!: string;

  @IsString()
  @IsNotEmpty()
  stellarTxHash!: string;

  /**
   * Optional idempotency key to prevent duplicate submissions
   * If not provided, one will be generated from splitId, participantId, and txHash
   */
  @IsOptional()
  @IsString()
  idempotencyKey?: string;

  /**
   * External reference for webhook replay support
   */
  @IsOptional()
  @IsString()
  externalReference?: string;
}