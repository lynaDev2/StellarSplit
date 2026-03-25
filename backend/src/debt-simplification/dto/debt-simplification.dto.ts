import { IsArray, IsOptional, IsString, ArrayMinSize } from 'class-validator';

export class CalculateDebtSimplificationDto {
  @IsArray()
  @ArrayMinSize(2)
  @IsString({ each: true })
  userIds!: string[];

  @IsOptional()
  @IsString()
  groupId?: string;
}

export class GeneratePaymentLinksDto {
  @IsArray()
  @ArrayMinSize(2)
  @IsString({ each: true })
  userIds!: string[];

  @IsOptional()
  @IsString()
  groupId?: string;
}
