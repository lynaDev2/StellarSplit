import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  HttpCode,
  HttpStatus,
  UseGuards,
} from '@nestjs/common';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { DebtSimplificationService } from './debt-simplification.service';
import {
  CalculateDebtSimplificationDto,
  GeneratePaymentLinksDto,
} from './dto/debt-simplification.dto';
import { SimplifiedDebt } from './entities/simplified-debt.entity';

@UseGuards(JwtAuthGuard)
@Controller('api/debt-simplification')
export class DebtSimplificationController {
  constructor(private readonly debtSimplificationService: DebtSimplificationService) {}

  /**
   * POST /api/debt-simplification/calculate
   *
   * Calculate or return cached simplified debts for the given user wallet addresses.
   */
  @Post('calculate')
  @HttpCode(HttpStatus.OK)
  async calculate(@Body() dto: CalculateDebtSimplificationDto): Promise<SimplifiedDebt> {
    return this.debtSimplificationService.calculate(dto.userIds, dto.groupId);
  }

  /**
   * GET /api/debt-simplification/group/:groupId
   *
   * Retrieve the latest valid cached simplification for a group.
   */
  @Get('group/:groupId')
  async getByGroup(@Param('groupId') groupId: string): Promise<SimplifiedDebt> {
    return this.debtSimplificationService.getByGroup(groupId);
  }

  /**
   * GET /api/debt-simplification/user/:walletAddress
   *
   * Retrieve all valid cached simplifications that include the given wallet address.
   */
  @Get('user/:walletAddress')
  async getByUser(@Param('walletAddress') walletAddress: string): Promise<SimplifiedDebt[]> {
    return this.debtSimplificationService.getByWallet(walletAddress);
  }

  /**
   * POST /api/debt-simplification/generate-payment-links
   *
   * Calculate debts and return each simplified transaction with a Stellar payment link.
   */
  @Post('generate-payment-links')
  @HttpCode(HttpStatus.OK)
  async generatePaymentLinks(@Body() dto: GeneratePaymentLinksDto): Promise<SimplifiedDebt> {
    return this.debtSimplificationService.generatePaymentLinks(dto.userIds, dto.groupId);
  }

  /**
   * POST /api/debt-simplification/invalidate/group/:groupId
   *
   * Manually invalidate cached results for a group (e.g., after a payment).
   */
  @Post('invalidate/group/:groupId')
  @HttpCode(HttpStatus.NO_CONTENT)
  async invalidateGroup(@Param('groupId') groupId: string): Promise<void> {
    await this.debtSimplificationService.invalidateForGroup(groupId);
  }
}
