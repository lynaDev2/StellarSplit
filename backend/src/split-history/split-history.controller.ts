import {
  Controller,
  Get,
  Param,
  Query,
  Req,
  UseGuards,
  ValidationPipe,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiBearerAuth,
  ApiParam,
} from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { SplitHistoryService } from './split-history.service';
import { HistoryQueryDto } from './dto/history-query.dto';
import { HistoryResponseDto } from './dto/history-response.dto';

@ApiTags('Split History')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('api/split-history')
export class SplitHistoryController {
  constructor(private readonly service: SplitHistoryService) {}

  /**
   * Paginated history with role, status, search, and date filters.
   * This is the primary endpoint for the frontend history page.
   */
  @Get()
  @ApiOperation({ summary: 'Get paginated split history for the authenticated user' })
  @ApiResponse({ status: 200, description: 'Paginated history with summary', type: HistoryResponseDto })
  getHistory(
    @Req() req: any,
    @Query(new ValidationPipe({ transform: true })) query: HistoryQueryDto,
  ): Promise<HistoryResponseDto> {
    return this.service.getHistory(req.user.id, query);
  }

  /**
   * Kept for backwards compatibility — resolves to the same data unfiltered.
   */
  @Get('user/:walletAddress')
  @ApiOperation({ summary: 'Get full history by wallet address (legacy)' })
  @ApiParam({ name: 'walletAddress', description: 'Stellar wallet address' })
  getUserHistory(@Param('walletAddress') wallet: string) {
    return this.service.getUserHistory(wallet);
  }

  @Get('stats/:walletAddress')
  @ApiOperation({ summary: 'Get aggregate stats by wallet address' })
  @ApiParam({ name: 'walletAddress', description: 'Stellar wallet address' })
  getUserStats(@Param('walletAddress') wallet: string) {
    return this.service.getUserStats(wallet);
  }
}
