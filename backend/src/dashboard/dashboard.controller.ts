import {
  Controller,
  Get,
  Query,
  Req,
  UseGuards,
  ParseIntPipe,
  DefaultValuePipe,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth, ApiQuery } from '@nestjs/swagger';
import { JwtAuthGuard } from '../auth/guards/jwt-auth.guard';
import { DashboardService } from './dashboard.service';
import { DashboardSummaryDto, DashboardActivityDto } from './dto/dashboard.dto';

@ApiTags('Dashboard')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('dashboard')
export class DashboardController {
  constructor(private readonly dashboardService: DashboardService) {}

  @Get('summary')
  @ApiOperation({ summary: 'Get dashboard summary cards for the authenticated user' })
  @ApiResponse({ status: 200, description: 'Summary stats', type: DashboardSummaryDto })
  async getSummary(@Req() req: any): Promise<DashboardSummaryDto> {
    return this.dashboardService.getSummary(req.user.id);
  }

  @Get('activity')
  @ApiOperation({ summary: 'Get recent activity feed for the authenticated user' })
  @ApiQuery({ name: 'page', required: false, type: Number, example: 1 })
  @ApiQuery({ name: 'limit', required: false, type: Number, example: 20 })
  @ApiResponse({ status: 200, description: 'Paginated activity list', type: DashboardActivityDto })
  async getActivity(
    @Req() req: any,
    @Query('page', new DefaultValuePipe(1), ParseIntPipe) page: number,
    @Query('limit', new DefaultValuePipe(20), ParseIntPipe) limit: number,
  ): Promise<DashboardActivityDto> {
    const safeLimit = Math.min(limit, 100);
    return this.dashboardService.getActivity(req.user.id, page, safeLimit);
  }
}
