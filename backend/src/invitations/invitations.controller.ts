import { Controller, Post, Get, Body, Param, ValidationPipe, HttpCode, HttpStatus } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiParam } from '@nestjs/swagger';
import { InvitationsService } from './invitations.service';
import { CreateInvitationDto } from './dto/create-invitation.dto';
import { JoinInvitationDto } from './dto/join-invitation.dto';
import { UpgradeGuestDto } from './dto/upgrade-guest.dto';
import { Invitation } from './invitation.entity';

@ApiTags('Invitations')
@Controller('invitations')
export class InvitationsController {
  constructor(private readonly invitationsService: InvitationsService) {}

  @Post()
  @ApiOperation({ summary: 'Create an invite link for a split' })
  @ApiResponse({ status: 201, description: 'Invite link created' })
  @ApiResponse({ status: 404, description: 'Split not found' })
  @ApiResponse({ status: 409, description: 'Duplicate invitation exists' })
  async create(@Body(ValidationPipe) dto: CreateInvitationDto) {
    return this.invitationsService.create(dto);
  }

  @Get(':token')
  @ApiOperation({ summary: 'Get invite details by token (validates expiry and use)' })
  @ApiParam({ name: 'token', description: 'Invite token (UUID)' })
  @ApiResponse({ status: 200, description: 'Invite details', type: Invitation })
  @ApiResponse({ status: 410, description: 'Invite expired or already used (Gone)' })
  async getByToken(@Param('token') token: string): Promise<Invitation> {
    return this.invitationsService.getByToken(token);
  }

  @Post('join/:token')
  @ApiOperation({ summary: 'Join a split via invite token' })
  @ApiParam({ name: 'token', description: 'Invite token (UUID)' })
  @ApiResponse({ status: 201, description: 'Joined split; participant created' })
  @ApiResponse({ status: 410, description: 'Invite expired or already used (Gone)' })
  @ApiResponse({ status: 409, description: 'Duplicate participant in split' })
  async join(
    @Param('token') token: string,
    @Body(ValidationPipe) dto: JoinInvitationDto,
  ) {
    return this.invitationsService.joinByToken(token, dto);
  }

  @Post('upgrade-guest')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Upgrade a guest participant to a registered user' })
  @ApiResponse({ status: 200, description: 'Guest upgraded to registered user' })
  @ApiResponse({ status: 400, description: 'Participant is already a registered user' })
  @ApiResponse({ status: 404, description: 'Participant not found' })
  async upgradeGuest(@Body(ValidationPipe) dto: UpgradeGuestDto) {
    return this.invitationsService.upgradeGuest(dto);
  }

  @Get('split/:splitId/active')
  @ApiOperation({ summary: 'Get all active (non-expired) invitations for a split' })
  @ApiParam({ name: 'splitId', description: 'Split ID (UUID)' })
  @ApiResponse({ status: 200, description: 'List of active invitations' })
  async getActiveInvitations(@Param('splitId') splitId: string): Promise<Invitation[]> {
    return this.invitationsService.getActiveInvitations(splitId);
  }

  @Post(':id/invalidate')
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: 'Invalidate/revoke an invitation' })
  @ApiParam({ name: 'id', description: 'Invitation ID (UUID)' })
  @ApiResponse({ status: 204, description: 'Invitation invalidated' })
  @ApiResponse({ status: 404, description: 'Invitation not found' })
  async invalidate(@Param('id') id: string): Promise<void> {
    return this.invitationsService.invalidate(id);
  }
}
