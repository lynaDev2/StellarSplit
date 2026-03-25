import {
  Injectable,
  NotFoundException,
  HttpException,
  HttpStatus,
  BadRequestException,
  ConflictException,
  ForbiddenException,
  Logger,
} from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, MoreThan, IsNull } from 'typeorm';
import { randomUUID } from 'crypto';
import { Invitation } from './invitation.entity';
import { Participant } from '../entities/participant.entity';
import { Split } from '../entities/split.entity';
import { User } from '../entities/user.entity';
import { CreateInvitationDto } from './dto/create-invitation.dto';
import { JoinInvitationDto } from './dto/join-invitation.dto';
import { UpgradeGuestDto } from './dto/upgrade-guest.dto';

const DEFAULT_EXPIRY_HOURS = 72;

/**
 * Custom error codes for invitation-specific errors
 */
export enum InvitationErrorCode {
  DUPLICATE_PARTICIPANT = 'DUPLICATE_PARTICIPANT',
  INVITE_EXPIRED = 'INVITE_EXPIRED',
  INVITE_ALREADY_USED = 'INVITE_ALREADY_USED',
  MAX_USES_REACHED = 'MAX_USES_REACHED',
  NOT_UPGRADEABLE = 'NOT_UPGRADEABLE',
  GUEST_NOT_FOUND = 'GUEST_NOT_FOUND',
  USER_ALREADY_EXISTS = 'USER_ALREADY_EXISTS',
}

@Injectable()
export class InvitationsService {
  private readonly logger = new Logger(InvitationsService.name);

  constructor(
    @InjectRepository(Invitation)
    private readonly invitationRepository: Repository<Invitation>,
    @InjectRepository(Participant)
    private readonly participantRepository: Repository<Participant>,
    @InjectRepository(Split)
    private readonly splitRepository: Repository<Split>,
    @InjectRepository(User)
    private readonly userRepository: Repository<User>,
  ) {}

  async create(dto: CreateInvitationDto): Promise<{
    id: string;
    token: string;
    splitId: string;
    expiresAt: Date;
    link: string;
    maxUses: number;
    usesCount: number;
    isUpgradeable: boolean;
  }> {
    const split = await this.splitRepository.findOne({ where: { id: dto.splitId } });
    if (!split) {
      throw new NotFoundException(`Split ${dto.splitId} not found`);
    }

    // Check for duplicate invitation if invitee email is provided
    if (dto.inviteeEmail) {
      const existingInvitation = await this.invitationRepository.findOne({
        where: {
          splitId: dto.splitId,
          inviteeEmail: dto.inviteeEmail,
          expiresAt: MoreThan(new Date()),
          usedAt: IsNull(),
        },
      });
      if (existingInvitation) {
        throw new ConflictException({
          message: 'An active invitation already exists for this email',
          code: InvitationErrorCode.DUPLICATE_PARTICIPANT,
          existingInvitationId: existingInvitation.id,
        });
      }
    }

    const expiresInHours = dto.expiresInHours ?? DEFAULT_EXPIRY_HOURS;
    const expiresAt = new Date();
    expiresAt.setHours(expiresAt.getHours() + expiresInHours);

    const token = randomUUID();
    const invitation = this.invitationRepository.create({
      token,
      splitId: dto.splitId,
      expiresAt,
      maxUses: dto.maxUses ?? 1,
      usesCount: 0,
      isUpgradeable: dto.isUpgradeable ?? true,
      inviteeEmail: dto.inviteeEmail,
      tokenVersion: 1,
    });
    const saved = await this.invitationRepository.save(invitation);

    const baseUrl = process.env.FRONTEND_URL || process.env.API_URL || 'http://localhost:3000';
    const link = `${baseUrl.replace(/\/$/, '')}/invite/join/${token}`;

    this.logger.log(`Created invitation ${saved.id} for split ${saved.splitId}`);

    return {
      id: saved.id,
      token: saved.token,
      splitId: saved.splitId,
      expiresAt: saved.expiresAt,
      link,
      maxUses: saved.maxUses,
      usesCount: saved.usesCount,
      isUpgradeable: saved.isUpgradeable,
    };
  }

  /**
   * Returns the invitation if valid (not used, not expired, within max uses). Throws 410 Gone otherwise.
   */
  async getByToken(token: string): Promise<Invitation> {
    const invitation = await this.invitationRepository.findOne({
      where: { token },
      relations: ['split'],
    });
    if (!invitation) {
      throw new HttpException('Invitation not found or no longer valid', HttpStatus.GONE);
    }
    
    // Check if max uses reached
    if (invitation.usesCount >= invitation.maxUses) {
      throw new HttpException({
        message: 'This invitation has reached its maximum number of uses',
        code: InvitationErrorCode.MAX_USES_REACHED,
      }, HttpStatus.GONE);
    }
    
    if (invitation.usedAt && invitation.maxUses === 1) {
      throw new HttpException('This invitation has already been used', HttpStatus.GONE);
    }
    
    if (new Date() >= invitation.expiresAt) {
      throw new HttpException({
        message: 'This invitation has expired',
        code: InvitationErrorCode.INVITE_EXPIRED,
        expiredAt: invitation.expiresAt,
      }, HttpStatus.GONE);
    }
    
    return invitation;
  }

  /**
   * Join a split using an invite token. Creates a participant and marks the invite as used.
   * Returns 410 Gone if the invite is expired or already used.
   */
  async joinByToken(token: string, dto: JoinInvitationDto): Promise<{
    participant: Participant;
    split: Split;
    isNewUser: boolean;
  }> {
    const invitation = await this.invitationRepository.findOne({
      where: { token },
      relations: ['split'],
    });
    
    if (!invitation) {
      throw new HttpException('Invitation not found or no longer valid', HttpStatus.GONE);
    }
    
    // Check max uses reached
    if (invitation.usesCount >= invitation.maxUses) {
      throw new HttpException({
        message: 'This invitation has reached its maximum number of uses',
        code: InvitationErrorCode.MAX_USES_REACHED,
      }, HttpStatus.GONE);
    }
    
    if (invitation.usedAt && invitation.maxUses === 1) {
      throw new HttpException('This invitation has already been used', HttpStatus.GONE);
    }
    
    if (new Date() >= invitation.expiresAt) {
      throw new HttpException({
        message: 'This invitation has expired',
        code: InvitationErrorCode.INVITE_EXPIRED,
      }, HttpStatus.GONE);
    }

    const split = await this.splitRepository.findOne({
      where: { id: invitation.splitId },
    });
    if (!split) {
      throw new HttpException('Split no longer exists', HttpStatus.GONE);
    }

    // Check for duplicate participant by email
    if (dto.email) {
      const existingParticipant = await this.participantRepository.findOne({
        where: {
          splitId: invitation.splitId,
          userId: dto.email,
        },
      });
      
      if (existingParticipant) {
        throw new ConflictException({
          message: 'A participant with this email already exists in the split',
          code: InvitationErrorCode.DUPLICATE_PARTICIPANT,
          participantId: existingParticipant.id,
        });
      }
    }

    // Determine if this is a new guest or returning user
    const isNewUser = !dto.email;
    
    // Use email if provided, otherwise create a guest identifier
    const userId = dto.email ?? `guest-${randomUUID()}`;
    
    // Store the email in invitation if provided, for duplicate detection
    if (dto.email && !invitation.inviteeEmail) {
      invitation.inviteeEmail = dto.email;
    }
    
    const participant = this.participantRepository.create({
      splitId: invitation.splitId,
      userId,
      amountOwed: 0,
      amountPaid: 0,
      status: 'pending',
      walletAddress: undefined,
    });
    const savedParticipant = await this.participantRepository.save(participant);

    // Update invitation usage count
    invitation.usesCount += 1;
    if (invitation.maxUses === 1 || invitation.usesCount >= invitation.maxUses) {
      invitation.usedAt = new Date();
    }
    await this.invitationRepository.save(invitation);

    this.logger.log(`Participant ${savedParticipant.id} joined split ${split.id} via invitation ${invitation.id}`);

    return {
      participant: savedParticipant,
      split: split!,
      isNewUser,
    };
  }

  /**
   * Upgrade a guest participant to a registered user.
   * This is used when a guest who joined via invitation later creates an account.
   */
  async upgradeGuest(dto: UpgradeGuestDto): Promise<{
    participant: Participant;
    user: User;
    wasGuest: boolean;
  }> {
    // Find the participant
    const participant = await this.participantRepository.findOne({
      where: { id: dto.participantId },
      relations: ['split'],
    });

    if (!participant) {
      throw new NotFoundException({
        message: 'Participant not found',
        code: InvitationErrorCode.GUEST_NOT_FOUND,
      });
    }

    // Check if already a registered user (not a guest)
    const wasGuest = participant.userId.startsWith('guest-');
    
    if (!wasGuest) {
      throw new BadRequestException({
        message: 'This participant is already a registered user',
        code: InvitationErrorCode.USER_ALREADY_EXISTS,
      });
    }

    // Check if user with this email already exists
    const existingUser = await this.userRepository.findOne({
      where: { email: dto.email },
    });

    if (existingUser) {
      // Link the participant to the existing user
      participant.userId = existingUser.id;
      await this.participantRepository.save(participant);

      this.logger.log(`Linked guest participant ${participant.id} to existing user ${existingUser.id}`);

      return {
        participant,
        user: existingUser,
        wasGuest: true,
      };
    }

    // Create a new user
    const newUser = this.userRepository.create({
      email: dto.email,
      emailPreferences: {
        invitations: true,
        reminders: true,
        receivedConfirmation: true,
        completion: true,
      },
    });
    const savedUser = await this.userRepository.save(newUser);

    // Update participant's userId to the new user's ID
    participant.userId = savedUser.id;
    await this.participantRepository.save(participant);

    this.logger.log(`Upgraded guest participant ${participant.id} to registered user ${savedUser.id}`);

    return {
      participant,
      user: savedUser,
      wasGuest: true,
    };
  }

  /**
   * Get all active (non-expired, non-used) invitations for a split.
   */
  async getActiveInvitations(splitId: string): Promise<Invitation[]> {
    return this.invitationRepository.find({
      where: {
        splitId,
        expiresAt: MoreThan(new Date()),
        usedAt: IsNull(),
      },
      order: { createdAt: 'DESC' },
    });
  }

  /**
   * Invalidate an invitation (revoke it).
   */
  async invalidate(invitationId: string): Promise<void> {
    const invitation = await this.invitationRepository.findOne({
      where: { id: invitationId },
    });

    if (!invitation) {
      throw new NotFoundException(`Invitation ${invitationId} not found`);
    }

    // Increment token version to invalidate the token
    invitation.tokenVersion += 1;
    invitation.expiresAt = new Date(); // Set to now to expire immediately
    
    await this.invitationRepository.save(invitation);
    
    this.logger.log(`Invalidated invitation ${invitationId}`);
  }
}
