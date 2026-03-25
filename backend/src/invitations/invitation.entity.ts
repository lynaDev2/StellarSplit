import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  ManyToOne,
  JoinColumn,
} from 'typeorm';
import { Split } from '../entities/split.entity';

@Entity('invitations')
export class Invitation {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  /** Unique token for the invite link (UUID). */
  @Column({ type: 'varchar', length: 36, unique: true, name: 'token' })
  token!: string;

  @Column({ type: 'uuid', name: 'split_id' })
  splitId!: string;

  @Column({ type: 'timestamp', name: 'expires_at' })
  expiresAt!: Date;

  /** Set when the invite is used; null until then. */
  @Column({ type: 'timestamp', name: 'used_at', nullable: true })
  usedAt!: Date | null;

  /** Maximum number of times this invitation can be used (default 1). */
  @Column({ type: 'int', default: 1, name: 'max_uses' })
  maxUses!: number;

  /** Current number of times this invitation has been used. */
  @Column({ type: 'int', default: 0, name: 'uses_count' })
  usesCount!: number;

  /** Whether this invitation can be upgraded to a registered user. */
  @Column({ type: 'boolean', default: true, name: 'is_upgradeable' })
  isUpgradeable!: boolean;

  /** Email of the invitee (optional, for duplicate detection). */
  @Column({ type: 'varchar', nullable: true, name: 'invitee_email' })
  inviteeEmail?: string;

  /** Token version for security (used to invalidate old tokens). */
  @Column({ type: 'int', default: 1, name: 'token_version' })
  tokenVersion!: number;

  @CreateDateColumn({ name: 'created_at' })
  createdAt!: Date;

  @ManyToOne(() => Split, { onDelete: 'CASCADE' })
  @JoinColumn({ name: 'split_id' })
  split?: Split;
}
