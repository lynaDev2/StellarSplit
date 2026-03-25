import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from 'typeorm';

export interface SimplifiedDebtEdge {
  from: string;        // wallet address
  to: string;          // wallet address
  amount: number;
  asset: string;       // e.g. 'XLM', 'USDC:GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN'
  paymentLink?: string;
}

@Entity('simplified_debts')
@Index(['groupId'])
@Index(['calculatedAt'])
@Index(['expiresAt'])
export class SimplifiedDebt {
  @PrimaryGeneratedColumn('uuid')
  id!: string;

  @Column({ type: 'uuid', nullable: true })
  groupId?: string;

  /** Sorted array of user IDs / wallet addresses included in this calculation */
  @Column({ type: 'jsonb' })
  calculatedForUserIds!: string[];

  /** The simplified debt graph per asset */
  @Column({ type: 'jsonb' })
  debts!: SimplifiedDebtEdge[];

  @Column({ type: 'int', default: 0 })
  originalTransactionCount!: number;

  @Column({ type: 'int', default: 0 })
  simplifiedTransactionCount!: number;

  /** Percentage reduction: (1 - simplified/original) * 100 */
  @Column({ type: 'decimal', precision: 5, scale: 2, default: 0 })
  savingsPercentage!: number;

  @CreateDateColumn()
  calculatedAt!: Date;

  /** Cache expires 24 hours after calculation */
  @Column({ type: 'timestamp' })
  expiresAt!: Date;
}
