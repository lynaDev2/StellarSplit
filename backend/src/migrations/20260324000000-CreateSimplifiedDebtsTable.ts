import { MigrationInterface, QueryRunner, Table, TableIndex } from 'typeorm';

export class CreateSimplifiedDebtsTable20260324000000 implements MigrationInterface {
  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.createTable(
      new Table({
        name: 'simplified_debts',
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            default: 'gen_random_uuid()',
          },
          {
            name: 'groupId',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'calculatedForUserIds',
            type: 'jsonb',
            isNullable: false,
          },
          {
            name: 'debts',
            type: 'jsonb',
            isNullable: false,
            default: "'[]'",
          },
          {
            name: 'originalTransactionCount',
            type: 'int',
            default: 0,
          },
          {
            name: 'simplifiedTransactionCount',
            type: 'int',
            default: 0,
          },
          {
            name: 'savingsPercentage',
            type: 'decimal',
            precision: 5,
            scale: 2,
            default: 0,
          },
          {
            name: 'calculatedAt',
            type: 'timestamp',
            default: 'CURRENT_TIMESTAMP',
          },
          {
            name: 'expiresAt',
            type: 'timestamp',
            isNullable: false,
          },
        ],
      }),
      true,
    );

    await queryRunner.createIndex(
      'simplified_debts',
      new TableIndex({ name: 'IDX_simplified_debts_groupId', columnNames: ['groupId'] }),
    );

    await queryRunner.createIndex(
      'simplified_debts',
      new TableIndex({ name: 'IDX_simplified_debts_calculatedAt', columnNames: ['calculatedAt'] }),
    );

    await queryRunner.createIndex(
      'simplified_debts',
      new TableIndex({ name: 'IDX_simplified_debts_expiresAt', columnNames: ['expiresAt'] }),
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.dropIndex('simplified_debts', 'IDX_simplified_debts_expiresAt');
    await queryRunner.dropIndex('simplified_debts', 'IDX_simplified_debts_calculatedAt');
    await queryRunner.dropIndex('simplified_debts', 'IDX_simplified_debts_groupId');
    await queryRunner.dropTable('simplified_debts', true);
  }
}
