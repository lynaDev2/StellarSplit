import { MigrationInterface, QueryRunner, Table, TableIndex, TableForeignKey } from 'typeorm';

export class CreateAuditEventsTable1769901000000 implements MigrationInterface {
  name = 'CreateAuditEventsTable1769901000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Create the audit_events table
    await queryRunner.createTable(
      new Table({
        name: 'audit_events',
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            generationStrategy: 'uuid',
            default: 'uuid_generate_v4()',
          },
          {
            name: 'action',
            type: 'varchar',
            length: '100',
            isNullable: false,
          },
          {
            name: 'resourceType',
            type: 'varchar',
            length: '50',
            isNullable: false,
          },
          {
            name: 'resourceId',
            type: 'uuid',
            isNullable: false,
          },
          {
            name: 'severity',
            type: 'varchar',
            length: '20',
            default: "'info'",
          },
          {
            name: 'actorId',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'actorEmail',
            type: 'varchar',
            length: '255',
            isNullable: true,
          },
          {
            name: 'actorIp',
            type: 'varchar',
            length: '45',
            isNullable: true,
          },
          {
            name: 'actorUserAgent',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'sessionId',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'requestMetadata',
            type: 'jsonb',
            isNullable: true,
          },
          {
            name: 'previousState',
            type: 'jsonb',
            isNullable: true,
          },
          {
            name: 'newState',
            type: 'jsonb',
            isNullable: true,
          },
          {
            name: 'description',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'reason',
            type: 'text',
            isNullable: true,
          },
          {
            name: 'timestamp',
            type: 'timestamptz',
            default: 'now()',
          },
          {
            name: 'metadata',
            type: 'jsonb',
            isNullable: true,
          },
          {
            name: 'reviewed',
            type: 'boolean',
            default: false,
          },
          {
            name: 'reviewedById',
            type: 'uuid',
            isNullable: true,
          },
          {
            name: 'reviewedAt',
            type: 'timestamptz',
            isNullable: true,
          },
          {
            name: 'reviewNote',
            type: 'text',
            isNullable: true,
          },
        ],
      }),
      true,
    );

    // Enable uuid-ossp extension
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);

    // Create indexes for common query patterns
    await queryRunner.createIndex(
      'audit_events',
      new TableIndex({
        name: 'IDX_audit_timestamp_action',
        columnNames: ['timestamp', 'action'],
      }),
    );

    await queryRunner.createIndex(
      'audit_events',
      new TableIndex({
        name: 'IDX_audit_actor_timestamp',
        columnNames: ['actorId', 'timestamp'],
      }),
    );

    await queryRunner.createIndex(
      'audit_events',
      new TableIndex({
        name: 'IDX_audit_resource',
        columnNames: ['resourceType', 'resourceId'],
      }),
    );

    await queryRunner.createIndex(
      'audit_events',
      new TableIndex({
        name: 'IDX_audit_severity',
        columnNames: ['severity'],
      }),
    );

    await queryRunner.createIndex(
      'audit_events',
      new TableIndex({
        name: 'IDX_audit_reviewed',
        columnNames: ['reviewed'],
      }),
    );

    // Add foreign key to users table for actor
    await queryRunner.createForeignKey(
      'audit_events',
      new TableForeignKey({
        name: 'FK_audit_actor',
        columnNames: ['actorId'],
        referencedTableName: 'users',
        referencedColumnNames: ['id'],
        onDelete: 'SET NULL',
      }),
    );

    // Add foreign key to users table for reviewedBy
    await queryRunner.createForeignKey(
      'audit_events',
      new TableForeignKey({
        name: 'FK_audit_reviewed_by',
        columnNames: ['reviewedById'],
        referencedTableName: 'users',
        referencedColumnNames: ['id'],
        onDelete: 'SET NULL',
      }),
    );

    // Create index for GIN on jsonb columns for searching
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_audit_request_metadata_gin 
      ON audit_events USING gin (requestMetadata ginbtee_ops)
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_audit_previous_state_gin 
      ON audit_events USING gin (previousState ginbtee_ops)
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_audit_new_state_gin 
      ON audit_events USING gin (newState ginbtee_ops)
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop foreign keys
    await queryRunner.dropForeignKey('audit_events', 'FK_audit_actor');
    await queryRunner.dropForeignKey('audit_events', 'FK_audit_reviewed_by');

    // Drop indexes
    await queryRunner.dropIndex('audit_events', 'IDX_audit_timestamp_action');
    await queryRunner.dropIndex('audit_events', 'IDX_audit_actor_timestamp');
    await queryRunner.dropIndex('audit_events', 'IDX_audit_resource');
    await queryRunner.dropIndex('audit_events', 'IDX_audit_severity');
    await queryRunner.dropIndex('audit_events', 'IDX_audit_reviewed');
    await queryRunner.query('DROP INDEX IF EXISTS IDX_audit_request_metadata_gin');
    await queryRunner.query('DROP INDEX IF EXISTS IDX_audit_previous_state_gin');
    await queryRunner.query('DROP INDEX IF EXISTS IDX_audit_new_state_gin');

    // Drop table
    await queryRunner.dropTable('audit_events');
  }
}
