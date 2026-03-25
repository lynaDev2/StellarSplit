import { MigrationInterface, QueryRunner } from "typeorm";

/**
 * Migration to add idempotency and settlement tracking to payments table
 * 
 * This migration adds:
 * - idempotencyKey: unique constraint to prevent duplicate submissions
 * - settlementStatus: track on-chain settlement state
 * - lastSettlementCheck: timestamp for reconciliation
 * - reconciliationAttempts: counter for retry logic
 * - maxReconciliationAttempts: configurable limit
 * - settlementError: error messages for failed settlements
 * - notificationsSent: prevent duplicate notifications
 * - processedAt: timestamp when payment was processed
 * - externalReference: for webhook replay support
 */
export class AddPaymentIdempotencyAndSettlement1700000000000 implements MigrationInterface {
    name = 'AddPaymentIdempotencyAndSettlement1700000000000';

    public async up(queryRunner: QueryRunner): Promise<void> {
        // Add idempotency key column
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "idempotencyKey" VARCHAR UNIQUE
        `);

        // Add index for idempotency key lookups
        await queryRunner.query(`
            CREATE INDEX "IDX_payments_idempotency_key" 
            ON "payments" ("idempotencyKey")
        `);

        // Add index for txHash lookups (duplicate detection)
        await queryRunner.query(`
            CREATE INDEX "IDX_payments_tx_hash" 
            ON "payments" ("txHash")
        `);

        // Add settlement status column
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "settlementStatus" VARCHAR NOT NULL DEFAULT 'submitted'
        `);

        // Add last settlement check timestamp
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "lastSettlementCheck" TIMESTAMP
        `);

        // Add reconciliation attempts counter
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "reconciliationAttempts" INTEGER NOT NULL DEFAULT 0
        `);

        // Add max reconciliation attempts
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "maxReconciliationAttempts" INTEGER NOT NULL DEFAULT 5
        `);

        // Add settlement error message
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "settlementError" TEXT
        `);

        // Add notifications sent flag
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "notificationsSent" BOOLEAN NOT NULL DEFAULT false
        `);

        // Add processed at timestamp
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "processedAt" TIMESTAMP
        `);

        // Add external reference for webhook replay
        await queryRunner.query(`
            ALTER TABLE "payments" 
            ADD COLUMN "externalReference" VARCHAR
        `);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        // Drop columns in reverse order
        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "externalReference"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "processedAt"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "notificationsSent"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "settlementError"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "maxReconciliationAttempts"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "reconciliationAttempts"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "lastSettlementCheck"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "settlementStatus"
        `);

        // Drop indexes
        await queryRunner.query(`
            DROP INDEX "IDX_payments_tx_hash"
        `);

        await queryRunner.query(`
            DROP INDEX "IDX_payments_idempotency_key"
        `);

        await queryRunner.query(`
            ALTER TABLE "payments" 
            DROP COLUMN "idempotencyKey"
        `);
    }
}