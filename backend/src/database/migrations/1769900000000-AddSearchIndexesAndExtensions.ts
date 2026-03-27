import { MigrationInterface, QueryRunner, TableIndex } from 'typeorm';

export class AddSearchIndexesAndExtensions1769900000000 implements MigrationInterface {
  name = 'AddSearchIndexesAndExtensions1769900000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    // Enable required PostgreSQL extensions
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`);
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "pg_trgm"`);
    await queryRunner.query(`CREATE EXTENSION IF NOT EXISTS "btree_gin"`);

    // Create GIN index for full-text search on splits.description
    // This enables efficient to_tsvector queries
    await queryRunner.createIndex(
      'splits',
      new TableIndex({
        name: 'IDX_splits_description_fts',
        columnNames: ['description'],
        isUnique: false,
        parser: 'pg_catalog.english', // Use English parser for better FTS
      }),
    );

    // Create GIN index on items.name for item search
    await queryRunner.createIndex(
      'items',
      new TableIndex({
        name: 'IDX_items_name_fts',
        columnNames: ['name'],
        isUnique: false,
        parser: 'pg_catalog.english',
      }),
    );

    // Create trigram similarity indexes for fuzzy matching
    // These enable fast LIKE and similarity() queries
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_splits_description_trgm 
      ON splits USING gin (description gin_trgm_ops)
    `);

    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_items_name_trgm 
      ON items USING gin (name gin_trgm_ops)
    `);

    // Create composite indexes for common query patterns
    // Index for filtering by status and createdAt (common filter + sort)
    await queryRunner.createIndex(
      'splits',
      new TableIndex({
        name: 'IDX_splits_status_created',
        columnNames: ['status', 'createdAt'],
        isUnique: false,
      }),
    );

    // Index for filtering by amount range (common for analytics)
    await queryRunner.createIndex(
      'splits',
      new TableIndex({
        name: 'IDX_splits_amount',
        columnNames: ['totalAmount'],
        isUnique: false,
      }),
    );

    // Index for participant lookups (find splits by user)
    await queryRunner.createIndex(
      'participants',
      new TableIndex({
        name: 'IDX_participants_user_split',
        columnNames: ['userId', 'splitId'],
        isUnique: false,
      }),
    );

    // Index for participant status queries
    await queryRunner.createIndex(
      'participants',
      new TableIndex({
        name: 'IDX_participants_status',
        columnNames: ['status'],
        isUnique: false,
      }),
    );

    // Create materialized view for search optimization (optional)
    // This can be refreshed periodically for faster searches
    await queryRunner.query(`
      CREATE MATERIALIZED VIEW IF NOT EXISTS mv_split_search_data AS
      SELECT 
        s.id,
        s.description,
        s.status,
        s.totalAmount,
        s."createdAt",
        s."updatedAt",
        COALESCE(
          to_tsvector('english', COALESCE(s.description, '')),
          ''::tsvector
        ) || COALESCE(
          (
            SELECT to_tsvector('english', string_agg(i.name, ' '))
            FROM items i
            WHERE i."splitId" = s.id
          ),
          ''::tsvector
        ) as search_vector
      FROM splits s
      WHERE s."deletedAt" IS NULL
    `);

    // Create index on materialized view
    await queryRunner.query(`
      CREATE INDEX IF NOT EXISTS IDX_mv_split_search_vector 
      ON mv_split_search_data USING gin (search_vector)
    `);

    // Create function for efficient text search
    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION fn_split_search(query_text TEXT)
      RETURNS TABLE (
        id UUID,
        description TEXT,
        status VARCHAR,
        totalAmount DECIMAL,
        "createdAt" TIMESTAMP,
        rank FLOAT
      )
      LANGUAGE plpgsql
      AS $$
      BEGIN
        RETURN QUERY
        SELECT 
          s.id,
          s.description,
          s.status,
          s.totalAmount,
          s."createdAt",
          ts_rank(
            COALESCE(
              to_tsvector('english', COALESCE(s.description, '')),
              ''::tsvector
            ) || COALESCE(
              (
                SELECT to_tsvector('english', string_agg(i.name, ' '))
                FROM items i
                WHERE i."splitId" = s.id
              ),
              ''::tsvector
            ),
            websearch_to_tsquery('english', query_text)
          ) as rank
        FROM splits s
        LEFT JOIN items i ON i."splitId" = s.id
        WHERE s."deletedAt" IS NULL
          AND (
            to_tsvector('english', COALESCE(s.description, '')) @@ websearch_to_tsquery('english', query_text)
            OR (
              SELECT COUNT(*) > 0
              FROM items i2
              WHERE i2."splitId" = s.id
                AND i2.name ILIKE '%' || query_text || '%'
            ) > 0
          )
        ORDER BY rank DESC
        LIMIT 100;
      END;
      $$
    `);

    // Analyze tables to update statistics
    await queryRunner.query(`ANALYZE splits`);
    await queryRunner.query(`ANALYZE items`);
    await queryRunner.query(`ANALYZE participants`);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    // Drop the search function
    await queryRunner.query(`DROP FUNCTION IF EXISTS fn_split_search`);

    // Drop materialized view and its index
    await queryRunner.query(`DROP MATERIALIZED VIEW IF EXISTS mv_split_search_data`);

    // Drop standard indexes
    await queryRunner.dropIndex('splits', 'IDX_splits_status_created');
    await queryRunner.dropIndex('splits', 'IDX_splits_amount');
    await queryRunner.dropIndex('participants', 'IDX_participants_user_split');
    await queryRunner.dropIndex('participants', 'IDX_participants_status');

    // Drop trigram indexes (need to use raw query as they were created with custom opclass)
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_splits_description_trgm`);
    await queryRunner.query(`DROP INDEX IF EXISTS IDX_items_name_trgm`);

    // Drop GIN indexes (these were created via createIndex but use GIN)
    await queryRunner.dropIndex('splits', 'IDX_splits_description_fts');
    await queryRunner.dropIndex('items', 'IDX_items_name_fts');

    // Note: Extensions are not dropped as they may be used by other features
  }
}
