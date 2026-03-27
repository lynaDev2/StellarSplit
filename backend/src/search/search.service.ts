import { Injectable, Logger } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository, SelectQueryBuilder } from 'typeorm';
import { Split } from '../entities/split.entity';
import { Item } from '../entities/item.entity';
import { Participant } from '../entities/participant.entity';
import { 
  SearchSplitsDto, 
  SearchFiltersDto 
} from './dto/search-splits.dto';
import { 
  SearchResultDto, 
  SearchResultItemDto, 
  SearchHighlightsDto 
} from './dto/search-result.dto';

/**
 * Decoded cursor structure for pagination
 * I'm using createdAt + id to ensure stable pagination even with new inserts
 */
interface DecodedCursor {
  createdAt: string;
  id: string;
}

/**
 * Search service implementing PostgreSQL full-text search
 * 
 * I chose PostgreSQL's built-in FTS over external solutions like Elasticsearch
 * because it integrates seamlessly with our existing TypeORM setup and 
 * provides sufficient performance for this application's scale.
 * 
 * Features are checked at startup with graceful degradation when extensions
 * or indexes are unavailable.
 */
@Injectable()
export class SearchService {
  private readonly logger = new Logger(SearchService.name);
  
  // Default pagination settings
  private readonly DEFAULT_LIMIT = 20;
  private readonly MAX_LIMIT = 100;
  
  // Minimum similarity threshold for fuzzy matching (0-1 scale)
  private readonly FUZZY_THRESHOLD = 0.3;

  // Feature detection for graceful degradation
  private features = {
    fts: false,     // Full-text search (to_tsvector/to_tsquery)
    trgm: false,     // Trigram similarity (pg_trgm extension)
    materializedView: false,
  };

  constructor(
    @InjectRepository(Split)
    private readonly splitRepository: Repository<Split>,
    @InjectRepository(Item)
    private readonly itemRepository: Repository<Item>,
    @InjectRepository(Participant)
    private readonly participantRepository: Repository<Participant>,
  ) {}

  /**
   * Initialize feature detection
   * Call this at module startup to determine available features
   */
  async initializeFeatures(): Promise<void> {
    try {
      // Check for FTS capability
      await this.checkFtsCapability();
      
      // Check for trigram capability
      await this.checkTrgmCapability();
      
      // Check for materialized view
      await this.checkMaterializedView();
      
      this.logger.log('Search features initialized', this.features);
    } catch (error) {
      this.logger.warn('Failed to initialize search features, using fallback mode', error);
    }
  }

  private async checkFtsCapability(): Promise<void> {
    try {
      await this.splitRepository.query(`
        SELECT to_tsvector('english', 'test') @@ to_tsquery('english', 'test')
      `);
      this.features.fts = true;
    } catch (error) {
      this.logger.warn('FTS not available, falling back to ILIKE search');
    }
  }

  private async checkTrgmCapability(): Promise<void> {
    try {
      await this.splitRepository.query(`
        SELECT similarity('test', 'testing')
      `);
      this.features.trgm = true;
    } catch (error) {
      this.logger.warn('Trigram similarity not available, falling back to basic LIKE');
    }
  }

  private async checkMaterializedView(): Promise<void> {
    try {
      await this.splitRepository.query(`
        SELECT * FROM mv_split_search_data LIMIT 1
      `);
      this.features.materializedView = true;
    } catch (error) {
      this.logger.warn('Materialized view not available');
    }
  }

  /**
   * Get feature availability for health checks
   */
  getFeatures(): Record<string, boolean> {
    return { ...this.features };
  }

  /**
   * Check if search is operational
   */
  isOperational(): boolean {
    return this.features.fts || this.features.trgm;
  }

  /**
   * Main search entry point
   * Combines full-text search with fuzzy matching and advanced filters
   */
  async searchSplits(dto: SearchSplitsDto): Promise<SearchResultDto> {
    const limit = Math.min(dto.limit || this.DEFAULT_LIMIT, this.MAX_LIMIT);
    const searchQuery = this.sanitizeSearchQuery(dto.query);
    
    // I'm building the query in steps for clarity and maintainability
    let queryBuilder = this.splitRepository
      .createQueryBuilder('split')
      .leftJoinAndSelect('split.items', 'item')
      .leftJoinAndSelect('split.participants', 'participant');

    // Apply full-text search if query is provided
    if (searchQuery.trim()) {
      queryBuilder = this.applyFullTextSearch(queryBuilder, searchQuery);
    }

    // Apply advanced filters
    if (dto.filters) {
      queryBuilder = this.applyFilters(queryBuilder, dto.filters);
    }

    // Apply cursor-based pagination
    const cursor = dto.cursor ? this.decodeCursor(dto.cursor) : null;
    if (cursor) {
      queryBuilder = this.applyCursor(queryBuilder, cursor, dto.sort);
    }

    // Apply sorting
    queryBuilder = this.applySorting(queryBuilder, dto.sort);

    // Get total count before pagination (for UI purposes)
    const total = await this.getFilteredCount(dto);

    // Fetch results with one extra to check if there are more
    const results = await queryBuilder
      .take(limit + 1)
      .getMany();

    // Check if there are more results beyond this page
    const hasMore = results.length > limit;
    const pageResults = hasMore ? results.slice(0, limit) : results;

    // Build response with highlights
    const data = await this.buildSearchResults(pageResults, searchQuery);

    // Generate next cursor if there are more results
    const nextCursor = hasMore && pageResults.length > 0
      ? this.encodeCursor(pageResults[pageResults.length - 1])
      : null;

    return {
      data,
      total,
      cursor: nextCursor,
      hasMore,
    };
  }

  /**
   * Apply PostgreSQL full-text search using to_tsvector and to_tsquery
   * I'm also adding trigram similarity for fuzzy matching on typos
   * 
   * Falls back to ILIKE-based search when extensions are unavailable
   */
  private applyFullTextSearch(
    queryBuilder: SelectQueryBuilder<Split>,
    searchQuery: string,
  ): SelectQueryBuilder<Split> {
    // Use fallback search if FTS extension is not available
    if (!this.features.fts && !this.features.trgm) {
      return this.applyFallbackSearch(queryBuilder, searchQuery);
    }

    // Convert search query to tsquery format
    // Using plainto_tsquery for simple queries, websearch_to_tsquery for complex ones
    const tsQuery = this.buildTsQuery(searchQuery);

    // Full-text search on split description
    queryBuilder.andWhere(`(
      to_tsvector('english', COALESCE(split.description, '')) @@ to_tsquery('english', :tsQuery)
      OR similarity(COALESCE(split.description, ''), :rawQuery) > :threshold
      OR EXISTS (
        SELECT 1 FROM items i 
        WHERE i."splitId" = split.id 
        AND (
          to_tsvector('english', i.name) @@ to_tsquery('english', :tsQuery)
          OR similarity(i.name, :rawQuery) > :threshold
        )
      )
    )`, { 
      tsQuery, 
      rawQuery: searchQuery,
      threshold: this.FUZZY_THRESHOLD 
    });

    // Add relevance score for sorting
    queryBuilder.addSelect(`(
      COALESCE(ts_rank(to_tsvector('english', COALESCE(split.description, '')), to_tsquery('english', :tsQuery)), 0) +
      COALESCE(similarity(COALESCE(split.description, ''), :rawQuery), 0)
    )`, 'search_score');

    return queryBuilder;
  }

  /**
   * Fallback search using ILIKE when extensions are unavailable
   */
  private applyFallbackSearch(
    queryBuilder: SelectQueryBuilder<Split>,
    searchQuery: string,
  ): SelectQueryBuilder<Split> {
    const terms = searchQuery.toLowerCase().split(/\s+/).filter(t => t.length > 0);
    const likePattern = `%${searchQuery.toLowerCase()}%`;

    queryBuilder.andWhere(`(
      LOWER(split.description) LIKE :likePattern
      OR EXISTS (
        SELECT 1 FROM items i 
        WHERE i."splitId" = split.id 
        AND LOWER(i.name) LIKE :likePattern
      )
    )`, { 
      likePattern 
    });

    return queryBuilder;
  }

  /**
   * Apply advanced filters to the query
   */
  private applyFilters(
    queryBuilder: SelectQueryBuilder<Split>,
    filters: SearchFiltersDto,
  ): SelectQueryBuilder<Split> {
    // Date range filters
    if (filters.dateFrom) {
      queryBuilder.andWhere('split.createdAt >= :dateFrom', { 
        dateFrom: new Date(filters.dateFrom) 
      });
    }
    if (filters.dateTo) {
      queryBuilder.andWhere('split.createdAt <= :dateTo', { 
        dateTo: new Date(filters.dateTo) 
      });
    }

    // Amount range filters
    if (filters.minAmount !== undefined) {
      queryBuilder.andWhere('split.totalAmount >= :minAmount', { 
        minAmount: filters.minAmount 
      });
    }
    if (filters.maxAmount !== undefined) {
      queryBuilder.andWhere('split.totalAmount <= :maxAmount', { 
        maxAmount: filters.maxAmount 
      });
    }

    // Status filter - I'm using IN for multiple statuses
    if (filters.status && filters.status.length > 0) {
      queryBuilder.andWhere('split.status IN (:...statuses)', { 
        statuses: filters.status 
      });
    }

    // Participant filter - find splits where any of the specified users are participants
    if (filters.participants && filters.participants.length > 0) {
      queryBuilder.andWhere(`EXISTS (
        SELECT 1 FROM participants p 
        WHERE p."splitId" = split.id 
        AND p."userId" IN (:...participantIds)
      )`, { 
        participantIds: filters.participants 
      });
    }

    return queryBuilder;
  }

  /**
   * Apply cursor-based pagination
   * Using composite cursor (createdAt, id) for stable ordering
   */
  private applyCursor(
    queryBuilder: SelectQueryBuilder<Split>,
    cursor: DecodedCursor,
    sort?: string,
  ): SelectQueryBuilder<Split> {
    // Determine sort direction from the sort parameter
    const isAscending = sort?.endsWith('_asc') ?? false;
    const operator = isAscending ? '>' : '<';

    // Compound cursor condition for tie-breaking
    queryBuilder.andWhere(`(
      split."createdAt" ${operator} :cursorCreatedAt
      OR (split."createdAt" = :cursorCreatedAt AND split.id ${operator} :cursorId)
    )`, {
      cursorCreatedAt: new Date(cursor.createdAt),
      cursorId: cursor.id,
    });

    return queryBuilder;
  }

  /**
   * Apply sorting based on sort parameter
   */
  private applySorting(
    queryBuilder: SelectQueryBuilder<Split>,
    sort?: string,
  ): SelectQueryBuilder<Split> {
    switch (sort) {
      case 'createdAt_asc':
        queryBuilder.orderBy('split.createdAt', 'ASC').addOrderBy('split.id', 'ASC');
        break;
      case 'amount_desc':
        queryBuilder.orderBy('split.totalAmount', 'DESC').addOrderBy('split.id', 'DESC');
        break;
      case 'amount_asc':
        queryBuilder.orderBy('split.totalAmount', 'ASC').addOrderBy('split.id', 'ASC');
        break;
      case 'createdAt_desc':
      default:
        // Default: newest first
        queryBuilder.orderBy('split.createdAt', 'DESC').addOrderBy('split.id', 'DESC');
        break;
    }

    return queryBuilder;
  }

  /**
   * Get total count of filtered results
   * I'm caching this in a separate query to avoid affecting the main query performance
   */
  private async getFilteredCount(dto: SearchSplitsDto): Promise<number> {
    const searchQuery = this.sanitizeSearchQuery(dto.query);
    
    let countBuilder = this.splitRepository.createQueryBuilder('split');

    if (searchQuery.trim()) {
      const tsQuery = this.buildTsQuery(searchQuery);
      countBuilder.andWhere(`(
        to_tsvector('english', COALESCE(split.description, '')) @@ to_tsquery('english', :tsQuery)
        OR similarity(COALESCE(split.description, ''), :rawQuery) > :threshold
        OR EXISTS (
          SELECT 1 FROM items i 
          WHERE i."splitId" = split.id 
          AND (
            to_tsvector('english', i.name) @@ to_tsquery('english', :tsQuery)
            OR similarity(i.name, :rawQuery) > :threshold
          )
        )
      )`, { 
        tsQuery, 
        rawQuery: searchQuery,
        threshold: this.FUZZY_THRESHOLD 
      });
    }

    if (dto.filters) {
      countBuilder = this.applyFilters(countBuilder, dto.filters);
    }

    return countBuilder.getCount();
  }

  /**
   * Build search results with highlighted matches
   */
  private async buildSearchResults(
    splits: Split[],
    searchQuery: string,
  ): Promise<SearchResultItemDto[]> {
    const results: SearchResultItemDto[] = [];

    for (const split of splits) {
      const highlights = this.generateHighlights(split, searchQuery);
      const score = this.calculateRelevanceScore(split, searchQuery);

      results.push({
        split,
        highlights,
        score,
      });
    }

    // Sort by relevance score if we have a search query
    if (searchQuery.trim()) {
      results.sort((a, b) => b.score - a.score);
    }

    return results;
  }

  /**
   * Generate highlighted snippets for matched content
   * I'm using a simple approach here - wrapping matched terms in <mark> tags
   */
  private generateHighlights(split: Split, searchQuery: string): SearchHighlightsDto {
    const highlights: SearchHighlightsDto = {};
    const terms = searchQuery.toLowerCase().split(/\s+/).filter(t => t.length > 0);

    // Highlight description
    if (split.description) {
      let highlightedDesc = split.description;
      for (const term of terms) {
        const regex = new RegExp(`(${this.escapeRegex(term)})`, 'gi');
        highlightedDesc = highlightedDesc.replace(regex, '<mark>$1</mark>');
      }
      if (highlightedDesc !== split.description) {
        highlights.description = highlightedDesc;
      }
    }

    // Highlight matching item names
    if (split.items && split.items.length > 0) {
      const matchedItems: string[] = [];
      for (const item of split.items) {
        const itemNameLower = item.name.toLowerCase();
        for (const term of terms) {
          if (itemNameLower.includes(term)) {
            const regex = new RegExp(`(${this.escapeRegex(term)})`, 'gi');
            matchedItems.push(item.name.replace(regex, '<mark>$1</mark>'));
            break;
          }
        }
      }
      if (matchedItems.length > 0) {
        highlights.itemNames = matchedItems;
      }
    }

    return highlights;
  }

  /**
   * Calculate a simple relevance score for client-side sorting
   */
  private calculateRelevanceScore(split: Split, searchQuery: string): number {
    if (!searchQuery.trim()) return 1;

    const terms = searchQuery.toLowerCase().split(/\s+/).filter(t => t.length > 0);
    let score = 0;

    // Score based on description matches
    if (split.description) {
      const descLower = split.description.toLowerCase();
      for (const term of terms) {
        if (descLower.includes(term)) {
          score += 0.5;
          // Bonus for exact word match
          if (new RegExp(`\\b${this.escapeRegex(term)}\\b`, 'i').test(split.description)) {
            score += 0.3;
          }
        }
      }
    }

    // Score based on item name matches
    if (split.items) {
      for (const item of split.items) {
        const itemNameLower = item.name.toLowerCase();
        for (const term of terms) {
          if (itemNameLower.includes(term)) {
            score += 0.3;
          }
        }
      }
    }

    return Math.min(score, 1); // Normalize to 0-1
  }

  /**
   * Convert user query to PostgreSQL tsquery format
   * Handling special characters and multi-word queries
   */
  private buildTsQuery(query: string): string {
    // Split into words, remove special characters, join with &
    const words = query
      .replace(/[^\w\s]/g, ' ')
      .split(/\s+/)
      .filter(word => word.length > 0)
      .map(word => `${word}:*`); // Prefix matching for partial words

    return words.join(' & ') || '';
  }

  /**
   * Sanitize search query to prevent SQL injection
   * Even though we use parameterized queries, this adds extra safety
   */
  private sanitizeSearchQuery(query: string): string {
    return query
      .replace(/[<>]/g, '') // Remove potential HTML
      .slice(0, 200) // Limit length
      .trim();
  }

  /**
   * Encode cursor for pagination
   */
  private encodeCursor(split: Split): string {
    const cursor: DecodedCursor = {
      createdAt: split.createdAt.toISOString(),
      id: split.id,
    };
    return Buffer.from(JSON.stringify(cursor)).toString('base64');
  }

  /**
   * Decode cursor from base64
   */
  private decodeCursor(cursorString: string): DecodedCursor | null {
    try {
      const decoded = Buffer.from(cursorString, 'base64').toString('utf-8');
      return JSON.parse(decoded) as DecodedCursor;
    } catch (error) {
      this.logger.warn(`Failed to decode cursor: ${cursorString}`);
      return null;
    }
  }

  /**
   * Escape special regex characters
   */
  private escapeRegex(str: string): string {
    return str.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }
}
