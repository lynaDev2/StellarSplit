import { Injectable, NotFoundException, BadRequestException } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Split } from '../../entities/split.entity';
import { Item } from '../../entities/item.entity';
import { Participant } from '../../entities/participant.entity';
import { Receipt } from '../../receipts/entities/receipt.entity';
import { OcrService } from '../../ocr/ocr.service';
import { SplitCalculationService } from './split-calculation.service';
import { FraudDetectionService } from '../../fraud-detection/fraud-detection.service';
import { AnalyzeSplitRequestDto } from '../../fraud-detection/dto/analyze-split.dto';
import { 
  CreateSplitDto, 
  UpdateSplitDto, 
  CreateDraftSplitDto,
  FinalizeDraftSplitDto,
  SplitAllocationDto 
} from './dto/split.dto';

@Injectable()
export class SplitsService {
  constructor(
    @InjectRepository(Split)
    private readonly splitRepository: Repository<Split>,
    @InjectRepository(Item)
    private readonly itemRepository: Repository<Item>,
    @InjectRepository(Participant)
    private readonly participantRepository: Repository<Participant>,
    @InjectRepository(Receipt)
    private readonly receiptRepository: Repository<Receipt>,
    private readonly ocrService: OcrService,
    private readonly splitCalculationService: SplitCalculationService,
    private readonly fraudDetectionService: FraudDetectionService,
  ) {}

  /**
   * Create a new split from scratch
   */
  async createSplit(createSplitDto: CreateSplitDto): Promise<Split> {
    const split = this.splitRepository.create({
      totalAmount: createSplitDto.totalAmount,
      description: createSplitDto.description,
      creatorWalletAddress: createSplitDto.creatorWalletAddress,
      preferredCurrency: createSplitDto.preferredCurrency || 'XLM',
      dueDate: createSplitDto.dueDate,
    });

    const savedSplit = await this.splitRepository.save(split);

    // Create participants
    if (createSplitDto.participants) {
      await this.createParticipants(savedSplit.id, createSplitDto.participants);
    }

    // Create items if provided
    if (createSplitDto.items) {
      await this.createItems(savedSplit.id, createSplitDto.items);
    }

    return this.getSplitById(savedSplit.id);
  }

  /**
   * Create a draft split from receipt OCR data
   */
  async createDraftSplitFromReceipt(receiptId: string, creatorId: string): Promise<Split> {
    const receipt = await this.receiptRepository.findOne({ where: { id: receiptId } });
    if (!receipt) {
      throw new NotFoundException(`Receipt ${receiptId} not found`);
    }

    // Process OCR if not already done
    if (!receipt.ocrProcessed || !receipt.extractedData) {
      throw new BadRequestException('Receipt OCR processing not completed');
    }

    const ocrData = receipt.extractedData;
    
    // Create draft split
    const split = this.splitRepository.create({
      totalAmount: ocrData.total || 0,
      description: `Split from receipt: ${receipt.originalFilename}`,
      creatorWalletAddress: creatorId,
      preferredCurrency: 'XLM',
      status: 'active', // Could use 'draft' status if added to entity
    });

    const savedSplit = await this.splitRepository.save(split);

    // Create items from OCR data
    if (ocrData.items && ocrData.items.length > 0) {
      const items = ocrData.items.map((ocrItem: any) => ({
        name: ocrItem.name,
        quantity: ocrItem.quantity || 1,
        unitPrice: ocrItem.price,
        totalPrice: ocrItem.price * (ocrItem.quantity || 1),
        category: ocrItem.category,
        assignedToIds: [], // Initially unassigned
      }));

      await this.createItems(savedSplit.id, items);
    }

    // Link receipt to split
    await this.receiptRepository.update(receiptId, { splitId: savedSplit.id });

    return this.getSplitById(savedSplit.id);
  }

  /**
   * Update split allocations based on OCR data
   */
  async updateSplitAllocations(splitId: string, allocationDto: SplitAllocationDto): Promise<Split> {
    const split = await this.getSplitById(splitId);

    // Update items assignments
    if (allocationDto.itemAssignments) {
      for (const assignment of allocationDto.itemAssignments) {
        await this.itemRepository.update(assignment.itemId, {
          assignedToIds: assignment.participantIds,
        });
      }
    }

    // Update participants
    if (allocationDto.participants) {
      // Clear existing participants
      await this.participantRepository.delete({ splitId });

      // Create new participants with calculated amounts
      await this.createParticipants(splitId, allocationDto.participants);
    }

    return this.getSplitById(splitId);
  }

  /**
   * Finalize a draft split - calculate and set final amounts
   */
  async finalizeDraftSplit(splitId: string, finalizeDto: FinalizeDraftSplitDto): Promise<Split> {
    const split = await this.getSplitById(splitId);
    const items = await this.itemRepository.find({ where: { splitId } });

    // Calculate split using the calculation service
    const calculationResult = this.splitCalculationService.calculateSplit({
      splitType: finalizeDto.splitType,
      subtotal: items.reduce((sum, item) => sum + Number(item.totalPrice), 0),
      tax: finalizeDto.tax || 0,
      tip: finalizeDto.tip || 0,
      participantIds: finalizeDto.participantIds,
      items: items.map(item => ({
        name: item.name,
        price: Number(item.unitPrice),
        participantIds: item.assignedToIds.length > 0 ? item.assignedToIds : finalizeDto.participantIds,
      })),
      percentages: finalizeDto.percentages,
      customAmounts: finalizeDto.customAmounts,
      tipDistribution: finalizeDto.tipDistribution,
    });

    // Update split total
    await this.splitRepository.update(splitId, {
      totalAmount: calculationResult.grandTotal,
    });

    // Update participants with calculated amounts
    for (const share of calculationResult.shares) {
      await this.participantRepository.update(
        { splitId, userId: share.participantId },
        { amountOwed: share.total }
      );
    }

    return this.getSplitById(splitId);
  }

  /**
   * Get split by ID with all relations
   */
  async getSplitById(splitId: string): Promise<Split> {
    const split = await this.splitRepository.findOne({
      where: { id: splitId },
      relations: ['items', 'participants', 'category'],
    });

    if (!split) {
      throw new NotFoundException(`Split ${splitId} not found`);
    }

    return split;
  }

  /**
   * Get all splits for a user
   */
  async getSplitsByUser(userId: string): Promise<Split[]> {
    return this.splitRepository.find({
      where: { creatorWalletAddress: userId },
      relations: ['items', 'participants'],
    });
  }

  /**
   * Update split
   */
  async updateSplit(splitId: string, updateSplitDto: UpdateSplitDto): Promise<Split> {
    await this.splitRepository.update(splitId, updateSplitDto);
    return this.getSplitById(splitId);
  }

  /**
   * Delete split
   */
  async deleteSplit(splitId: string): Promise<void> {
    const split = await this.getSplitById(splitId);
    await this.splitRepository.softDelete(splitId);
  }

  /**
   * Helper method to create participants
   */
  private async createParticipants(splitId: string, participants: any[]): Promise<void> {
    const participantEntities = participants.map(p => 
      this.participantRepository.create({
        splitId,
        userId: p.userId,
        amountOwed: p.amountOwed || 0,
        walletAddress: p.walletAddress,
      })
    );

    await this.participantRepository.save(participantEntities);
  }

  /**
   * Helper method to create items
   */
  private async createItems(splitId: string, items: any[]): Promise<void> {
    const itemEntities = items.map(item => 
      this.itemRepository.create({
        splitId,
        name: item.name,
        quantity: item.quantity,
        unitPrice: item.unitPrice,
        totalPrice: item.totalPrice,
        category: item.category,
        assignedToIds: item.assignedToIds || [],
      })
    );

    await this.itemRepository.save(itemEntities);
  }
}
