<?php
// includes/table_view.php - Main table view with separate Deals, Insurance, and Upsells columns
?>

<table class="w-full">
    <thead class="bg-gray-50 border-b border-gray-200">
    <tr>
        <th class="px-4 py-3 text-left">
            <input type="checkbox" id="selectAll" data-action="toggleSelectAll"
                   class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500">
        </th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">
            <div class="flex items-center gap-2">
                <i class="fas fa-grip-vertical text-gray-400"></i>
                Order
            </div>
        </th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Name</th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Description</th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Keywords & Matches</th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Quick Controls</th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Inventory</th>

        <!-- NEW: Separate columns for Deals, Insurance, and Upsells -->
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">
            <div class="flex items-center gap-1">
                <i class="fas fa-tags text-purple-600"></i>
                Deals
            </div>
        </th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">
            <div class="flex items-center gap-1">
                <i class="fas fa-shield-alt text-green-600"></i>
                Insurance
            </div>
        </th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">
            <div class="flex items-center gap-1">
                <i class="fas fa-arrow-up text-indigo-600"></i>
                Upsells
            </div>
        </th>

        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Actions</th>
    </tr>
    </thead>
    <tbody class="divide-y divide-gray-200" id="sortableTable" data-sortable="true">
    <?php foreach ($data['descriptors'] as $descriptor): ?>
        <tr class="hover:bg-gray-50 sortable-item transition-colors" data-id="<?= htmlspecialchars($descriptor['_id']) ?>">
            <td class="px-4 py-3">
                <input type="checkbox" class="descriptor-checkbox w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                       value="<?= htmlspecialchars($descriptor['_id']) ?>"
                       data-action="updateSelection">
            </td>

            <td class="px-4 py-3 text-sm text-gray-900">
                <div class="flex items-center gap-2">
                    <i class="fas fa-grip-vertical text-gray-400 drag-handle cursor-move hover:text-gray-600"></i>
                    <span class="bg-gray-100 px-2 py-1 rounded text-xs font-mono border">
                        <?= htmlspecialchars($descriptor['ordinalPosition'] ?? 0) ?>
                    </span>
                </div>
            </td>

            <td class="px-4 py-3">
                <div class="flex items-center gap-3">
                    <?php if (!empty($descriptor['picture'])): ?>
                        <img src="<?= htmlspecialchars($descriptor['picture']) ?>"
                             alt="<?= htmlspecialchars($descriptor['name']) ?>"
                             class="w-12 h-12 object-cover rounded-lg border border-gray-200 shadow-sm">
                    <?php endif; ?>
                    <div>
                        <div class="text-sm font-medium text-gray-900">
                            <?= htmlspecialchars($descriptor['name']) ?>
                        </div>
                        <?php if (!empty($descriptor['specialText'])): ?>
                            <div class="text-xs text-gray-500 truncate max-w-40">
                                <?= htmlspecialchars($descriptor['specialText']) ?>
                            </div>
                        <?php endif; ?>
                        <div class="text-xs text-gray-400 mt-1">
                            ID: <?= htmlspecialchars(substr($descriptor['_id'], -8)) ?>
                        </div>
                    </div>
                </div>
            </td>

            <td class="px-4 py-3">
                <div class="text-sm text-gray-900 max-w-64">
                    <?= htmlspecialchars($descriptor['description'] ?? '') ?>
                    <?php if (!empty($descriptor['descriptions']) && is_array($descriptor['descriptions'])): ?>
                        <div class="text-xs text-gray-500 mt-1">
                            <?php foreach (array_slice($descriptor['descriptions'], 0, 2) as $desc): ?>
                                <div>• <?= htmlspecialchars($desc) ?></div>
                            <?php endforeach; ?>
                            <?php if (count($descriptor['descriptions']) > 2): ?>
                                <div>+ <?= count($descriptor['descriptions']) - 2 ?> more...</div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <!-- Keywords & Matches Column -->
            <td class="px-4 py-3">
                <div class="text-xs max-w-64">
                    <?php
                    $keywords = $descriptor['inventory']['keywords'] ?? [];
                    $matchedTypes = $descriptor['inventory']['matched_unit_types'] ?? [];
                    ?>

                    <?php if (!empty($keywords)): ?>
                        <div class="mb-2">
                            <div class="text-xs font-semibold text-blue-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-key"></i>
                                Keywords (<?= count($keywords) ?>):
                            </div>
                            <?php foreach (array_slice($keywords, 0, 2) as $keyword): ?>
                                <div class="bg-blue-50 border border-blue-200 rounded px-2 py-1 mb-1">
                                    <code class="text-xs text-blue-800"><?= htmlspecialchars($keyword) ?></code>
                                </div>
                            <?php endforeach; ?>
                            <?php if (count($keywords) > 2): ?>
                                <div class="text-xs text-blue-600">+ <?= count($keywords) - 2 ?> more</div>
                            <?php endif; ?>
                        </div>
                    <?php else: ?>
                        <div class="mb-2">
                            <div class="text-xs font-semibold text-red-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-exclamation-triangle"></i>
                                No Keywords
                            </div>
                            <div class="bg-red-50 border border-red-200 rounded px-2 py-1 text-xs text-red-600">
                                No matching criteria found
                            </div>
                        </div>
                    <?php endif; ?>

                    <?php if (!empty($matchedTypes)): ?>
                        <div>
                            <div class="text-xs font-semibold text-green-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-check-circle"></i>
                                Matches (<?= count($matchedTypes) ?>):
                            </div>
                            <?php foreach (array_slice($matchedTypes, 0, 2) as $match): ?>
                                <div class="bg-green-50 border border-green-200 rounded px-2 py-1 mb-1">
                                    <div class="text-xs text-green-800 font-mono truncate">
                                        <?= htmlspecialchars($match['type_name']) ?>
                                    </div>
                                    <div class="text-xs text-green-600">
                                        <?= $match['total'] ?> units, <?= $match['vacant'] ?> vacant
                                    </div>
                                </div>
                            <?php endforeach; ?>
                            <?php if (count($matchedTypes) > 2): ?>
                                <div class="text-xs text-green-600">+ <?= count($matchedTypes) - 2 ?> more</div>
                            <?php endif; ?>
                        </div>
                    <?php elseif (!empty($keywords)): ?>
                        <div>
                            <div class="text-xs font-semibold text-orange-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-exclamation-circle"></i>
                                No Matches
                            </div>
                            <div class="bg-orange-50 border border-orange-200 rounded px-2 py-1 text-xs text-orange-600">
                                Keywords don't match any unit types
                            </div>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <!-- Quick Controls Column -->
            <td class="px-4 py-3">
                <div class="flex flex-col gap-2">
                    <!-- Enabled Toggle -->
                    <label class="flex items-center gap-2 cursor-pointer">
                        <input type="checkbox"
                               class="status-toggle sr-only"
                            <?= $descriptor['enabled'] ? 'checked' : '' ?>
                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'enabled', this.checked)">
                        <div class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors
                                    <?= $descriptor['enabled'] ? 'bg-green-600' : 'bg-gray-200' ?>">
                            <span class="inline-block h-3 w-3 transform rounded-full bg-white transition-transform
                                         <?= $descriptor['enabled'] ? 'translate-x-5' : 'translate-x-1' ?>"></span>
                        </div>
                        <span class="text-xs <?= $descriptor['enabled'] ? 'text-green-600 font-medium' : 'text-gray-500' ?>">
                            <?= $descriptor['enabled'] ? 'Enabled' : 'Disabled' ?>
                        </span>
                    </label>

                    <!-- Visibility Toggle -->
                    <label class="flex items-center gap-2 cursor-pointer">
                        <input type="checkbox"
                               class="status-toggle sr-only"
                            <?= !$descriptor['hidden'] ? 'checked' : '' ?>
                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'hidden', !this.checked)">
                        <div class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors
                                    <?= !$descriptor['hidden'] ? 'bg-blue-600' : 'bg-gray-200' ?>">
                            <span class="inline-block h-3 w-3 transform rounded-full bg-white transition-transform
                                         <?= !$descriptor['hidden'] ? 'translate-x-5' : 'translate-x-1' ?>"></span>
                        </div>
                        <span class="text-xs <?= !$descriptor['hidden'] ? 'text-blue-600 font-medium' : 'text-gray-500' ?>">
                            <?= !$descriptor['hidden'] ? 'Visible' : 'Hidden' ?>
                        </span>
                    </label>

                    <!-- Carousel Toggle -->
                    <label class="flex items-center gap-2 cursor-pointer">
                        <input type="checkbox"
                               class="status-toggle sr-only"
                            <?= $descriptor['useForCarousel'] ? 'checked' : '' ?>
                               onchange="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'useForCarousel', this.checked)">
                        <div class="relative inline-flex h-5 w-9 items-center rounded-full transition-colors
                                    <?= $descriptor['useForCarousel'] ? 'bg-purple-600' : 'bg-gray-200' ?>">
                            <span class="inline-block h-3 w-3 transform rounded-full bg-white transition-transform
                                         <?= $descriptor['useForCarousel'] ? 'translate-x-5' : 'translate-x-1' ?>"></span>
                        </div>
                        <span class="text-xs <?= $descriptor['useForCarousel'] ? 'text-purple-600 font-medium' : 'text-gray-500' ?>">
                            Carousel
                        </span>
                    </label>
                </div>
            </td>

            <!-- Inventory Column -->
            <td class="px-4 py-3">
                <div class="text-xs space-y-2">
                    <div class="flex items-center justify-between">
                        <span class="text-gray-600">Units:</span>
                        <span class="font-medium"><?= $descriptor['inventory']['total'] ?></span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-gray-600">Occupancy:</span>
                        <span class="font-medium <?= $descriptor['inventory']['occupancy'] > 80 ? 'text-red-600' :
                            ($descriptor['inventory']['occupancy'] > 60 ? 'text-orange-600' : 'text-green-600') ?>">
                            <?= $descriptor['inventory']['occupancy'] ?>%
                        </span>
                    </div>

                    <div class="flex items-center justify-between">
                        <span class="text-gray-500 text-xs">Available:</span>
                        <span class="text-xs text-gray-600"><?= $descriptor['inventory']['availability'] ?>%</span>
                    </div>

                    <!-- Visual Inventory Bar -->
                    <div class="inventory-bar bg-gray-200 rounded">
                        <?php if ($descriptor['inventory']['total'] > 0): ?>
                            <?php
                            $occupiedPercent = ($descriptor['inventory']['occupied'] / $descriptor['inventory']['total']) * 100;
                            $reservedPercent = ($descriptor['inventory']['reserved'] / $descriptor['inventory']['total']) * 100;
                            $vacantPercent = ($descriptor['inventory']['vacant'] / $descriptor['inventory']['total']) * 100;
                            ?>
                            <div class="flex h-full rounded overflow-hidden">
                                <?php if ($occupiedPercent > 0): ?>
                                    <div class="inventory-segment bg-red-500" style="width: <?= $occupiedPercent ?>%"
                                         title="Occupied: <?= $descriptor['inventory']['occupied'] ?> (<?= round($occupiedPercent, 1) ?>%)"></div>
                                <?php endif; ?>
                                <?php if ($reservedPercent > 0): ?>
                                    <div class="inventory-segment bg-yellow-500" style="width: <?= $reservedPercent ?>%"
                                         title="Reserved: <?= $descriptor['inventory']['reserved'] ?> (<?= round($reservedPercent, 1) ?>%)"></div>
                                <?php endif; ?>
                                <?php if ($vacantPercent > 0): ?>
                                    <div class="inventory-segment bg-green-500" style="width: <?= $vacantPercent ?>%"
                                         title="Vacant: <?= $descriptor['inventory']['vacant'] ?> (<?= round($vacantPercent, 1) ?>%)"></div>
                                <?php endif; ?>
                            </div>
                        <?php endif; ?>
                    </div>

                    <!-- Detailed Breakdown -->
                    <div class="grid grid-cols-3 gap-1 text-xs">
                        <div class="text-center">
                            <div class="w-2 h-2 bg-red-500 rounded-full mx-auto mb-1"></div>
                            <span><?= $descriptor['inventory']['occupied'] ?></span>
                            <div class="text-xs text-gray-500">Occ</div>
                        </div>
                        <div class="text-center">
                            <div class="w-2 h-2 bg-yellow-500 rounded-full mx-auto mb-1"></div>
                            <span><?= $descriptor['inventory']['reserved'] ?></span>
                            <div class="text-xs text-gray-500">Res</div>
                        </div>
                        <div class="text-center">
                            <div class="w-2 h-2 bg-green-500 rounded-full mx-auto mb-1"></div>
                            <span><?= $descriptor['inventory']['vacant'] ?></span>
                            <div class="text-xs text-gray-500">Vac</div>
                        </div>
                    </div>
                </div>
            </td>

            <!-- NEW: Deals Column -->
            <td class="px-4 py-3">
                <div class="max-w-48">
                    <?php if (!empty($descriptor['deals']) && is_array($descriptor['deals'])): ?>
                        <div class="space-y-2">
                            <?php foreach (array_slice($descriptor['deals'], 0, 3) as $dealId): ?>
                                <?php if (isset($data['lookups']['deals'][$dealId])): ?>
                                    <?php $deal = $data['lookups']['deals'][$dealId]; ?>
                                    <div class="bg-purple-50 border border-purple-200 rounded-lg p-2">
                                        <div class="text-xs font-medium text-purple-800 truncate">
                                            <?= htmlspecialchars($deal['title']) ?>
                                        </div>
                                        <div class="flex items-center justify-between mt-1">
                                            <span class="text-xs <?= $deal['enable'] ? 'text-green-600' : 'text-gray-500' ?>">
                                                <?= $deal['enable'] ? '✓ Active' : '○ Inactive' ?>
                                            </span>
                                            <?php if (!empty($deal['discount'])): ?>
                                                <span class="text-xs text-purple-600 font-medium">
                                                    <?= htmlspecialchars($deal['discount']) ?>
                                                </span>
                                            <?php endif; ?>
                                        </div>
                                    </div>
                                <?php else: ?>
                                    <div class="bg-gray-50 border border-gray-200 rounded-lg p-2">
                                        <div class="text-xs text-gray-500">
                                            Unknown Deal
                                        </div>
                                        <div class="text-xs text-gray-400">
                                            ID: <?= htmlspecialchars(substr($dealId, -6)) ?>
                                        </div>
                                    </div>
                                <?php endif; ?>
                            <?php endforeach; ?>

                            <?php if (count($descriptor['deals']) > 3): ?>
                                <div class="text-xs text-purple-600 text-center p-1">
                                    + <?= count($descriptor['deals']) - 3 ?> more deals
                                </div>
                            <?php endif; ?>
                        </div>
                    <?php else: ?>
                        <div class="text-center py-3">
                            <div class="text-gray-400 mb-1">
                                <i class="fas fa-tags text-lg"></i>
                            </div>
                            <div class="text-xs text-gray-500">No deals</div>
                            <button data-action="quickAddDeal" data-descriptor-id='<?= htmlspecialchars($descriptor['_id']) ?>')"
                            class="text-xs text-purple-600 hover:text-purple-800 mt-1">
                            + Add Deal
                            </button>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <!-- NEW: Insurance Column -->
            <td class="px-4 py-3">
                <div class="max-w-40">
                    <?php if (!empty($descriptor['defaultInsuranceCoverage'])): ?>
                        <?php if (isset($data['lookups']['insurance'][$descriptor['defaultInsuranceCoverage']])): ?>
                            <?php $coverage = $data['lookups']['insurance'][$descriptor['defaultInsuranceCoverage']]; ?>
                            <div class="bg-green-50 border border-green-200 rounded-lg p-2">
                                <div class="text-xs font-medium text-green-800 truncate">
                                    <?= htmlspecialchars($coverage['sCoverageDesc']) ?>
                                </div>
                                <div class="text-xs text-green-600 mt-1">
                                    $<?= number_format($coverage['dcCoverage']) ?> coverage
                                </div>
                                <div class="text-xs text-gray-500 mt-1">
                                    <?= !empty($coverage['monthlyRate']) ? '$' . number_format($coverage['monthlyRate'], 2) . '/month' : 'Rate varies' ?>
                                </div>
                            </div>
                        <?php else: ?>
                            <div class="bg-gray-50 border border-gray-200 rounded-lg p-2">
                                <div class="text-xs text-gray-600">
                                    Unknown Insurance
                                </div>
                                <div class="text-xs text-gray-400">
                                    ID: <?= htmlspecialchars(substr($descriptor['defaultInsuranceCoverage'], -6)) ?>
                                </div>
                            </div>
                        <?php endif; ?>
                    <?php else: ?>
                        <div class="text-center py-3">
                            <div class="text-gray-400 mb-1">
                                <i class="fas fa-shield-alt text-lg"></i>
                            </div>
                            <div class="text-xs text-gray-500">No insurance</div>
                            <button data-action="quickAddInsurance" data-descriptor-id='<?= htmlspecialchars($descriptor['_id']) ?>')"
                            class="text-xs text-green-600 hover:text-green-800 mt-1">
                            + Add Insurance
                            </button>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <!-- NEW: Upsells Column -->
            <td class="px-4 py-3">
                <div class="max-w-48">
                    <?php if (!empty($descriptor['upgradesTo']) && is_array($descriptor['upgradesTo'])): ?>
                        <div class="space-y-2">
                            <?php foreach (array_slice($descriptor['upgradesTo'], 0, 3) as $upgrade): ?>
                                <?php
                                // Find the target descriptor
                                $targetDescriptor = null;
                                foreach ($data['descriptors'] as $desc) {
                                    if ($desc['_id'] === $upgrade['_id']) {
                                        $targetDescriptor = $desc;
                                        break;
                                    }
                                }
                                ?>
                                <div class="bg-indigo-50 border border-indigo-200 rounded-lg p-2">
                                    <div class="flex items-start gap-1">
                                        <?php if (!empty($upgrade['upgradeIcon'])): ?>
                                            <i class="<?= htmlspecialchars($upgrade['upgradeIconPrefix'] ?? 'fas') ?> <?= htmlspecialchars($upgrade['upgradeIcon']) ?> text-indigo-600 text-xs mt-0.5"></i>
                                        <?php endif; ?>
                                        <div class="flex-1 min-w-0">
                                            <div class="text-xs font-medium text-indigo-800 truncate">
                                                <?= htmlspecialchars($upgrade['upgradeReason'] ?? 'Upgrade Option') ?>
                                            </div>
                                            <?php if ($targetDescriptor): ?>
                                                <div class="text-xs text-indigo-600 truncate mt-1">
                                                    → <?= htmlspecialchars($targetDescriptor['name']) ?>
                                                </div>
                                                <div class="text-xs text-gray-500 mt-1">
                                                    <?= $targetDescriptor['inventory']['availability'] ?>% available
                                                </div>
                                            <?php else: ?>
                                                <div class="text-xs text-gray-500 mt-1">
                                                    Target: <?= htmlspecialchars(substr($upgrade['_id'], -6)) ?>
                                                </div>
                                            <?php endif; ?>
                                        </div>
                                    </div>
                                </div>
                            <?php endforeach; ?>

                            <?php if (count($descriptor['upgradesTo']) > 3): ?>
                                <div class="text-xs text-indigo-600 text-center p-1">
                                    + <?= count($descriptor['upgradesTo']) - 3 ?> more upsells
                                </div>
                            <?php endif; ?>
                        </div>
                    <?php else: ?>
                        <div class="text-center py-3">
                            <div class="text-gray-400 mb-1">
                                <i class="fas fa-arrow-up text-lg"></i>
                            </div>
                            <div class="text-xs text-gray-500">No upsells</div>
                            <button data-action="quickAddUpsell" data-descriptor-id='<?= htmlspecialchars($descriptor['_id']) ?>')"
                            class="text-xs text-indigo-600 hover:text-indigo-800 mt-1">
                            + Add Upsell
                            </button>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <!-- Actions Column -->
            <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                    <a href="?edit=<?= htmlspecialchars($descriptor['_id']) ?>"
                       class="text-blue-600 hover:text-blue-800 p-1 rounded hover:bg-blue-50 transition-colors"
                       title="Edit descriptor">
                        <i class="fas fa-edit"></i>
                    </a>

                    <button onclick="deleteDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>', '<?= htmlspecialchars(addslashes($descriptor['name'])) ?>')"
                            class="text-red-600 hover:text-red-800 p-1 rounded hover:bg-red-50 transition-colors"
                            title="Delete descriptor">
                        <i class="fas fa-trash"></i>
                    </button>

                    <button onclick="duplicateDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>', '<?= htmlspecialchars(addslashes($descriptor['name'])) ?>')"
                            class="text-green-600 hover:text-green-800 p-1 rounded hover:bg-green-50 transition-colors"
                            title="Duplicate descriptor">
                        <i class="fas fa-copy"></i>
                    </button>
                </div>
            </td>
        </tr>
    <?php endforeach; ?>

    <?php if (empty($data['descriptors'])): ?>
        <tr>
            <td colspan="11" class="text-center py-12 text-gray-500">
                <div class="flex flex-col items-center">
                    <i class="fas fa-inbox text-4xl text-gray-300 mb-4"></i>
                    <p class="text-lg font-medium">No descriptors found</p>
                    <?php if ($searchTerm): ?>
                        <p class="text-sm">Try adjusting your search criteria</p>
                        <a href="?" class="text-blue-600 hover:text-blue-800 text-sm mt-2">Clear search</a>
                    <?php else: ?>
                        <p class="text-sm">Get started by creating your first descriptor</p>
                        <a href="?create=1" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded mt-3 inline-flex items-center gap-2">
                            <i class="fas fa-plus"></i>
                            Create Descriptor
                        </a>
                    <?php endif; ?>
                </div>
            </td>
        </tr>
    <?php endif; ?>
    </tbody>
</table>

<!-- Quick Add Modals and JavaScript -->
<script>
    // Quick add functions for deals, insurance, and upsells
    function quickAddDeal(descriptorId) {
        // Implementation for quick deal addition
        const deals = Object.values(dealsLookup || {});
        if (deals.length === 0) {
            RapidStorApp.showToast('No deals available to add', 'warning');
            return;
        }

        // Show a simple selection modal
        const dealOptions = deals.map(deal =>
            `<option value="${deal._id}">${deal.title} ${deal.enable ? '(Active)' : '(Inactive)'}</option>`
        ).join('');

        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-gray-600 bg-opacity-50 z-50 flex items-center justify-center';
        modal.innerHTML = `
        <div class="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 class="text-lg font-semibold mb-4">Quick Add Deal</h3>
            <select id="quickDealSelect" class="w-full border border-gray-300 rounded-md px-3 py-2 mb-4">
                <option value="">Select a deal...</option>
                ${dealOptions}
            </select>
            <div class="flex gap-3 justify-end">
                <button onclick="this.closest('.fixed').remove()" class="px-4 py-2 text-gray-600 border border-gray-300 rounded-md">Cancel</button>
                <button onclick="executeQuickAddDeal('${descriptorId}')" class="px-4 py-2 bg-purple-600 text-white rounded-md">Add Deal</button>
            </div>
        </div>
    `;
        document.body.appendChild(modal);
    }

    function executeQuickAddDeal(descriptorId) {
        const select = document.getElementById('quickDealSelect');
        const dealId = select.value;

        if (!dealId) {
            RapidStorApp.showToast('Please select a deal', 'warning');
            return;
        }

        // Use batch apply with single descriptor
        const formData = new FormData();
        formData.append('action', 'batch_apply');
        formData.append('descriptor_ids', JSON.stringify([descriptorId]));
        formData.append('update_data', JSON.stringify({
            field: 'deals',
            value: [dealId],
            mode: 'add'
        }));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    RapidStorApp.showToast('Deal added successfully', 'success');
                    document.querySelector('.fixed').remove();
                    setTimeout(() => location.reload(), 1000);
                } else {
                    RapidStorApp.showToast('Failed to add deal: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                RapidStorApp.showToast('Network error: ' + error.message, 'error');
            });
    }

    function quickAddInsurance(descriptorId) {
        const insuranceOptions = Object.values(insuranceLookup || {});
        if (insuranceOptions.length === 0) {
            RapidStorApp.showToast('No insurance options available', 'warning');
            return;
        }

        const options = insuranceOptions.map(insurance =>
            `<option value="${insurance._id}">${insurance.sCoverageDesc || 'Unknown Coverage'} - ${new Intl.NumberFormat().format(insurance.dcCoverage || 0)}</option>`
        ).join('');

        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-gray-600 bg-opacity-50 z-50 flex items-center justify-center';
        modal.innerHTML = `
        <div class="bg-white rounded-lg p-6 max-w-md w-full mx-4">
            <h3 class="text-lg font-semibold mb-4">Quick Add Insurance</h3>
            <select id="quickInsuranceSelect" class="w-full border border-gray-300 rounded-md px-3 py-2 mb-4">
                <option value="">Select insurance coverage...</option>
                ${options}
            </select>
            <div class="flex gap-3 justify-end">
                <button onclick="this.closest('.fixed').remove()" class="px-4 py-2 text-gray-600 border border-gray-300 rounded-md">Cancel</button>
                <button onclick="executeQuickAddInsurance('${descriptorId}')" class="px-4 py-2 bg-green-600 text-white rounded-md">Add Insurance</button>
            </div>
        </div>
    `;
        document.body.appendChild(modal);
    }

    function executeQuickAddInsurance(descriptorId) {
        const select = document.getElementById('quickInsuranceSelect');
        const insuranceId = select.value;

        if (!insuranceId) {
            RapidStorApp.showToast('Please select an insurance option', 'warning');
            return;
        }

        const formData = new FormData();
        formData.append('action', 'batch_apply');
        formData.append('descriptor_ids', JSON.stringify([descriptorId]));
        formData.append('update_data', JSON.stringify({
            field: 'defaultInsuranceCoverage',
            value: insuranceId
        }));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    RapidStorApp.showToast('Insurance added successfully', 'success');
                    document.querySelector('.fixed').remove();
                    setTimeout(() => location.reload(), 1000);
                } else {
                    RapidStorApp.showToast('Failed to add insurance: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                RapidStorApp.showToast('Network error: ' + error.message, 'error');
            });
    }

    function quickAddUpsell(descriptorId) {
        // Find potential upsell targets (other descriptors with good availability)
        const potentialTargets = descriptors.filter(desc =>
            desc._id !== descriptorId &&
            desc.inventory &&
            desc.inventory.availability > 20
        ).sort((a, b) => b.inventory.availability - a.inventory.availability);

        if (potentialTargets.length === 0) {
            RapidStorApp.showToast('No suitable upsell targets available (need >20% availability)', 'warning');
            return;
        }

        const targetOptions = potentialTargets.slice(0, 10).map(desc =>
            `<option value="${desc._id}">${desc.name} (${desc.inventory.availability}% available)</option>`
        ).join('');

        const modal = document.createElement('div');
        modal.className = 'fixed inset-0 bg-gray-600 bg-opacity-50 z-50 flex items-center justify-center';
        modal.innerHTML = `
        <div class="bg-white rounded-lg p-6 max-w-lg w-full mx-4">
            <h3 class="text-lg font-semibold mb-4">Quick Add Upsell</h3>
            <div class="space-y-4">
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">Target Descriptor</label>
                    <select id="quickUpsellTarget" class="w-full border border-gray-300 rounded-md px-3 py-2">
                        <option value="">Select upsell target...</option>
                        ${targetOptions}
                    </select>
                </div>
                <div>
                    <label class="block text-sm font-medium text-gray-700 mb-1">Upsell Reason</label>
                    <input type="text" id="quickUpsellReason" placeholder="e.g., Larger Option +10sqft"
                           class="w-full border border-gray-300 rounded-md px-3 py-2">
                </div>
            </div>
            <div class="flex gap-3 justify-end mt-6">
                <button onclick="this.closest('.fixed').remove()" class="px-4 py-2 text-gray-600 border border-gray-300 rounded-md">Cancel</button>
                <button onclick="executeQuickAddUpsell('${descriptorId}')" class="px-4 py-2 bg-indigo-600 text-white rounded-md">Add Upsell</button>
            </div>
        </div>
    `;
        document.body.appendChild(modal);

        // Auto-generate reason when target is selected
        document.getElementById('quickUpsellTarget').addEventListener('change', function() {
            if (this.value) {
                const targetDesc = descriptors.find(d => d._id === this.value);
                const currentDesc = descriptors.find(d => d._id === descriptorId);
                if (targetDesc && currentDesc) {
                    const reason = generateUpsellReason(currentDesc.name, targetDesc.name);
                    document.getElementById('quickUpsellReason').value = reason;
                }
            }
        });
    }

    function executeQuickAddUpsell(descriptorId) {
        const targetSelect = document.getElementById('quickUpsellTarget');
        const reasonInput = document.getElementById('quickUpsellReason');

        const targetId = targetSelect.value;
        const reason = reasonInput.value.trim();

        if (!targetId) {
            RapidStorApp.showToast('Please select an upsell target', 'warning');
            return;
        }

        if (!reason) {
            RapidStorApp.showToast('Please enter an upsell reason', 'warning');
            return;
        }

        // First, get the current descriptor to add the upsell
        fetch(window.location.href, {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: `action=get_descriptor&descriptor_id=${descriptorId}`
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    const descriptor = data.data;
                    const currentUpsells = descriptor.upgradesTo || [];

                    // Add new upsell
                    const newUpsell = {
                        _id: targetId,
                        upgradeReason: reason,
                        upgradeIcon: 'fa-warehouse',
                        upgradeIconPrefix: 'fa-light'
                    };

                    currentUpsells.push(newUpsell);
                    descriptor.upgradesTo = currentUpsells;

                    // Save the descriptor
                    const formData = new FormData();
                    formData.append('action', 'save_descriptor');
                    Object.keys(descriptor).forEach(key => {
                        if (typeof descriptor[key] === 'object') {
                            formData.append(key, JSON.stringify(descriptor[key]));
                        } else {
                            formData.append(key, descriptor[key]);
                        }
                    });

                    return fetch(window.location.href, {
                        method: 'POST',
                        body: formData
                    });
                } else {
                    throw new Error('Failed to get descriptor: ' + (data.error || 'Unknown error'));
                }
            })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    RapidStorApp.showToast('Upsell added successfully', 'success');
                    document.querySelector('.fixed').remove();
                    setTimeout(() => location.reload(), 1000);
                } else {
                    RapidStorApp.showToast('Failed to add upsell: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                RapidStorApp.showToast('Error: ' + error.message, 'error');
            });
    }

    function generateUpsellReason(fromName, toName) {
        // Extract size patterns
        const fromSize = fromName.match(/(\d+(?:-\d+)?)\s*sq\s*ft/i);
        const toSize = toName.match(/(\d+(?:-\d+)?)\s*sq\s*ft/i);

        if (fromSize && toSize) {
            const fromNum = parseInt(fromSize[1].split('-').pop());
            const toNum = parseInt(toSize[1].split('-').pop());
            if (toNum > fromNum) {
                return `Upgrade to ${toSize[1]}sqft (+${toNum - fromNum}sqft)`;
            }
            return `Upgrade to ${toSize[1]}sqft`;
        }

        // Check for premium upgrade
        if (fromName.toLowerCase().includes('regular') && toName.toLowerCase().includes('premium')) {
            return 'Premium upgrade';
        }

        // Check for climate control
        if (!fromName.toLowerCase().includes('climate') && toName.toLowerCase().includes('climate')) {
            return 'Climate controlled upgrade';
        }

        // Generic fallback
        return `Upgrade to ${toName.split(' ').slice(0, 2).join(' ')}`;
    }