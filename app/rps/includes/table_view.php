<?php
// includes/table_view.php - Main table view for descriptors with keywords column and occupancy %
?>

<table class="w-full">
    <thead class="bg-gray-50 border-b border-gray-200">
    <tr>
        <th class="px-4 py-3 text-left">
            <input type="checkbox" id="selectAll" onchange="toggleSelectAll()"
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
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Deals & Insurance</th>
        <th class="px-4 py-3 text-left text-sm font-medium text-gray-900">Actions</th>
    </tr>
    </thead>
    <tbody class="divide-y divide-gray-200" id="sortableTable" data-sortable="true">
    <?php foreach ($data['descriptors'] as $descriptor): ?>
        <tr class="hover:bg-gray-50 sortable-item transition-colors" data-id="<?= htmlspecialchars($descriptor['_id']) ?>">
            <td class="px-4 py-3">
                <input type="checkbox" class="descriptor-checkbox w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500"
                       value="<?= htmlspecialchars($descriptor['_id']) ?>"
                       onchange="updateSelection()">
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

            <!-- NEW: Keywords & Matches Column -->
            <td class="px-4 py-3">
                <div class="text-xs max-w-64">
                    <!-- Keywords -->
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

                    <!-- Matched Unit Types -->
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

            <td class="px-4 py-3">
                <div class="text-xs space-y-2">
                    <!-- Inventory Summary -->
                    <div class="flex items-center justify-between">
                        <span class="text-gray-600">Units:</span>
                        <span class="font-medium"><?= $descriptor['inventory']['total'] ?></span>
                    </div>

                    <!-- NEW: Occupancy Percentage (instead of availability) -->
                    <div class="flex items-center justify-between">
                        <span class="text-gray-600">Occupancy:</span>
                        <span class="font-medium <?= $descriptor['inventory']['occupancy'] > 80 ? 'text-red-600' :
                            ($descriptor['inventory']['occupancy'] > 60 ? 'text-orange-600' : 'text-green-600') ?>">
                            <?= $descriptor['inventory']['occupancy'] ?>%
                        </span>
                    </div>

                    <!-- Availability (smaller, secondary) -->
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

            <td class="px-4 py-3">
                <div class="text-xs text-gray-600 max-w-40">
                    <!-- Deals -->
                    <?php if (!empty($descriptor['deals']) && is_array($descriptor['deals'])): ?>
                        <div class="mb-2">
                            <div class="text-xs font-semibold text-blue-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-tags"></i>
                                Deals:
                            </div>
                            <?php foreach (array_slice($descriptor['deals'], 0, 1) as $dealId): ?>
                                <?php if (isset($data['lookups']['deals'][$dealId])): ?>
                                    <?php $deal = $data['lookups']['deals'][$dealId]; ?>
                                    <div class="bg-blue-50 border border-blue-200 rounded px-2 py-1 mb-1">
                                        <div class="font-medium text-blue-800"><?= htmlspecialchars($deal['title']) ?></div>
                                        <div class="text-xs <?= $deal['enable'] ? 'text-green-600' : 'text-gray-500' ?>">
                                            <?= $deal['enable'] ? '✓ Active' : '○ Inactive' ?>
                                        </div>
                                    </div>
                                <?php endif; ?>
                            <?php endforeach; ?>
                            <?php if (count($descriptor['deals']) > 1): ?>
                                <div class="text-xs text-blue-600">+ <?= count($descriptor['deals']) - 1 ?> more</div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>

                    <!-- Insurance -->
                    <?php if (!empty($descriptor['defaultInsuranceCoverage'])): ?>
                        <div class="mb-1">
                            <div class="text-xs font-semibold text-green-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-shield-alt"></i>
                                Insurance:
                            </div>
                            <?php if (isset($data['lookups']['insurance'][$descriptor['defaultInsuranceCoverage']])): ?>
                                <?php $coverage = $data['lookups']['insurance'][$descriptor['defaultInsuranceCoverage']]; ?>
                                <div class="bg-green-50 border border-green-200 rounded px-2 py-1">
                                    <div class="font-medium text-green-800 text-xs">
                                        <?= htmlspecialchars($coverage['sCoverageDesc']) ?>
                                    </div>
                                    <div class="text-xs text-green-600">
                                        $<?= number_format($coverage['dcCoverage']) ?>
                                    </div>
                                </div>
                            <?php else: ?>
                                <div class="text-xs text-gray-500">
                                    ID: <?= htmlspecialchars(substr($descriptor['defaultInsuranceCoverage'], 0, 8)) ?>...
                                </div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>

                    <!-- Upgrades -->
                    <?php if (!empty($descriptor['upgradesTo']) && is_array($descriptor['upgradesTo'])): ?>
                        <div class="mt-2">
                            <div class="text-xs font-semibold text-orange-700 mb-1 flex items-center gap-1">
                                <i class="fas fa-arrow-up"></i>
                                Upgrades:
                            </div>
                            <?php foreach (array_slice($descriptor['upgradesTo'], 0, 2) as $upgrade): ?>
                                <div class="bg-orange-50 border border-orange-200 rounded px-2 py-1 mb-1">
                                    <div class="flex items-center gap-1">
                                        <?php if (!empty($upgrade['upgradeIcon'])): ?>
                                            <i class="<?= htmlspecialchars($upgrade['upgradeIconPrefix'] ?? 'fas') ?> <?= htmlspecialchars($upgrade['upgradeIcon']) ?> text-orange-600"></i>
                                        <?php endif; ?>
                                        <span class="text-orange-800 font-medium text-xs">
                                    <?= htmlspecialchars(substr($upgrade['upgradeReason'] ?? 'Upgrade', 0, 15)) ?>
                                </span>
                                    </div>
                                    <?php if (isset($data['lookups']['unitTypes'][$upgrade['_id']])): ?>
                                        <div class="text-xs text-orange-600">
                                            <?= htmlspecialchars($data['lookups']['unitTypes'][$upgrade['_id']]['name'] ?? 'Unit') ?>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            <?php endforeach; ?>
                            <?php if (count($descriptor['upgradesTo']) > 2): ?>
                                <div class="text-xs text-orange-600">+ <?= count($descriptor['upgradesTo']) - 2 ?> more</div>
                            <?php endif; ?>
                        </div>
                    <?php endif; ?>
                </div>
            </td>

            <td class="px-4 py-3">
                <div class="flex items-center gap-2">
                    <a href="?edit=<?= htmlspecialchars($descriptor['_id']) ?>"
                       class="text-blue-600 hover:text-blue-800 p-1 rounded hover:bg-blue-50 transition-colors"
                       title="Edit descriptor">
                        <i class="fas fa-edit"></i>
                    </a>

                    <button onclick="deleteDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>', '<?= htmlspecialchars($descriptor['name']) ?>')"
                            class="text-red-600 hover:text-red-800 p-1 rounded hover:bg-red-50 transition-colors"
                            title="Delete descriptor">
                        <i class="fas fa-trash"></i>
                    </button>

                    <button onclick="duplicateDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>')"
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
            <td colspan="9" class="text-center py-12 text-gray-500">
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