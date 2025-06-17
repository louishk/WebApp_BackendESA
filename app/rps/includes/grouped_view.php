<?php
// includes/grouped_view.php - Grouped view for descriptors organized by size
?>

<div class="p-6 space-y-6">
    <?php foreach ($data['groupedDescriptors'] as $groupName => $groupDescriptors): ?>
        <div class="border border-gray-200 rounded-lg overflow-hidden shadow-sm">
            <div class="group-header px-6 py-4 text-white">
                <div class="flex items-center justify-between">
                    <h3 class="text-lg font-semibold flex items-center gap-3">
                        <i class="fas fa-layer-group"></i>
                        <?= htmlspecialchars($groupName) ?>
                        <span class="bg-white bg-opacity-20 px-3 py-1 rounded-full text-sm font-medium">
                        <?= count($groupDescriptors) ?> descriptor<?= count($groupDescriptors) !== 1 ? 's' : '' ?>
                    </span>
                    </h3>
                    <div class="flex items-center gap-4">
                        <?php
                        // Calculate group inventory stats
                        $groupInventory = [
                            'total' => 0,
                            'vacant' => 0,
                            'occupied' => 0,
                            'reserved' => 0,
                            'enabled' => 0,
                            'visible' => 0
                        ];
                        foreach ($groupDescriptors as $desc) {
                            $groupInventory['total'] += $desc['inventory']['total'];
                            $groupInventory['vacant'] += $desc['inventory']['vacant'];
                            $groupInventory['occupied'] += $desc['inventory']['occupied'];
                            $groupInventory['reserved'] += $desc['inventory']['reserved'];
                            $groupInventory['enabled'] += $desc['enabled'] ? 1 : 0;
                            $groupInventory['visible'] += !($desc['hidden'] ?? false) ? 1 : 0;
                        }
                        $groupAvailability = $groupInventory['total'] > 0 ?
                            round(($groupInventory['vacant'] / $groupInventory['total']) * 100, 1) : 0;
                        ?>

                        <!-- Group Stats -->
                        <div class="flex items-center gap-4 text-sm">
                            <div class="bg-white bg-opacity-20 px-3 py-1 rounded-full">
                                <i class="fas fa-chart-bar mr-1"></i>
                                <?= $groupAvailability ?>% available
                            </div>
                            <div class="bg-white bg-opacity-20 px-3 py-1 rounded-full">
                                <i class="fas fa-cubes mr-1"></i>
                                <?= $groupInventory['total'] ?> units
                            </div>
                            <div class="bg-white bg-opacity-20 px-3 py-1 rounded-full">
                                <i class="fas fa-toggle-on mr-1"></i>
                                <?= $groupInventory['enabled'] ?>/<?= count($groupDescriptors) ?> enabled
                            </div>
                        </div>

                        <!-- Group Actions -->
                        <div class="flex items-center gap-2">
                            <button onclick="selectGroup('<?= htmlspecialchars($groupName) ?>')"
                                    class="text-white hover:bg-white hover:bg-opacity-20 p-2 rounded transition-colors"
                                    title="Select all in group">
                                <i class="fas fa-check-square"></i>
                            </button>
                            <button onclick="toggleGroup('<?= htmlspecialchars($groupName) ?>')"
                                    class="text-white hover:bg-white hover:bg-opacity-20 p-2 rounded transition-colors"
                                    title="Expand/collapse group">
                                <i class="fas fa-chevron-down group-toggle"></i>
                            </button>
                        </div>
                    </div>
                </div>

                <!-- Group Inventory Bar -->
                <div class="mt-3">
                    <div class="flex items-center justify-between text-sm mb-1">
                        <span>Group Inventory</span>
                        <span><?= $groupInventory['vacant'] ?> of <?= $groupInventory['total'] ?> available</span>
                    </div>
                    <div class="inventory-bar bg-white bg-opacity-20 rounded">
                        <?php if ($groupInventory['total'] > 0): ?>
                            <?php
                            $occupiedPercent = ($groupInventory['occupied'] / $groupInventory['total']) * 100;
                            $reservedPercent = ($groupInventory['reserved'] / $groupInventory['total']) * 100;
                            $vacantPercent = ($groupInventory['vacant'] / $groupInventory['total']) * 100;
                            ?>
                            <div class="flex h-full rounded overflow-hidden">
                                <?php if ($occupiedPercent > 0): ?>
                                    <div class="bg-red-400" style="width: <?= $occupiedPercent ?>%" title="Occupied: <?= $groupInventory['occupied'] ?>"></div>
                                <?php endif; ?>
                                <?php if ($reservedPercent > 0): ?>
                                    <div class="bg-yellow-400" style="width: <?= $reservedPercent ?>%" title="Reserved: <?= $groupInventory['reserved'] ?>"></div>
                                <?php endif; ?>
                                <?php if ($vacantPercent > 0): ?>
                                    <div class="bg-green-400" style="width: <?= $vacantPercent ?>%" title="Vacant: <?= $groupInventory['vacant'] ?>"></div>
                                <?php endif; ?>
                            </div>
                        <?php endif; ?>
                    </div>
                </div>
            </div>

            <div class="group-content" id="group-<?= htmlspecialchars($groupName) ?>">
                <div class="overflow-x-auto">
                    <table class="w-full">
                        <thead class="bg-gray-50 border-b border-gray-200">
                        <tr>
                            <th class="px-4 py-2 text-left">
                                <input type="checkbox" class="group-select-all w-4 h-4 text-blue-600"
                                       data-group="<?= htmlspecialchars($groupName) ?>"
                                       onchange="toggleGroupSelection('<?= htmlspecialchars($groupName) ?>')">
                            </th>
                            <th class="px-4 py-2 text-left text-sm font-medium text-gray-700">Name</th>
                            <th class="px-4 py-2 text-left text-sm font-medium text-gray-700">Controls</th>
                            <th class="px-4 py-2 text-left text-sm font-medium text-gray-700">Inventory</th>
                            <th class="px-4 py-2 text-left text-sm font-medium text-gray-700">Details</th>
                            <th class="px-4 py-2 text-left text-sm font-medium text-gray-700">Actions</th>
                        </tr>
                        </thead>
                        <tbody class="divide-y divide-gray-200 bg-white" data-sortable="true">
                        <?php foreach ($groupDescriptors as $descriptor): ?>
                            <tr class="hover:bg-gray-50 sortable-item transition-colors"
                                data-id="<?= htmlspecialchars($descriptor['_id']) ?>"
                                data-group="<?= htmlspecialchars($groupName) ?>">
                                <td class="px-4 py-3">
                                    <input type="checkbox" class="descriptor-checkbox group-item-<?= htmlspecialchars($groupName) ?> w-4 h-4 text-blue-600"
                                           value="<?= htmlspecialchars($descriptor['_id']) ?>"
                                           onchange="updateSelection()">
                                </td>

                                <td class="px-4 py-3">
                                    <div class="flex items-center gap-3">
                                        <?php if (!empty($descriptor['picture'])): ?>
                                            <img src="<?= htmlspecialchars($descriptor['picture']) ?>"
                                                 alt="<?= htmlspecialchars($descriptor['name']) ?>"
                                                 class="w-10 h-10 object-cover rounded-lg border border-gray-200">
                                        <?php endif; ?>
                                        <div>
                                            <div class="text-sm font-medium text-gray-900">
                                                <?= htmlspecialchars($descriptor['name']) ?>
                                            </div>
                                            <?php if (!empty($descriptor['specialText'])): ?>
                                                <div class="text-xs text-gray-500 truncate max-w-48">
                                                    <?= htmlspecialchars($descriptor['specialText']) ?>
                                                </div>
                                            <?php endif; ?>
                                            <div class="text-xs text-gray-400">
                                                Pos: <?= $descriptor['ordinalPosition'] ?? 0 ?> | ID: <?= substr($descriptor['_id'], -6) ?>
                                            </div>
                                        </div>
                                    </div>
                                </td>

                                <td class="px-4 py-3">
                                    <div class="flex gap-2">
                                        <!-- Quick toggle buttons for grouped view -->
                                        <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'enabled', <?= $descriptor['enabled'] ? 'false' : 'true' ?>)"
                                                class="p-2 rounded-full transition-colors <?= $descriptor['enabled'] ? 'text-green-600 bg-green-50 hover:bg-green-100' : 'text-gray-400 bg-gray-50 hover:bg-gray-100' ?>"
                                                title="<?= $descriptor['enabled'] ? 'Disable' : 'Enable' ?>">
                                            <i class="fas fa-power-off text-sm"></i>
                                        </button>

                                        <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'hidden', <?= !$descriptor['hidden'] ? 'true' : 'false' ?>)"
                                                class="p-2 rounded-full transition-colors <?= !$descriptor['hidden'] ? 'text-blue-600 bg-blue-50 hover:bg-blue-100' : 'text-gray-400 bg-gray-50 hover:bg-gray-100' ?>"
                                                title="<?= !$descriptor['hidden'] ? 'Hide' : 'Show' ?>">
                                            <i class="fas fa-eye<?= $descriptor['hidden'] ? '-slash' : '' ?> text-sm"></i>
                                        </button>

                                        <button onclick="quickToggle('<?= htmlspecialchars($descriptor['_id']) ?>', 'useForCarousel', <?= $descriptor['useForCarousel'] ? 'false' : 'true' ?>)"
                                                class="p-2 rounded-full transition-colors <?= $descriptor['useForCarousel'] ? 'text-purple-600 bg-purple-50 hover:bg-purple-100' : 'text-gray-400 bg-gray-50 hover:bg-gray-100' ?>"
                                                title="<?= $descriptor['useForCarousel'] ? 'Remove from Carousel' : 'Add to Carousel' ?>">
                                            <i class="fas fa-images text-sm"></i>
                                        </button>
                                    </div>
                                </td>

                                <td class="px-4 py-3">
                                    <div class="text-sm">
                                        <div class="flex items-center justify-between mb-1">
                                            <span class="text-gray-600 text-xs">Total:</span>
                                            <span class="font-medium"><?= $descriptor['inventory']['total'] ?></span>
                                        </div>
                                        <div class="flex items-center justify-between mb-2">
                                            <span class="text-gray-600 text-xs">Available:</span>
                                            <span class="font-medium <?= $descriptor['inventory']['availability'] > 50 ? 'text-green-600' :
                                                ($descriptor['inventory']['availability'] > 20 ? 'text-yellow-600' : 'text-red-600') ?>">
                                            <?= $descriptor['inventory']['availability'] ?>%
                                        </span>
                                        </div>

                                        <!-- Mini inventory bar -->
                                        <div class="inventory-bar bg-gray-200 rounded">
                                            <?php if ($descriptor['inventory']['total'] > 0): ?>
                                                <?php
                                                $occupiedPercent = ($descriptor['inventory']['occupied'] / $descriptor['inventory']['total']) * 100;
                                                $reservedPercent = ($descriptor['inventory']['reserved'] / $descriptor['inventory']['total']) * 100;
                                                $vacantPercent = ($descriptor['inventory']['vacant'] / $descriptor['inventory']['total']) * 100;
                                                ?>
                                                <div class="flex h-full rounded overflow-hidden">
                                                    <?php if ($occupiedPercent > 0): ?>
                                                        <div class="bg-red-500" style="width: <?= $occupiedPercent ?>%"></div>
                                                    <?php endif; ?>
                                                    <?php if ($reservedPercent > 0): ?>
                                                        <div class="bg-yellow-500" style="width: <?= $reservedPercent ?>%"></div>
                                                    <?php endif; ?>
                                                    <?php if ($vacantPercent > 0): ?>
                                                        <div class="bg-green-500" style="width: <?= $vacantPercent ?>%"></div>
                                                    <?php endif; ?>
                                                </div>
                                            <?php endif; ?>
                                        </div>
                                    </div>
                                </td>

                                <td class="px-4 py-3">
                                    <div class="text-xs space-y-1">
                                        <!-- Status badges -->
                                        <div class="flex flex-wrap gap-1">
                                            <?php if ($descriptor['enabled']): ?>
                                                <span class="bg-green-100 text-green-800 px-2 py-1 rounded-full text-xs">Enabled</span>
                                            <?php endif; ?>
                                            <?php if (!$descriptor['hidden']): ?>
                                                <span class="bg-blue-100 text-blue-800 px-2 py-1 rounded-full text-xs">Visible</span>
                                            <?php endif; ?>
                                            <?php if ($descriptor['useForCarousel']): ?>
                                                <span class="bg-purple-100 text-purple-800 px-2 py-1 rounded-full text-xs">Carousel</span>
                                            <?php endif; ?>
                                        </div>

                                        <!-- Deals count -->
                                        <?php if (!empty($descriptor['deals'])): ?>
                                            <div class="text-blue-600">
                                                <i class="fas fa-tags mr-1"></i>
                                                <?= count($descriptor['deals']) ?> deal<?= count($descriptor['deals']) !== 1 ? 's' : '' ?>
                                            </div>
                                        <?php endif; ?>

                                        <!-- Insurance -->
                                        <?php if (!empty($descriptor['defaultInsuranceCoverage'])): ?>
                                            <div class="text-green-600">
                                                <i class="fas fa-shield-alt mr-1"></i>
                                                Insurance
                                            </div>
                                        <?php endif; ?>

                                        <!-- Upgrades count -->
                                        <?php if (!empty($descriptor['upgradesTo'])): ?>
                                            <div class="text-orange-600">
                                                <i class="fas fa-arrow-up mr-1"></i>
                                                <?= count($descriptor['upgradesTo']) ?> upgrade<?= count($descriptor['upgradesTo']) !== 1 ? 's' : '' ?>
                                            </div>
                                        <?php endif; ?>
                                    </div>
                                </td>

                                <td class="px-4 py-3">
                                    <div class="flex items-center gap-1">
                                        <a href="?edit=<?= htmlspecialchars($descriptor['_id']) ?>"
                                           class="text-blue-600 hover:text-blue-800 p-2 rounded-full hover:bg-blue-50 transition-colors"
                                           title="Edit">
                                            <i class="fas fa-edit text-sm"></i>
                                        </a>

                                        <button onclick="deleteDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>', '<?= htmlspecialchars($descriptor['name']) ?>')"
                                                class="text-red-600 hover:text-red-800 p-2 rounded-full hover:bg-red-50 transition-colors"
                                                title="Delete">
                                            <i class="fas fa-trash text-sm"></i>
                                        </button>

                                        <button onclick="duplicateDescriptor('<?= htmlspecialchars($descriptor['_id']) ?>')"
                                                class="text-green-600 hover:text-green-800 p-2 rounded-full hover:bg-green-50 transition-colors"
                                                title="Duplicate">
                                            <i class="fas fa-copy text-sm"></i>
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        <?php endforeach; ?>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    <?php endforeach; ?>

    <?php if (empty($data['groupedDescriptors'])): ?>
        <div class="text-center py-12 text-gray-500">
            <div class="flex flex-col items-center">
                <i class="fas fa-layer-group text-4xl text-gray-300 mb-4"></i>
                <p class="text-lg font-medium">No grouped descriptors found</p>
                <?php if ($searchTerm): ?>
                    <p class="text-sm">Try adjusting your search criteria</p>
                    <a href="?" class="text-blue-600 hover:text-blue-800 text-sm mt-2">Clear search</a>
                <?php else: ?>
                    <p class="text-sm">Descriptors will be automatically grouped by size patterns</p>
                    <a href="?create=1" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded mt-3 inline-flex items-center gap-2">
                        <i class="fas fa-plus"></i>
                        Create Descriptor
                    </a>
                <?php endif; ?>
            </div>
        </div>
    <?php endif; ?>
</div>

<script>
    // Group-specific JavaScript functions
    function selectGroup(groupName) {
        const groupCheckboxes = document.querySelectorAll(`.group-item-${groupName.replace(/[^a-zA-Z0-9]/g, '')}`);
        const allSelected = Array.from(groupCheckboxes).every(cb => cb.checked);

        groupCheckboxes.forEach(checkbox => {
            checkbox.checked = !allSelected;
        });

        updateSelection();
    }

    function toggleGroupSelection(groupName) {
        const groupSelectAll = document.querySelector(`[data-group="${groupName}"]`);
        const safeGroupName = groupName.replace(/[^a-zA-Z0-9]/g, '');
        const groupCheckboxes = document.querySelectorAll(`.group-item-${safeGroupName}`);

        groupCheckboxes.forEach(checkbox => {
            checkbox.checked = groupSelectAll.checked;
        });

        updateSelection();
    }

    // Override the toggleGroup function for grouped view
    function toggleGroup(groupName) {
        const groupContent = document.getElementById(`group-${groupName}`);
        const toggleIcon = document.querySelector(`[onclick="toggleGroup('${groupName}')"] i`);

        if (groupContent.style.display === 'none') {
            groupContent.style.display = 'block';
            toggleIcon.className = 'fas fa-chevron-down group-toggle';
        } else {
            groupContent.style.display = 'none';
            toggleIcon.className = 'fas fa-chevron-right group-toggle';
        }
    }

    // Group management functions
    function bulkGroupAction(groupName, action) {
        const safeGroupName = groupName.replace(/[^a-zA-Z0-9]/g, '');
        const groupCheckboxes = document.querySelectorAll(`.group-item-${safeGroupName}`);
        const descriptorIds = Array.from(groupCheckboxes).map(cb => cb.value);

        // Temporarily set selected IDs to group items
        const originalSelection = new Set(RapidStorApp.selectedIds);
        RapidStorApp.selectedIds.clear();
        descriptorIds.forEach(id => RapidStorApp.selectedIds.add(id));

        // Perform the action
        RapidStorApp.batchAction(action);

        // Restore original selection
        RapidStorApp.selectedIds = originalSelection;
    }

    // Enhanced group statistics
    function updateGroupStats(groupName) {
        const safeGroupName = groupName.replace(/[^a-zA-Z0-9]/g, '');
        const groupRows = document.querySelectorAll(`tr[data-group="${groupName}"]`);

        let stats = {
            total: groupRows.length,
            enabled: 0,
            visible: 0,
            carousel: 0,
            totalUnits: 0,
            vacantUnits: 0
        };

        groupRows.forEach(row => {
            // Count enabled descriptors
            const enabledButton = row.querySelector('[onclick*="enabled"]');
            if (enabledButton && enabledButton.classList.contains('text-green-600')) {
                stats.enabled++;
            }

            // Count visible descriptors
            const visibleButton = row.querySelector('[onclick*="hidden"]');
            if (visibleButton && visibleButton.classList.contains('text-blue-600')) {
                stats.visible++;
            }

            // Count carousel descriptors
            const carouselButton = row.querySelector('[onclick*="useForCarousel"]');
            if (carouselButton && carouselButton.classList.contains('text-purple-600')) {
                stats.carousel++;
            }
        });

        return stats;
    }

    // Auto-collapse/expand groups based on content
    function autoManageGroups() {
        const groups = document.querySelectorAll('.group-content');
        groups.forEach(group => {
            const rows = group.querySelectorAll('tbody tr');

            // Auto-collapse if group has more than 10 items
            if (rows.length > 10) {
                group.style.display = 'none';
                const toggleIcon = document.querySelector(`[onclick*="${group.id.replace('group-', '')}"] i`);
                if (toggleIcon) {
                    toggleIcon.className = 'fas fa-chevron-right group-toggle';
                }
            }
        });
    }

    // Initialize group management
    document.addEventListener('DOMContentLoaded', function() {
        // Set up group toggle states
        const groupContents = document.querySelectorAll('.group-content');
        groupContents.forEach(content => {
            content.style.display = 'block'; // Start expanded
        });

        // Auto-manage groups if there are many
        autoManageGroups();
    });
</script>