<?php
// includes/simplified_form.php - Enhanced edit form with upsell management
?>

<h3 class="text-lg font-semibold mb-4 flex items-center gap-2">
    <i class="fas fa-edit text-blue-600"></i>
    Edit Descriptor - <?= htmlspecialchars($editingDescriptor['name'] ?? 'Unknown') ?>
</h3>

<form method="post" class="space-y-6">
    <input type="hidden" name="action" value="save_descriptor_limited">
    <input type="hidden" name="_id" value="<?= htmlspecialchars($editingDescriptor['_id']) ?>">

    <!-- Descriptor Info (Read-only) -->
    <div class="bg-gray-50 p-4 rounded-lg border">
        <h4 class="font-medium text-gray-900 mb-2">Descriptor Information (Read-only)</h4>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4 text-sm">
            <div>
                <span class="text-gray-600">Name:</span>
                <span class="font-medium"><?= htmlspecialchars($editingDescriptor['name'] ?? '') ?></span>
            </div>
            <div>
                <span class="text-gray-600">Position:</span>
                <span class="font-medium"><?= htmlspecialchars($editingDescriptor['ordinalPosition'] ?? 0) ?></span>
            </div>
            <div>
                <span class="text-gray-600">Status:</span>
                <span class="font-medium">
                    <?= $editingDescriptor['enabled'] ? '✅ Enabled' : '❌ Disabled' ?>,
                    <?= !($editingDescriptor['hidden'] ?? false) ? '👁️ Visible' : '🙈 Hidden' ?>
                </span>
            </div>
        </div>
    </div>

    <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">

        <!-- Left Column -->
        <div class="space-y-6">
            <!-- Keywords (Editable) -->
            <div class="bg-white p-4 rounded-lg border border-blue-200">
                <label class="block text-sm font-medium text-gray-700 mb-2">
                    <i class="fas fa-key text-blue-600 mr-1"></i>
                    Keywords (Unit Type Matching)
                </label>
                <div id="keywordsContainer">
                    <?php
                    $keywords = $editingDescriptor['criteria']['include']['keywords'] ?? [];
                    if (empty($keywords)) {
                        $keywords = [''];
                    }
                    ?>
                    <?php foreach ($keywords as $index => $keyword): ?>
                        <div class="keyword-row flex items-center gap-2 mb-2">
                            <input type="text"
                                   name="keywords[]"
                                   value="<?= htmlspecialchars($keyword) ?>"
                                   class="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 font-mono"
                                   placeholder="e.g., XL/RG/90-110/WR/SD/NP/PLAT">
                            <button type="button" onclick="removeKeyword(this)"
                                    class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50">
                                <i class="fas fa-trash text-sm"></i>
                            </button>
                        </div>
                    <?php endforeach; ?>
                </div>
                <button type="button" onclick="addKeyword()"
                        class="text-blue-600 hover:text-blue-800 text-sm flex items-center gap-1 mt-2">
                    <i class="fas fa-plus"></i>
                    Add Keyword
                </button>
                <p class="text-xs text-gray-500 mt-2">
                    Keywords must exactly match the sTypeName of unit types. Use the debug page to find available type names.
                </p>
            </div>

            <!-- Descriptions (Editable) -->
            <div class="bg-white p-4 rounded-lg border border-green-200">
                <label class="block text-sm font-medium text-gray-700 mb-2">
                    <i class="fas fa-list text-green-600 mr-1"></i>
                    Descriptions
                </label>

                <!-- Main Description -->
                <div class="mb-4">
                    <label class="block text-sm font-medium text-gray-600 mb-1">Main Description</label>
                    <textarea name="description" rows="2"
                              class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-green-500 focus:border-green-500"
                              placeholder="Main descriptor description"><?= htmlspecialchars($editingDescriptor['description'] ?? '') ?></textarea>
                </div>

                <!-- Additional Descriptions -->
                <div class="mb-4">
                    <label class="block text-sm font-medium text-gray-600 mb-1">Additional Descriptions</label>
                    <div id="descriptionsContainer">
                        <?php
                        $descriptions = $editingDescriptor['descriptions'] ?? [];
                        if (empty($descriptions)) {
                            $descriptions = [''];
                        }
                        ?>
                        <?php foreach ($descriptions as $index => $desc): ?>
                            <div class="description-row flex items-center gap-2 mb-2">
                                <input type="text"
                                       name="descriptions[]"
                                       value="<?= htmlspecialchars($desc) ?>"
                                       class="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-green-500 focus:border-green-500"
                                       placeholder="Additional description">
                                <button type="button" onclick="removeDescription(this)"
                                        class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50">
                                    <i class="fas fa-trash text-sm"></i>
                                </button>
                            </div>
                        <?php endforeach; ?>
                    </div>
                    <button type="button" onclick="addDescription()"
                            class="text-green-600 hover:text-green-800 text-sm flex items-center gap-1 mt-2">
                        <i class="fas fa-plus"></i>
                        Add Description
                    </button>
                </div>

                <!-- Special Text -->
                <div>
                    <label class="block text-sm font-medium text-gray-600 mb-1">Special Text</label>
                    <input type="text" name="specialText"
                           value="<?= htmlspecialchars($editingDescriptor['specialText'] ?? '') ?>"
                           class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-green-500 focus:border-green-500"
                           placeholder="Special promotional text">
                </div>
            </div>
        </div>

        <!-- Right Column -->
        <div class="space-y-6">
            <!-- Deals (Editable) -->
            <div class="bg-white p-4 rounded-lg border border-purple-200">
                <label class="block text-sm font-medium text-gray-700 mb-2">
                    <i class="fas fa-tags text-purple-600 mr-1"></i>
                    Deals & Promotions
                </label>
                <div id="dealsContainer">
                    <?php
                    $selectedDeals = $editingDescriptor['deals'] ?? [];
                    ?>
                    <?php foreach ($data['deals'] as $deal): ?>
                        <label class="flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                            <input type="checkbox"
                                   name="deals[]"
                                   value="<?= htmlspecialchars($deal['_id']) ?>"
                                <?= in_array($deal['_id'], $selectedDeals) ? 'checked' : '' ?>
                                   class="w-4 h-4 text-purple-600 bg-gray-100 border-gray-300 rounded focus:ring-purple-500">
                            <div class="flex-1">
                                <div class="text-sm font-medium text-gray-900">
                                    <?= htmlspecialchars($deal['title'] ?? 'Untitled Deal') ?>
                                </div>
                                <div class="text-xs text-gray-500">
                                    <?= $deal['enable'] ? '✅ Active' : '⏸️ Inactive' ?>
                                    <?php if (!empty($deal['description'])): ?>
                                        - <?= htmlspecialchars(substr($deal['description'], 0, 50)) ?>...
                                    <?php endif; ?>
                                </div>
                            </div>
                        </label>
                    <?php endforeach; ?>
                </div>
            </div>
            <p class="text-xs text-gray-500 mt-2">
                Select deals to associate with this descriptor. Only active deals will be shown to customers.
            </p>
        </div>

        <!-- Insurance (Editable) -->
        <div class="bg-white p-4 rounded-lg border border-orange-200">
            <label class="block text-sm font-medium text-gray-700 mb-2">
                <i class="fas fa-shield-alt text-orange-600 mr-1"></i>
                Default Insurance Coverage
            </label>
            <select name="defaultInsuranceCoverage"
                    class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-orange-500 focus:border-orange-500">
                <option value="">No default insurance</option>
                <?php foreach ($data['insurance'] as $insurance): ?>
                    <option value="<?= htmlspecialchars($insurance['_id']) ?>"
                        <?= ($editingDescriptor['defaultInsuranceCoverage'] ?? '') === $insurance['_id'] ? 'selected' : '' ?>>
                        <?= htmlspecialchars($insurance['sCoverageDesc'] ?? 'Unknown Coverage') ?>
                        - $<?= number_format($insurance['dcCoverage'] ?? 0) ?>
                    </option>
                <?php endforeach; ?>
            </select>
            <p class="text-xs text-gray-500 mt-2">
                Select the default insurance coverage to offer with this unit type.
            </p>
        </div>
    </div>
    </div>

    <!-- Upsell Management (New Section) -->
    <div class="bg-white p-4 rounded-lg border border-indigo-200">
        <div class="flex items-center justify-between mb-4">
            <label class="block text-sm font-medium text-gray-700">
                <i class="fas fa-arrow-up text-indigo-600 mr-1"></i>
                Upsell Management
            </label>
            <div class="flex gap-2">
                <button type="button" onclick="addUpsell()"
                        class="text-indigo-600 hover:text-indigo-800 text-sm flex items-center gap-1">
                    <i class="fas fa-plus"></i>
                    Add Upsell
                </button>
                <button type="button" onclick="autoGenerateUpsells()"
                        class="bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded text-sm flex items-center gap-1">
                    <i class="fas fa-robot"></i>
                    Auto Generate
                </button>
            </div>
        </div>

        <div id="upsellsContainer">
            <?php
            $upsells = $editingDescriptor['upgradesTo'] ?? [];
            if (empty($upsells)) {
                echo '<div class="text-sm text-gray-500 text-center py-4">No upsells configured. Click "Add Upsell" or "Auto Generate" to get started.</div>';
            } else {
                foreach ($upsells as $index => $upsell):
                    $targetDescriptor = null;
                    foreach ($data['descriptors'] as $desc) {
                        if ($desc['_id'] === $upsell['_id']) {
                            $targetDescriptor = $desc;
                            break;
                        }
                    }
                    ?>
                    <div class="upsell-row border border-gray-200 rounded-lg p-3 mb-3">
                        <div class="flex items-center justify-between">
                            <div class="flex-1">
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <div>
                                        <label class="block text-xs font-medium text-gray-600 mb-1">Target Descriptor</label>
                                        <select name="upsells[<?= $index ?>][_id]"
                                                class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
                                                onchange="updateUpsellPreview(this, <?= $index ?>)">
                                            <option value="">Select descriptor to upsell to...</option>
                                            <?php foreach ($data['descriptors'] as $desc): ?>
                                                <?php if ($desc['_id'] !== $editingDescriptor['_id']): ?>
                                                    <option value="<?= htmlspecialchars($desc['_id']) ?>"
                                                        <?= $upsell['_id'] === $desc['_id'] ? 'selected' : '' ?>
                                                            data-name="<?= htmlspecialchars($desc['name']) ?>"
                                                            data-occupancy="<?= $desc['inventory']['occupancy'] ?? 0 ?>"
                                                            data-availability="<?= $desc['inventory']['availability'] ?? 0 ?>">
                                                        <?= htmlspecialchars($desc['name']) ?>
                                                        (<?= $desc['inventory']['occupancy'] ?? 0 ?>% occ, <?= $desc['inventory']['availability'] ?? 0 ?>% avail)
                                                    </option>
                                                <?php endif; ?>
                                            <?php endforeach; ?>
                                        </select>
                                    </div>
                                    <div>
                                        <label class="block text-xs font-medium text-gray-600 mb-1">Upsell Reason</label>
                                        <input type="text"
                                               name="upsells[<?= $index ?>][upgradeReason]"
                                               value="<?= htmlspecialchars($upsell['upgradeReason'] ?? '') ?>"
                                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
                                               placeholder="e.g., Larger Option +10sqft">
                                    </div>
                                </div>

                                <!-- Hidden fields for fixed values -->
                                <input type="hidden" name="upsells[<?= $index ?>][upgradeIcon]" value="fa-warehouse">
                                <input type="hidden" name="upsells[<?= $index ?>][upgradeIconPrefix]" value="fa-light">

                                <!-- Upsell Preview -->
                                <div id="upsellPreview<?= $index ?>" class="mt-2 p-2 bg-gray-50 rounded text-xs">
                                    <?php if ($targetDescriptor): ?>
                                        <div class="flex items-center gap-2">
                                            <i class="fa-light fa-warehouse text-indigo-600"></i>
                                            <span class="font-medium"><?= htmlspecialchars($targetDescriptor['name']) ?></span>
                                            <span class="text-gray-500">•</span>
                                            <span class="<?= ($targetDescriptor['inventory']['occupancy'] ?? 0) > 80 ? 'text-red-600' : 'text-green-600' ?>">
                                            <?= $targetDescriptor['inventory']['occupancy'] ?? 0 ?>% occupied
                                        </span>
                                            <span class="text-gray-500">•</span>
                                            <span class="<?= ($targetDescriptor['inventory']['availability'] ?? 0) < 20 ? 'text-red-600' : 'text-green-600' ?>">
                                            <?= $targetDescriptor['inventory']['availability'] ?? 0 ?>% available
                                        </span>
                                        </div>
                                    <?php endif; ?>
                                </div>
                            </div>

                            <button type="button" onclick="removeUpsell(this)"
                                    class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50 ml-3">
                                <i class="fas fa-trash text-sm"></i>
                            </button>
                        </div>
                    </div>
                <?php
                endforeach;
            }
            ?>
        </div>

        <p class="text-xs text-gray-500 mt-2">
            Configure which descriptors customers should be offered as upgrades. Target descriptors with good availability (20%+) for best results.
        </p>
    </div>

    <!-- Current Inventory Info (Read-only) -->
    <?php if (!empty($editingDescriptor['inventory'])): ?>
        <div class="bg-gray-50 p-4 rounded-lg border">
            <h4 class="font-medium text-gray-900 mb-2">Current Inventory (Read-only)</h4>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                <div>
                    <span class="text-gray-600">Total Units:</span>
                    <span class="font-medium"><?= $editingDescriptor['inventory']['total'] ?></span>
                </div>
                <div>
                    <span class="text-gray-600">Occupancy:</span>
                    <span class="font-medium text-red-600"><?= $editingDescriptor['inventory']['occupancy'] ?>%</span>
                </div>
                <div>
                    <span class="text-gray-600">Available:</span>
                    <span class="font-medium text-green-600"><?= $editingDescriptor['inventory']['availability'] ?>%</span>
                </div>
                <div>
                    <span class="text-gray-600">Matches:</span>
                    <span class="font-medium"><?= count($editingDescriptor['inventory']['matched_unit_types'] ?? []) ?></span>
                </div>
            </div>
        </div>
    <?php endif; ?>

    <!-- Form Actions -->
    <div class="flex gap-3 pt-4 border-t">
        <button type="submit"
                class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
            <i class="fas fa-save"></i>
            Save Changes
        </button>

        <a href="?"
           class="bg-gray-600 hover:bg-gray-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
            <i class="fas fa-times"></i>
            Cancel
        </a>

        <a href="debug_inventory.php?location=<?= $selectedLocation ?>"
           target="_blank"
           class="bg-purple-600 hover:bg-purple-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
            <i class="fas fa-bug"></i>
            Debug Keywords
        </a>
    </div>
</form>

<script>
    let upsellCounter = <?= count($editingDescriptor['upgradesTo'] ?? []) ?>;

    // Keywords management
    function addKeyword() {
        const container = document.getElementById('keywordsContainer');
        const newRow = document.createElement('div');
        newRow.className = 'keyword-row flex items-center gap-2 mb-2';
        newRow.innerHTML = `
        <input type="text"
               name="keywords[]"
               value=""
               class="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 font-mono"
               placeholder="e.g., XL/RG/90-110/WR/SD/NP/PLAT">
        <button type="button" onclick="removeKeyword(this)"
                class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50">
            <i class="fas fa-trash text-sm"></i>
        </button>
    `;
        container.appendChild(newRow);
        newRow.querySelector('input').focus();
    }

    function removeKeyword(button) {
        const container = document.getElementById('keywordsContainer');
        if (container.children.length > 1) {
            button.closest('.keyword-row').remove();
        } else {
            // Don't remove the last one, just clear it
            button.closest('.keyword-row').querySelector('input').value = '';
        }
    }

    // Descriptions management
    function addDescription() {
        const container = document.getElementById('descriptionsContainer');
        const newRow = document.createElement('div');
        newRow.className = 'description-row flex items-center gap-2 mb-2';
        newRow.innerHTML = `
        <input type="text"
               name="descriptions[]"
               value=""
               class="flex-1 border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-green-500 focus:border-green-500"
               placeholder="Additional description">
        <button type="button" onclick="removeDescription(this)"
                class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50">
            <i class="fas fa-trash text-sm"></i>
        </button>
    `;
        container.appendChild(newRow);
        newRow.querySelector('input').focus();
    }

    function removeDescription(button) {
        const container = document.getElementById('descriptionsContainer');
        if (container.children.length > 1) {
            button.closest('.description-row').remove();
        } else {
            // Don't remove the last one, just clear it
            button.closest('.description-row').querySelector('input').value = '';
        }
    }

    // Upsell management
    function addUpsell() {
        const container = document.getElementById('upsellsContainer');

        // Remove "no upsells" message if it exists
        const noUpsellsMsg = container.querySelector('.text-center');
        if (noUpsellsMsg) {
            noUpsellsMsg.remove();
        }

        const newRow = document.createElement('div');
        newRow.className = 'upsell-row border border-gray-200 rounded-lg p-3 mb-3';
        newRow.innerHTML = `
        <div class="flex items-center justify-between">
            <div class="flex-1">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="block text-xs font-medium text-gray-600 mb-1">Target Descriptor</label>
                        <select name="upsells[${upsellCounter}][_id]"
                                class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
                                onchange="updateUpsellPreview(this, ${upsellCounter})">
                            <option value="">Select descriptor to upsell to...</option>
                            ${descriptors.filter(d => d._id !== '<?= $editingDescriptor['_id'] ?>').map(desc =>
            `<option value="${desc._id}"
                                         data-name="${desc.name}"
                                         data-occupancy="${desc.inventory?.occupancy || 0}"
                                         data-availability="${desc.inventory?.availability || 0}">
                                    ${desc.name} (${desc.inventory?.occupancy || 0}% occ, ${desc.inventory?.availability || 0}% avail)
                                </option>`
        ).join('')}
                        </select>
                    </div>
                    <div>
                        <label class="block text-xs font-medium text-gray-600 mb-1">Upsell Reason</label>
                        <input type="text"
                               name="upsells[${upsellCounter}][upgradeReason]"
                               value=""
                               class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500"
                               placeholder="e.g., Larger Option +10sqft">
                    </div>
                </div>

                <input type="hidden" name="upsells[${upsellCounter}][upgradeIcon]" value="fa-warehouse">
                <input type="hidden" name="upsells[${upsellCounter}][upgradeIconPrefix]" value="fa-light">

                <div id="upsellPreview${upsellCounter}" class="mt-2 p-2 bg-gray-50 rounded text-xs">
                    Select a target descriptor to see preview
                </div>
            </div>

            <button type="button" onclick="removeUpsell(this)"
                    class="text-red-600 hover:text-red-800 p-2 rounded hover:bg-red-50 ml-3">
                <i class="fas fa-trash text-sm"></i>
            </button>
        </div>
    `;
        container.appendChild(newRow);
        upsellCounter++;
    }

    function removeUpsell(button) {
        button.closest('.upsell-row').remove();

        // Add "no upsells" message if container is empty
        const container = document.getElementById('upsellsContainer');
        if (container.children.length === 0) {
            container.innerHTML = '<div class="text-sm text-gray-500 text-center py-4">No upsells configured. Click "Add Upsell" or "Auto Generate" to get started.</div>';
        }
    }

    function updateUpsellPreview(selectElement, index) {
        const selectedOption = selectElement.options[selectElement.selectedIndex];
        const preview = document.getElementById(`upsellPreview${index}`);

        if (selectedOption.value) {
            const name = selectedOption.dataset.name;
            const occupancy = selectedOption.dataset.occupancy;
            const availability = selectedOption.dataset.availability;

            const occupancyClass = occupancy > 80 ? 'text-red-600' : 'text-green-600';
            const availabilityClass = availability < 20 ? 'text-red-600' : 'text-green-600';

            preview.innerHTML = `
            <div class="flex items-center gap-2">
                <i class="fa-light fa-warehouse text-indigo-600"></i>
                <span class="font-medium">${name}</span>
                <span class="text-gray-500">•</span>
                <span class="${occupancyClass}">${occupancy}% occupied</span>
                <span class="text-gray-500">•</span>
                <span class="${availabilityClass}">${availability}% available</span>
            </div>
        `;

            // Auto-suggest reason based on names
            const reasonInput = selectElement.closest('.upsell-row').querySelector('input[name*="upgradeReason"]');
            if (!reasonInput.value) {
                reasonInput.value = generateUpsellReason('<?= $editingDescriptor['name'] ?>', name);
            }
        } else {
            preview.innerHTML = 'Select a target descriptor to see preview';
        }
    }

    function generateUpsellReason(fromName, toName) {
        // Extract size patterns
        const fromSize = fromName.match(/(\d+(?:-\d+)?)\s*sq\s*ft/i);
        const toSize = toName.match(/(\d+(?:-\d+)?)\s*sq\s*ft/i);

        if (fromSize && toSize) {
            return `Upgrade to ${toSize[1]}sqft`;
        }

        // Check for premium upgrade
        if (fromName.toLowerCase().includes('regular') && toName.toLowerCase().includes('premium')) {
            return 'Premium upgrade';
        }

        // Generic fallback
        return `Upgrade to ${toName.split(' ').slice(0, 2).join(' ')}`;
    }

    function autoGenerateUpsells() {
        if (!confirm('This will replace all current upsells with auto-generated ones based on smart rules. Continue?')) {
            return;
        }

        // Clear current upsells
        document.getElementById('upsellsContainer').innerHTML = '';
        upsellCounter = 0;

        // Generate upsells based on rules
        const currentDescriptor = descriptors.find(d => d._id === '<?= $editingDescriptor['_id'] ?>');
        const suggestedUpsells = generateSmartUpsells(currentDescriptor, descriptors);

        suggestedUpsells.forEach(upsell => {
            addUpsell();
            const lastRow = document.querySelector('.upsell-row:last-child');
            const select = lastRow.querySelector('select');
            const reasonInput = lastRow.querySelector('input[name*="upgradeReason"]');

            select.value = upsell.targetId;
            reasonInput.value = upsell.reason;
            updateUpsellPreview(select, upsellCounter - 1);
        });

        if (suggestedUpsells.length > 0) {
            RapidStorApp.showToast(`Generated ${suggestedUpsells.length} smart upsell suggestions`, 'success');
        } else {
            RapidStorApp.showToast('No suitable upsells found based on current rules', 'info');
        }
    }

    function generateSmartUpsells(currentDesc, allDescriptors) {
        const suggestions = [];
        const currentName = currentDesc.name.toLowerCase();

        // Rule 1: Same size Regular → Premium
        if (currentName.includes('regular')) {
            const premiumVersion = allDescriptors.find(d => {
                const name = d.name.toLowerCase();
                return name.includes('premium') &&
                    extractSize(name) === extractSize(currentName) &&
                    d.inventory?.availability > 20;
            });

            if (premiumVersion) {
                suggestions.push({
                    targetId: premiumVersion._id,
                    reason: 'Premium upgrade'
                });
            }
        }

        // Rule 2: Next size up with availability
        const currentSize = extractSizeNumber(currentName);
        if (currentSize) {
            const nextSizes = allDescriptors
                .filter(d => {
                    const size = extractSizeNumber(d.name.toLowerCase());
                    return size && size > currentSize &&
                        d.inventory?.availability > 20 &&
                        d._id !== currentDesc._id;
                })
                .sort((a, b) => extractSizeNumber(a.name.toLowerCase()) - extractSizeNumber(b.name.toLowerCase()))
                .slice(0, 2); // Get top 2 next sizes

            nextSizes.forEach(desc => {
                suggestions.push({
                    targetId: desc._id,
                    reason: `Larger Option +${extractSizeNumber(desc.name.toLowerCase()) - currentSize}sqft`
                });
            });
        }

        return suggestions.slice(0, 3); // Limit to 3 suggestions
    }

    function extractSize(name) {
        const match = name.match(/(\d+(?:-\d+)?)\s*sq\s*ft/i);
        return match ? match[1] : null;
    }

    function extractSizeNumber(name) {
        const match = name.match(/(\d+)(?:-(\d+))?\s*sq\s*ft/i);
        if (match) {
            return match[2] ? parseInt(match[2]) : parseInt(match[1]); // Use upper bound if range
        }
        return null;
    }

    // Form validation
    document.querySelector('form').addEventListener('submit', function(e) {
        const keywords = Array.from(document.querySelectorAll('input[name="keywords[]"]'))
            .map(input => input.value.trim())
            .filter(value => value !== '');

        if (keywords.length === 0) {
            if (!confirm('No keywords specified. This descriptor will not match any unit types and will show 0 inventory. Continue anyway?')) {
                e.preventDefault();
                return false;
            }
        }
    });
</script>