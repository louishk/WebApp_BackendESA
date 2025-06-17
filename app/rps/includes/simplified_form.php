<?php
// includes/simplified_form.php - Simplified edit form for specific fields only
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
            <div class="grid grid-cols-1 gap-2 max-h-48 overflow-y-auto">
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