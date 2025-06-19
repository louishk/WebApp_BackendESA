<?php
// includes/form.php - Create/Edit descriptor form

// Sanitize input
$isCreate = isset($_GET['create']);
$pageTitle = $isCreate ? 'Create New Descriptor' : 'Edit Descriptor';
?>

<h3 class="text-lg font-semibold mb-4">
    <?= htmlspecialchars($pageTitle) ?>
</h3>

<form method="post" id="descriptorForm">
    <input type="hidden" name="action" value="save_descriptor">
    <?php if ($editingDescriptor): ?>
        <input type="hidden" name="_id" value="<?= htmlspecialchars($editingDescriptor['_id'] ?? '') ?>">
    <?php endif; ?>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1" for="name">Name *</label>
            <input type="text" id="name" name="name" required
                   value="<?= htmlspecialchars($editingDescriptor['name'] ?? '') ?>"
                   class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
        </div>

        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1" for="description">Description</label>
            <input type="text" id="description" name="description"
                   value="<?= htmlspecialchars($editingDescriptor['description'] ?? '') ?>"
                   class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
        </div>

        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1" for="specialText">Special Text</label>
            <input type="text" id="specialText" name="specialText"
                   value="<?= htmlspecialchars($editingDescriptor['specialText'] ?? '') ?>"
                   class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
            <p class="text-xs text-gray-500 mt-1">Additional text displayed with the descriptor</p>
        </div>

        <div>
            <label class="block text-sm font-medium text-gray-700 mb-1" for="ordinalPosition">Ordinal Position</label>
            <input type="number" id="ordinalPosition" name="ordinalPosition" min="1"
                   value="<?= htmlspecialchars($editingDescriptor['ordinalPosition'] ?? (count($data['descriptors'] ?? []) + 1)) ?>"
                   class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
            <p class="text-xs text-gray-500 mt-1">Display order (lower numbers appear first)</p>
        </div>
    </div>

    <!-- Status Toggles -->
    <div class="mt-6">
        <label class="block text-sm font-medium text-gray-700 mb-3">Status Options</label>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
            <?php
            $statusOptions = [
                'enabled' => [
                    'label' => 'Enabled',
                    'description' => 'Descriptor is active and available',
                    'checked' => $editingDescriptor['enabled'] ?? true,
                    'color' => 'green'
                ],
                'visible' => [
                    'label' => 'Visible',
                    'description' => 'Show descriptor to customers',
                    'checked' => !($editingDescriptor['hidden'] ?? false),
                    'color' => 'blue'
                ],
                'useForCarousel' => [
                    'label' => 'Use for Carousel',
                    'description' => 'Include in image carousel',
                    'checked' => $editingDescriptor['useForCarousel'] ?? true,
                    'color' => 'purple'
                ]
            ];

            foreach ($statusOptions as $name => $option): ?>
                <label class="flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                    <input type="checkbox" 
                           id="<?= htmlspecialchars($name) ?>"
                           name="<?= htmlspecialchars($name) ?>" 
                           <?= $option['checked'] ? 'checked' : '' ?>
                           class="w-4 h-4 text-<?= htmlspecialchars($option['color']) ?>-600 bg-gray-100 border-gray-300 rounded focus:ring-<?= htmlspecialchars($option['color']) ?>-500">
                    <div>
                        <div class="text-sm font-medium text-gray-900"><?= htmlspecialchars($option['label']) ?></div>
                        <div class="text-xs text-gray-500"><?= htmlspecialchars($option['description']) ?></div>
                    </div>
                </label>
            <?php endforeach; ?>
        </div>
    </div>

    <!-- Additional Information -->
    <?php if ($editingDescriptor): ?>
        <div class="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
            <h4 class="text-sm font-semibold text-blue-900 mb-2">Additional Information</h4>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                <?php
                $additionalInfo = [
                    'ID' => substr($editingDescriptor['_id'] ?? '', -8),
                    'Corp Code' => $editingDescriptor['sCorpCode'] ?? 'CNCK',
                    'Location' => $editingDescriptor['sLocationCode'] ?? $selectedLocation,
                    'Deals' => count($editingDescriptor['deals'] ?? [])
                ];

                foreach ($additionalInfo as $label => $value): ?>
                    <div>
                        <span class="text-blue-700 font-medium"><?= htmlspecialchars($label) ?>:</span>
                        <span class="text-blue-600"><?= htmlspecialchars($value) ?></span>
                    </div>
                <?php endforeach; ?>
            </div>

            <?php if (!empty($editingDescriptor['inventory'])): ?>
                <div class="mt-3">
                    <p class="text-blue-700 font-medium text-xs mb-1">Current Inventory:</p>
                    <ul class="flex items-center gap-4">
                        <?php
                        $inventoryItems = [
                            'Total' => $editingDescriptor['inventory']['total'],
                            'Available' => $editingDescriptor['inventory']['vacant'],
                            'Availability' => $editingDescriptor['inventory']['availability'] . '%'
                        ];

                        foreach ($inventoryItems as $label => $value): ?>
                            <li class="text-xs text-blue-600">
                                <?= htmlspecialchars($label) ?>: <?= htmlspecialchars($value) ?>
                            </li>
                        <?php endforeach; ?>
                    </ul>
                </div>
            <?php endif; ?>
        </div>
    <?php endif; ?>

    <!-- Form Actions -->
    <div class="mt-6 flex gap-3">
        <button type="submit" class="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
            <i class="fas fa-save"></i>
            <?= htmlspecialchars($isCreate ? 'Create Descriptor' : 'Update Descriptor') ?>
        </button>

        <a href="?" class="bg-gray-600 hover:bg-gray-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
            <i class="fas fa-times"></i>
            Cancel
        </a>

        <?php if ($editingDescriptor): ?>
            <button type="button" id="duplicateBtn"
                    class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
                <i class="fas fa-copy"></i>
                Duplicate
            </button>
        <?php endif; ?>
    </div>
</form>

<?php if ($editingDescriptor): ?>
    <script>
    document.getElementById('duplicateBtn').addEventListener('click', function() {
        const formData = {
            action: 'save_descriptor',
            name: <?= json_encode($editingDescriptor['name'] . ' (Copy)') ?>,
            description: <?= json_encode($editingDescriptor['description'] ?? '') ?>,
            specialText: <?= json_encode($editingDescriptor['specialText'] ?? '') ?>,
            ordinalPosition: <?= json_encode(count($data['descriptors'] ?? []) + 1) ?>,
            enabled: <?= json_encode($editingDescriptor['enabled'] ?? true) ?>,
            visible: <?= json_encode(!($editingDescriptor['hidden'] ?? false)) ?>,
            useForCarousel: <?= json_encode($editingDescriptor['useForCarousel'] ?? true) ?>
        };

        const form = document.createElement('form');
        form.method = 'post';
        form.style.display = 'none';

        Object.entries(formData).forEach(([key, value]) => {
            if (value) {
                const input = document.createElement('input');
                input.type = 'hidden';
                input.name = key;
                input.value = value;
                form.appendChild(input);
            }
        });

        document.body.appendChild(form);
        form.submit();
    });
    </script>
<?php endif; ?>