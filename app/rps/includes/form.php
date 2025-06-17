<?php
// includes/form.php - Create/Edit descriptor form
?>

    <h3 class="text-lg font-semibold mb-4">
        <?= isset($_GET['create']) ? 'Create New Descriptor' : 'Edit Descriptor' ?>
    </h3>

    <form method="post">
        <input type="hidden" name="action" value="save_descriptor">
        <?php if ($editingDescriptor): ?>
            <input type="hidden" name="_id" value="<?= htmlspecialchars($editingDescriptor['_id']) ?>">
        <?php endif; ?>

        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Name *</label>
                <input type="text" name="name" required
                       value="<?= htmlspecialchars($editingDescriptor['name'] ?? '') ?>"
                       class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Description</label>
                <input type="text" name="description"
                       value="<?= htmlspecialchars($editingDescriptor['description'] ?? '') ?>"
                       class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Special Text</label>
                <input type="text" name="specialText"
                       value="<?= htmlspecialchars($editingDescriptor['specialText'] ?? '') ?>"
                       class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                <p class="text-xs text-gray-500 mt-1">Additional text displayed with the descriptor</p>
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Ordinal Position</label>
                <input type="number" name="ordinalPosition" min="1"
                       value="<?= htmlspecialchars($editingDescriptor['ordinalPosition'] ?? (count($data['descriptors']) + 1)) ?>"
                       class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500">
                <p class="text-xs text-gray-500 mt-1">Display order (lower numbers appear first)</p>
            </div>
        </div>

        <!-- Status Toggles -->
        <div class="mt-6">
            <label class="block text-sm font-medium text-gray-700 mb-3">Status Options</label>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <label class="flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                    <input type="checkbox" name="enabled" <?= ($editingDescriptor['enabled'] ?? true) ? 'checked' : '' ?>
                           class="w-4 h-4 text-green-600 bg-gray-100 border-gray-300 rounded focus:ring-green-500">
                    <div>
                        <div class="text-sm font-medium text-gray-900">Enabled</div>
                        <div class="text-xs text-gray-500">Descriptor is active and available</div>
                    </div>
                </label>

                <label class="flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                    <input type="checkbox" name="visible" <?= !($editingDescriptor['hidden'] ?? false) ? 'checked' : '' ?>
                           class="w-4 h-4 text-blue-600 bg-gray-100 border-gray-300 rounded focus:ring-blue-500">
                    <div>
                        <div class="text-sm font-medium text-gray-900">Visible</div>
                        <div class="text-xs text-gray-500">Show descriptor to customers</div>
                    </div>
                </label>

                <label class="flex items-center gap-3 p-3 border border-gray-200 rounded-lg hover:bg-gray-50 cursor-pointer">
                    <input type="checkbox" name="useForCarousel" <?= ($editingDescriptor['useForCarousel'] ?? true) ? 'checked' : '' ?>
                           class="w-4 h-4 text-purple-600 bg-gray-100 border-gray-300 rounded focus:ring-purple-500">
                    <div>
                        <div class="text-sm font-medium text-gray-900">Use for Carousel</div>
                        <div class="text-xs text-gray-500">Include in image carousel</div>
                    </div>
                </label>
            </div>
        </div>

        <!-- Additional Information -->
        <?php if ($editingDescriptor): ?>
            <div class="mt-6 p-4 bg-blue-50 border border-blue-200 rounded-lg">
                <h4 class="text-sm font-semibold text-blue-900 mb-2">Additional Information</h4>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-4 text-xs">
                    <div>
                        <span class="text-blue-700 font-medium">ID:</span>
                        <span class="text-blue-600"><?= htmlspecialchars(substr($editingDescriptor['_id'], -8)) ?></span>
                    </div>
                    <div>
                        <span class="text-blue-700 font-medium">Corp Code:</span>
                        <span class="text-blue-600"><?= htmlspecialchars($editingDescriptor['sCorpCode'] ?? 'CNCK') ?></span>
                    </div>
                    <div>
                        <span class="text-blue-700 font-medium">Location:</span>
                        <span class="text-blue-600"><?= htmlspecialchars($editingDescriptor['sLocationCode'] ?? $selectedLocation) ?></span>
                    </div>
                    <div>
                        <span class="text-blue-700 font-medium">Deals:</span>
                        <span class="text-blue-600"><?= count($editingDescriptor['deals'] ?? []) ?></span>
                    </div>
                </div>

                <?php if (!empty($editingDescriptor['inventory'])): ?>
                    <div class="mt-3">
                        <span class="text-blue-700 font-medium text-xs">Current Inventory:</span>
                        <div class="flex items-center gap-4 mt-1">
                <span class="text-xs text-blue-600">
                    Total: <?= $editingDescriptor['inventory']['total'] ?>
                </span>
                            <span class="text-xs text-blue-600">
                    Available: <?= $editingDescriptor['inventory']['vacant'] ?>
                </span>
                            <span class="text-xs text-blue-600">
                    Availability: <?= $editingDescriptor['inventory']['availability'] ?>%
                </span>
                        </div>
                    </div>
                <?php endif; ?>
            </div>
        <?php endif; ?>

        <!-- Form Actions -->
        <div class="mt-6 flex gap-3">
            <button type="submit" class="bg-green-600 hover:bg-green-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
                <i class="fas fa-save"></i>
                <?= isset($_GET['create']) ? 'Create Descriptor' : 'Update Descriptor' ?>
            </button>

            <a href="?" class="bg-gray-600 hover:bg-gray-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
                <i class="fas fa-times"></i>
                Cancel
            </a>

            <?php if ($editingDescriptor): ?>
                <button type="button" onclick="duplicateCurrentDescriptor()"
                        class="bg-blue-600 hover:bg-blue-700 text-white px-6 py-2 rounded-lg flex items-center gap-2 transition-colors">
                    <i class="fas fa-copy"></i>
                    Duplicate
                </button>
            <?php endif; ?>
        </div>
    </form>

<?php if ($editingDescriptor): ?>
    <script>
        function duplicateCurrentDescriptor() {
            // Create a form to duplicate the current descriptor
            const form = document.createElement('form');
            form.method = 'post';
            form.style.display = 'none';

            // Add action
            const actionInput = document.createElement('input');
            actionInput.type = 'hidden';
            actionInput.name = 'action';
            actionInput.value = 'save_descriptor';
            form.appendChild(actionInput);

            // Add descriptor data with modified name
            const nameInput = document.createElement('input');
            nameInput.type = 'hidden';
            nameInput.name = 'name';
            nameInput.value = '<?= htmlspecialchars($editingDescriptor['name']) ?> (Copy)';
            form.appendChild(nameInput);

            const descInput = document.createElement('input');
            descInput.type = 'hidden';
            descInput.name = 'description';
            descInput.value = '<?= htmlspecialchars($editingDescriptor['description'] ?? '') ?>';
            form.appendChild(descInput);

            const specialInput = document.createElement('input');
            specialInput.type = 'hidden';
            specialInput.name = 'specialText';
            specialInput.value = '<?= htmlspecialchars($editingDescriptor['specialText'] ?? '') ?>';
            form.appendChild(specialInput);

            const positionInput = document.createElement('input');
            positionInput.type = 'hidden';
            positionInput.name = 'ordinalPosition';
            positionInput.value = '<?= (count($data['descriptors']) + 1) ?>';
            form.appendChild(positionInput);

            // Add checkboxes
            <?php if ($editingDescriptor['enabled']): ?>
            const enabledInput = document.createElement('input');
            enabledInput.type = 'hidden';
            enabledInput.name = 'enabled';
            enabledInput.value = '1';
            form.appendChild(enabledInput);
            <?php endif; ?>

            <?php if (!($editingDescriptor['hidden'] ?? false)): ?>
            const visibleInput = document.createElement('input');
            visibleInput.type = 'hidden';
            visibleInput.name = 'visible';
            visibleInput.value = '1';
            form.appendChild(visibleInput);
            <?php endif; ?>

            <?php if ($editingDescriptor['useForCarousel']): ?>
            const carouselInput = document.createElement('input');
            carouselInput.type = 'hidden';
            carouselInput.name = 'useForCarousel';
            carouselInput.value = '1';
            form.appendChild(carouselInput);
            <?php endif; ?>

            document.body.appendChild(form);
            form.submit();
        }
    </script>
<?php endif; ?>