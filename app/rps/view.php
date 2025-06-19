<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Enhanced RapidStor Descriptor Manager</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/sortablejs@1.15.0/Sortable.min.js"></script>
    <style>
        .sortable-ghost { opacity: 0.4; background: #f3f4f6; }
        .sortable-chosen { transform: rotate(5deg); }
        .sortable-drag { opacity: 0.8; transform: rotate(5deg); }
        .group-header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); }
        .status-toggle { transition: all 0.2s ease; }
        .status-toggle:hover { transform: scale(1.1); }
        .inventory-bar { height: 4px; border-radius: 2px; overflow: hidden; }
        .inventory-segment { height: 100%; display: inline-block; }
    </style>
</head>
<body class="bg-gray-50 min-h-screen">

<div class="max-w-full mx-auto p-6">
    <div class="bg-white rounded-lg shadow-lg">

        <!-- Header -->
        <div class="border-b border-gray-200 p-6">
            <?php include 'includes/header.php'; ?>
        </div>

        <!-- Create/Edit Form -->
        <?php if (isset($_GET['create'])): ?>
            <div class="border-b border-gray-200 p-6 bg-gray-50">
                <?php include 'includes/form.php'; ?>
            </div>
        <?php elseif ($editingDescriptor): ?>
            <div class="border-b border-gray-200 p-6 bg-gray-50">
                <?php include 'includes/simplified_form.php'; ?>
            </div>
        <?php endif; ?>

        <!-- Batch Actions -->
        <div id="batchActions" class="border-b border-gray-200 p-4 bg-gray-50" style="display: none;">
            <?php include 'includes/batch_actions.php'; ?>
        </div>

        <!-- Main Content Area -->
        <div class="overflow-x-auto">
            <?php if ($viewMode === 'grouped' && isset($data['groupedDescriptors'])): ?>
                <?php include 'includes/grouped_view.php'; ?>
            <?php else: ?>
                <?php include 'includes/table_view.php'; ?>
            <?php endif; ?>
        </div>

        <!-- Footer -->
        <div class="border-t border-gray-200 px-6 py-4 bg-gray-50">
            <?php include 'includes/footer.php'; ?>
        </div>

    </div>
</div>

<!-- Group Creation Modal -->
<?php include 'includes/group_modal.php'; ?>

<!-- Data for JavaScript (centralized approach) -->
<script type="application/json" id="appData">
    {
        "descriptors": <?= json_encode($data['descriptors']) ?>,
    "deals": <?= json_encode($data['deals']) ?>,
    "insurance": <?= json_encode($data['insurance']) ?>,
    "unitTypes": <?= json_encode($data['unitTypes']) ?>,
    "lookups": {
        "deals": <?= json_encode($data['lookups']['deals']) ?>,
        "insurance": <?= json_encode($data['lookups']['insurance']) ?>,
        "unitTypes": <?= json_encode($data['lookups']['unitTypes']) ?>
    },
    "stats": <?= json_encode($data['stats']) ?>,
    "config": {
        "selectedLocation": "<?= htmlspecialchars($selectedLocation) ?>",
        "searchTerm": "<?= htmlspecialchars($searchTerm) ?>",
        "sortBy": "<?= htmlspecialchars($sortBy) ?>",
        "sortOrder": "<?= htmlspecialchars($sortOrder) ?>",
        "viewMode": "<?= htmlspecialchars($viewMode) ?>",
        "debug": <?= json_encode($debug) ?>
    }
}
</script>

<!-- Legacy compatibility layer -->
<script src="js/legacy-app.js"></script>

<script>
    // Initialize legacy compatibility
    document.addEventListener('DOMContentLoaded', function() {
        // Make data globally available for backward compatibility
        const appData = JSON.parse(document.getElementById('appData').textContent);

        // Legacy global variables
        window.descriptors = appData.descriptors;
        window.dealsLookup = appData.lookups.deals;
        window.insuranceLookup = appData.lookups.insurance;
        window.unitTypesLookup = appData.lookups.unitTypes;
        window.appStats = appData.stats;
        window.appConfig = appData.config;

        // Initialize legacy app if modern modules aren't available
        if (window.RapidStorApp && typeof window.RapidStorApp.init === 'function') {
            window.RapidStorApp.init();
        }

        console.log('✅ App data loaded and legacy compatibility established');
    });

    // Smart utilities for data-action handlers
    window.smartCarouselToggle = function() {
        if (!confirm('This will:\n• Turn OFF carousel for fully occupied units (100%)\n• Turn ON carousel for available units (<100%)\n\nContinue?')) {
            return;
        }

        if (window.RapidStorApp && window.RapidStorApp.showLoadingOverlay) {
            window.RapidStorApp.showLoadingOverlay('Processing smart carousel...');
        }

        const formData = new FormData();
        formData.append('action', 'smart_carousel_off');

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                if (window.RapidStorApp && window.RapidStorApp.hideLoadingOverlay) {
                    window.RapidStorApp.hideLoadingOverlay();
                }

                if (data.success) {
                    if (window.RapidStorApp && window.RapidStorApp.showToast) {
                        window.RapidStorApp.showToast(data.message, 'success', 4000);
                    }

                    if (data.updated_count > 0) {
                        setTimeout(() => {
                            window.location.reload(true);
                        }, 2000);
                    }
                } else {
                    if (window.RapidStorApp && window.RapidStorApp.showToast) {
                        window.RapidStorApp.showToast(data.error || 'Failed to update carousel settings', 'error');
                    }
                }
            })
            .catch(error => {
                if (window.RapidStorApp) {
                    window.RapidStorApp.hideLoadingOverlay();
                    window.RapidStorApp.showToast('Error: ' + error.message, 'error');
                }
            });
    };

    // Bulk toggle utilities
    window.bulkToggle = function(field, value) {
        const action = field === 'enabled' ? (value ? 'enable' : 'disable') : (value ? 'show' : 'hide');

        // Select all descriptors first
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');
        checkboxes.forEach(checkbox => checkbox.checked = true);

        // Update selection
        if (window.RapidStorApp && window.RapidStorApp.updateSelection) {
            window.RapidStorApp.updateSelection();
        }

        // Then perform batch action
        setTimeout(() => {
            if (window.RapidStorApp && window.RapidStorApp.batchAction) {
                window.RapidStorApp.batchAction(action);
            }
        }, 100);
    };

    // Export dropdown toggle
    window.exportSelected = function() {
        const dropdown = document.getElementById('exportDropdown');
        if (dropdown) {
            dropdown.classList.toggle('hidden');

            // Close dropdown when clicking elsewhere
            const closeDropdown = (e) => {
                if (!e.target.closest('#exportDropdown') && !e.target.closest('[data-action="show-export-dropdown"]')) {
                    dropdown.classList.add('hidden');
                    document.removeEventListener('click', closeDropdown);
                }
            };
            document.addEventListener('click', closeDropdown);
        }
    };

    // Group management functions
    window.updateSelectedDescriptorsList = function() {
        const selectedList = document.getElementById('selectedDescriptorsList');
        if (!selectedList) return;

        const selectedIds = window.RapidStorApp ? Array.from(window.RapidStorApp.selectedIds) : [];

        if (selectedIds.length === 0) {
            selectedList.innerHTML = '<span class="text-gray-400 italic">No descriptors selected</span>';
            return;
        }

        let listHTML = '';
        selectedIds.forEach((id, index) => {
            // Find descriptor name from the table
            const row = document.querySelector(`tr[data-id="${id}"]`);
            if (row) {
                const nameElement = row.querySelector('.text-sm.font-medium');
                const name = nameElement ? nameElement.textContent.trim() : `Descriptor ${id.substr(-6)}`;
                listHTML += `<div class="flex items-center justify-between py-1">
                    <span>${name}</span>
                    <span class="text-xs text-gray-400">${id.substr(-6)}</span>
                </div>`;
            }
        });

        selectedList.innerHTML = listHTML;
    };

    // Enhanced event delegation for data-action attributes
    document.addEventListener('click', function(e) {
        const target = e.target.closest('[data-action]');
        if (!target) return;

        const action = target.dataset.action;

        switch (action) {
            case 'smart-carousel-toggle':
                e.preventDefault();
                smartCarouselToggle();
                break;
            case 'bulk-enable-all':
                e.preventDefault();
                bulkToggle('enabled', true);
                break;
            case 'bulk-disable-all':
                e.preventDefault();
                bulkToggle('enabled', false);
                break;
            case 'show-export-dropdown':
                e.preventDefault();
                exportSelected();
                break;
            case 'show-group-modal':
                e.preventDefault();
                const modal = document.getElementById('groupModal');
                if (modal) {
                    updateSelectedDescriptorsList();
                    modal.classList.remove('hidden');
                }
                break;
            case 'close-group-modal':
                e.preventDefault();
                const groupModal = document.getElementById('groupModal');
                if (groupModal) {
                    groupModal.classList.add('hidden');
                }
                break;
            case 'close-success-modal':
                e.preventDefault();
                const successModal = document.getElementById('successModal');
                if (successModal) {
                    successModal.classList.add('hidden');
                    location.reload();
                }
                break;
            case 'create-group':
                e.preventDefault();
                createGroup();
                break;
        }
    });

    // Group creation function
    window.createGroup = function() {
        const groupName = document.getElementById('groupName').value.trim();
        if (!groupName) {
            if (window.RapidStorApp && window.RapidStorApp.showToast) {
                window.RapidStorApp.showToast('Please enter a group name', 'warning');
            }
            return;
        }

        const selectedArray = window.RapidStorApp ? Array.from(window.RapidStorApp.selectedIds) : [];
        if (selectedArray.length === 0) {
            if (window.RapidStorApp && window.RapidStorApp.showToast) {
                window.RapidStorApp.showToast('Please select at least one descriptor to group', 'warning');
            }
            return;
        }

        // Get group options
        const autoSort = document.getElementById('groupAutoSort').checked;
        const collapsible = document.getElementById('groupCollapsible').checked;
        const showStats = document.getElementById('groupShowStats').checked;

        const formData = new FormData();
        formData.append('action', 'group_descriptors');
        formData.append('descriptor_ids', JSON.stringify(selectedArray));
        formData.append('group_name', groupName);
        formData.append('auto_sort', autoSort);
        formData.append('collapsible', collapsible);
        formData.append('show_stats', showStats);

        // Show loading state
        const createButton = document.querySelector('[data-action="create-group"]');
        const originalText = createButton.innerHTML;
        createButton.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Creating...';
        createButton.disabled = true;

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                createButton.innerHTML = originalText;
                createButton.disabled = false;

                if (data.success) {
                    // Show success modal
                    document.getElementById('successMessage').textContent =
                        `Successfully created group "${groupName}" with ${selectedArray.length} descriptors.`;
                    document.getElementById('groupModal').classList.add('hidden');
                    document.getElementById('successModal').classList.remove('hidden');
                } else {
                    if (window.RapidStorApp && window.RapidStorApp.showToast) {
                        window.RapidStorApp.showToast('Failed to create group: ' + (data.error || 'Unknown error'), 'error');
                    }
                }
            })
            .catch(error => {
                createButton.innerHTML = originalText;
                createButton.disabled = false;
                if (window.RapidStorApp && window.RapidStorApp.showToast) {
                    window.RapidStorApp.showToast('Network error: ' + error.message, 'error');
                }
            });
    };
</script>

</body>
</html>