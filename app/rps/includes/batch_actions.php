<?php
// includes/batch_actions.php - Enhanced batch action controls
?>

<div class="flex items-center justify-between">
    <div class="flex items-center gap-2">
        <span id="selectedCount" class="text-sm text-gray-600 font-medium">0 selected</span>
        <span class="text-gray-300">|</span>
        <span class="text-xs text-gray-500">Select descriptors to enable batch actions</span>
    </div>

    <div class="flex items-center gap-2">
        <!-- Quick Toggle Actions -->
        <div class="flex items-center gap-1 border-r border-gray-300 pr-2 mr-2">
            <button onclick="batchAction('enable')"
                    class="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Enable selected descriptors">
                <i class="fas fa-toggle-on text-xs"></i>
                Enable
            </button>

            <button onclick="batchAction('disable')"
                    class="bg-yellow-600 hover:bg-yellow-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Disable selected descriptors">
                <i class="fas fa-toggle-off text-xs"></i>
                Disable
            </button>
        </div>

        <!-- Visibility Actions -->
        <div class="flex items-center gap-1 border-r border-gray-300 pr-2 mr-2">
            <button onclick="batchAction('show')"
                    class="bg-blue-600 hover:bg-blue-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Make selected descriptors visible">
                <i class="fas fa-eye text-xs"></i>
                Show
            </button>

            <button onclick="batchAction('hide')"
                    class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Hide selected descriptors">
                <i class="fas fa-eye-slash text-xs"></i>
                Hide
            </button>
        </div>

        <!-- Batch Apply Actions -->
        <div class="flex items-center gap-1 border-r border-gray-300 pr-2 mr-2">
            <button onclick="showBatchApplyModal('insurance')"
                    class="bg-teal-600 hover:bg-teal-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Apply insurance to selected descriptors">
                <i class="fas fa-shield-alt text-xs"></i>
                Apply Insurance
            </button>

            <button onclick="showBatchApplyModal('deals')"
                    class="bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Apply deals to selected descriptors">
                <i class="fas fa-tags text-xs"></i>
                Apply Deals
            </button>
        </div>

        <!-- Management Actions -->
        <div class="flex items-center gap-1">
            <button onclick="groupSelected()"
                    class="bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Group selected descriptors">
                <i class="fas fa-layer-group text-xs"></i>
                Group
            </button>

            <button onclick="exportSelected()"
                    class="bg-cyan-600 hover:bg-cyan-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Export selected descriptors">
                <i class="fas fa-download text-xs"></i>
                Export
            </button>

            <!-- DELETE BUTTON - Restored -->
            <button onclick="RapidStorApp.batchAction('delete')"
                    class="bg-red-600 hover:bg-red-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Delete selected descriptors">
                <i class="fas fa-trash text-xs"></i>
                Delete
            </button>
        </div>
    </div>
</div>

<!-- Export dropdown (hidden by default) -->
<div id="exportDropdown" class="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-10 hidden">
    <div class="py-1">
        <button onclick="RapidStorApp.exportData('csv')" class="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
            <i class="fas fa-file-csv mr-2"></i>Export as CSV
        </button>
        <button onclick="RapidStorApp.exportData('json')" class="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
            <i class="fas fa-file-code mr-2"></i>Export as JSON
        </button>
    </div>
</div>

<!-- Batch Apply Modal -->
<div id="batchApplyModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-md w-full transform transition-all">
            <div class="flex justify-between items-center px-6 py-4 border-b border-gray-200">
                <h3 class="text-lg font-semibold text-gray-900 flex items-center gap-2" id="batchApplyTitle">
                    <i class="fas fa-magic text-blue-600"></i>
                    Batch Apply
                </h3>
                <button onclick="closeBatchApplyModal()" class="text-gray-400 hover:text-gray-600 transition-colors">
                    <i class="fas fa-times"></i>
                </button>
            </div>

            <div class="px-6 py-4" id="batchApplyContent">
                <!-- Content will be populated by JavaScript -->
            </div>

            <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
                <button onclick="closeBatchApplyModal()"
                        class="px-4 py-2 text-gray-600 hover:text-gray-800 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors">
                    Cancel
                </button>
                <button onclick="executeBatchApply()"
                        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors flex items-center gap-2">
                    <i class="fas fa-magic"></i>
                    Apply to Selected
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Auto Upsell Modal -->
<div id="autoUpsellModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-lg w-full transform transition-all">
            <div class="flex justify-between items-center px-6 py-4 border-b border-gray-200">
                <h3 class="text-lg font-semibold text-gray-900 flex items-center gap-2">
                    <i class="fas fa-robot text-purple-600"></i>
                    Auto-Generate Upsells
                </h3>
                <button onclick="closeAutoUpsellModal()" class="text-gray-400 hover:text-gray-600 transition-colors">
                    <i class="fas fa-times"></i>
                </button>
            </div>

            <div class="px-6 py-4">
                <p class="text-sm text-gray-600 mb-4">
                    This will automatically generate upsell recommendations for selected descriptors based on:
                </p>

                <div class="space-y-3 mb-4">
                    <div class="flex items-center gap-2">
                        <input type="checkbox" id="upsellRuleSameSize" checked class="w-4 h-4 text-purple-600">
                        <label class="text-sm">Same size Regular → Premium (if available)</label>
                    </div>
                    <div class="flex items-center gap-2">
                        <input type="checkbox" id="upsellRuleNextSize" checked class="w-4 h-4 text-purple-600">
                        <label class="text-sm">Next size up with availability (Regular or Premium)</label>
                    </div>
                    <div class="flex items-center gap-2">
                        <input type="checkbox" id="upsellRuleAvailabilityOnly" checked class="w-4 h-4 text-purple-600">
                        <label class="text-sm">Only suggest units with >20% availability</label>
                    </div>
                </div>

                <div class="bg-blue-50 p-3 rounded border border-blue-200">
                    <div class="text-sm font-medium text-blue-900 mb-1">Preview Rules:</div>
                    <div class="text-xs text-blue-700">
                        • Regular 10-12sqft → Premium 10-12sqft (if available)<br>
                        • If not available → Regular/Premium 12-14sqft (next size)<br>
                        • Only suggest units with sufficient availability
                    </div>
                </div>
            </div>

            <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
                <button onclick="closeAutoUpsellModal()"
                        class="px-4 py-2 text-gray-600 hover:text-gray-800 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors">
                    Cancel
                </button>
                <button onclick="executeAutoUpsell()"
                        class="px-4 py-2 bg-purple-600 hover:bg-purple-700 text-white rounded-md transition-colors flex items-center gap-2">
                    <i class="fas fa-robot"></i>
                    Generate Upsells
                </button>
            </div>
        </div>
    </div>
</div>

<script>
    let currentBatchApplyType = '';

    function exportSelected() {
        const dropdown = document.getElementById('exportDropdown');
        dropdown.classList.toggle('hidden');

        // Close dropdown when clicking elsewhere
        document.addEventListener('click', function closeDropdown(e) {
            if (!e.target.closest('#exportDropdown') && !e.target.closest('[onclick="exportSelected()"]')) {
                dropdown.classList.add('hidden');
                document.removeEventListener('click', closeDropdown);
            }
        });
    }

        // Batch Apply Functions
    function showBatchApplyModal(type) {
        if (RapidStorApp.selectedIds.size === 0) {
            RapidStorApp.showToast('Please select at least one descriptor', 'warning');
            return;
        }

        currentBatchApplyType = type;
        const modal = document.getElementById('batchApplyModal');
        const title = document.getElementById('batchApplyTitle');
        const content = document.getElementById('batchApplyContent');

        if (type === 'insurance') {
            title.innerHTML = '<i class="fas fa-shield-alt text-teal-600"></i> Batch Apply Insurance';
            content.innerHTML = `
            <div class="mb-4">
                <div class="text-sm font-medium text-gray-700 mb-2">Apply insurance to ${RapidStorApp.selectedIds.size} selected descriptors:</div>
                <select id="batchInsuranceSelect" class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-teal-500 focus:border-teal-500">
                    <option value="">Remove default insurance</option>
                    ${Object.values(insuranceLookup).map(insurance =>
                `<option value="${insurance._id}">${insurance.sCoverageDesc} - $${new Intl.NumberFormat().format(insurance.dcCoverage)}</option>`
            ).join('')}
                </select>
                <p class="text-sm text-gray-500 mt-2">This will replace the current default insurance for all selected descriptors.</p>
            </div>
        `;
        } else if (type === 'deals') {
            title.innerHTML = '<i class="fas fa-tags text-purple-600"></i> Batch Apply Deals';
            content.innerHTML = `
            <div class="mb-4">
                <div class="text-sm font-medium text-gray-700 mb-2">Apply deals to ${RapidStorApp.selectedIds.size} selected descriptors:</div>
                <div class="max-h-48 overflow-y-auto border border-gray-300 rounded-md p-3">
                    ${Object.values(dealsLookup).map(deal => `
                        <label class="flex items-center gap-2 mb-2 cursor-pointer">
                            <input type="checkbox" class="batch-deal-checkbox w-4 h-4 text-purple-600" value="${deal._id}">
                            <div class="flex-1">
                                <div class="text-sm font-medium">${deal.title}</div>
                                <div class="text-xs text-gray-500">${deal.enable ? '✅ Active' : '⏸️ Inactive'}</div>
                            </div>
                        </label>
                    `).join('')}
                </div>
                <div class="mt-3">
                    <label class="flex items-center gap-2">
                        <input type="radio" name="dealApplyMode" value="replace" checked class="w-4 h-4 text-purple-600">
                        <span class="text-sm">Replace existing deals</span>
                    </label>
                    <label class="flex items-center gap-2">
                        <input type="radio" name="dealApplyMode" value="add" class="w-4 h-4 text-purple-600">
                        <span class="text-sm">Add to existing deals</span>
                    </label>
                </div>
                <p class="text-sm text-gray-500 mt-2">Select which deals to apply and whether to replace or add to existing deals.</p>
            </div>
        `;
        }

        modal.classList.remove('hidden');
    }

    function closeBatchApplyModal() {
        document.getElementById('batchApplyModal').classList.add('hidden');
        currentBatchApplyType = '';
    }

    function executeBatchApply() {
        const selectedArray = Array.from(RapidStorApp.selectedIds);
        let updateData = {};

        if (currentBatchApplyType === 'insurance') {
            const insuranceId = document.getElementById('batchInsuranceSelect').value;
            updateData = {
                field: 'defaultInsuranceCoverage',
                value: insuranceId || null
            };
        } else if (currentBatchApplyType === 'deals') {
            const selectedDeals = Array.from(document.querySelectorAll('.batch-deal-checkbox:checked')).map(cb => cb.value);
            const applyMode = document.querySelector('input[name="dealApplyMode"]:checked').value;
            updateData = {
                field: 'deals',
                value: selectedDeals,
                mode: applyMode
            };
        }

        // Show loading state
        const button = document.querySelector('[onclick="executeBatchApply()"]');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Applying...';
        button.disabled = true;

        const formData = new FormData();
        formData.append('action', 'batch_apply');
        formData.append('descriptor_ids', JSON.stringify(selectedArray));
        formData.append('update_data', JSON.stringify(updateData));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                button.innerHTML = originalText;
                button.disabled = false;

                if (data.success) {
                    RapidStorApp.showToast(data.message, 'success');
                    closeBatchApplyModal();
                    setTimeout(() => location.reload(), 1500);
                } else {
                    RapidStorApp.showToast('Batch apply failed: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                button.innerHTML = originalText;
                button.disabled = false;
                RapidStorApp.showToast('Network error: ' + error.message, 'error');
            });
    }

    // Enhanced batch action handler that includes proper delete functionality
    function batchAction(action) {
        if (RapidStorApp.selectedIds.size === 0) {
            RapidStorApp.showToast('Please select at least one descriptor', 'warning');
            return;
        }

        // Special handling for delete action
        if (action === 'delete') {
            const selectedCount = RapidStorApp.selectedIds.size;
            const confirmMessage = `Are you sure you want to delete ${selectedCount} selected descriptor${selectedCount !== 1 ? 's' : ''}?\n\nThis action cannot be undone.`;

            if (!confirm(confirmMessage)) {
                return;
            }

            // Show loading state
            const deleteButton = document.querySelector('[onclick="RapidStorApp.batchAction(\'delete\')"]');
            const originalText = deleteButton.innerHTML;
            deleteButton.innerHTML = '<i class="fas fa-spinner fa-spin text-xs"></i>Deleting...';
            deleteButton.disabled = true;

            // Prepare form data for batch delete
            const selectedArray = Array.from(RapidStorApp.selectedIds);
            const formData = new FormData();
            formData.append('action', 'batch_update');
            formData.append('operation', 'delete');
            formData.append('selected_ids', JSON.stringify(selectedArray));

            fetch(window.location.href, {
                method: 'POST',
                body: formData
            })
                .then(response => response.json())
                .then(data => {
                    deleteButton.innerHTML = originalText;
                    deleteButton.disabled = false;

                    if (data.success) {
                        RapidStorApp.showToast(data.message || `Successfully deleted ${selectedCount} descriptors`, 'success');

                        // Clear selection
                        RapidStorApp.selectedIds.clear();
                        RapidStorApp.updateSelection();

                        // Refresh the page after a delay
                        setTimeout(() => {
                            window.location.reload();
                        }, 1500);
                    } else {
                        RapidStorApp.showToast('Batch delete failed: ' + (data.error || 'Unknown error'), 'error');
                    }
                })
                .catch(error => {
                    deleteButton.innerHTML = originalText;
                    deleteButton.disabled = false;
                    RapidStorApp.showToast('Network error: ' + error.message, 'error');
                });

            return;
        }

        // Handle other batch actions using the existing logic
        RapidStorApp.batchAction(action);
    }



    // Auto Upsell Functions
    function showAutoUpsellModal() {
        if (RapidStorApp.selectedIds.size === 0) {
            RapidStorApp.showToast('Please select at least one descriptor', 'warning');
            return;
        }

        document.getElementById('autoUpsellModal').classList.remove('hidden');
    }

    function closeAutoUpsellModal() {
        document.getElementById('autoUpsellModal').classList.add('hidden');
    }

    function executeAutoUpsell() {
        const selectedArray = Array.from(RapidStorApp.selectedIds);
        const rules = {
            sameSize: document.getElementById('upsellRuleSameSize').checked,
            nextSize: document.getElementById('upsellRuleNextSize').checked,
            availabilityOnly: document.getElementById('upsellRuleAvailabilityOnly').checked
        };

        // Show loading state
        const button = document.querySelector('[onclick="executeAutoUpsell()"]');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-2"></i>Generating...';
        button.disabled = true;

        const formData = new FormData();
        formData.append('action', 'auto_generate_upsells');
        formData.append('descriptor_ids', JSON.stringify(selectedArray));
        formData.append('rules', JSON.stringify(rules));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                button.innerHTML = originalText;
                button.disabled = false;

                if (data.success) {
                    RapidStorApp.showToast(data.message, 'success');
                    closeAutoUpsellModal();
                    setTimeout(() => location.reload(), 1500);
                } else {
                    RapidStorApp.showToast('Auto upsell failed: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                button.innerHTML = originalText;
                button.disabled = false;
                RapidStorApp.showToast('Network error: ' + error.message, 'error');
            });
    }

    // Add auto upsell button to the batch actions
    document.addEventListener('DOMContentLoaded', function() {
        // Add the auto upsell button to the management actions
        const managementActions = document.querySelector('.flex.items-center.gap-1:last-child');
        if (managementActions) {
            const autoUpsellButton = document.createElement('button');
            autoUpsellButton.onclick = showAutoUpsellModal;
            autoUpsellButton.className = 'bg-violet-600 hover:bg-violet-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1';
            autoUpsellButton.title = 'Auto-generate upsells for selected descriptors';
            autoUpsellButton.innerHTML = '<i class="fas fa-robot text-xs"></i>Auto Upsell';

            managementActions.insertBefore(autoUpsellButton, managementActions.lastElementChild);
        }
    });
</script>