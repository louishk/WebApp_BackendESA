/**
 * RapidStor Descriptor Manager - JavaScript Application
 * Corrected and optimized version
 */

const RapidStorApp = {
    selectedIds: new Set(),
    dragDropEnabled: false,
    sortableInstance: null,

    init() {
        console.log('Initializing RapidStor App...');
        this.setupEventListeners();
        this.updateSelection();
        this.initializeGroupToggles();
        this.trackPerformance();
    },

    setupEventListeners() {
        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            // Ctrl/Cmd + A to select all
            if ((e.ctrlKey || e.metaKey) && e.key === 'a' && e.target.tagName !== 'INPUT') {
                e.preventDefault();
                const selectAllCheckbox = document.getElementById('selectAll');
                if (selectAllCheckbox) {
                    selectAllCheckbox.checked = true;
                    this.toggleSelectAll();
                }
            }

            // Escape to clear selection
            if (e.key === 'Escape') {
                const selectAllCheckbox = document.getElementById('selectAll');
                if (selectAllCheckbox) {
                    selectAllCheckbox.checked = false;
                    this.toggleSelectAll();
                }
                this.closeGroupModal();
            }

            // Delete key to delete selected
            if (e.key === 'Delete' && this.selectedIds.size > 0) {
                this.batchAction('delete');
            }
        });
    },

    // Quick Toggle Functions
    quickToggle(descriptorId, field, value) {
        // Find the toggle element that was clicked
        const toggleElement = event.target.closest('label').querySelector('input');
        const statusLabel = event.target.closest('label').querySelector('span');
        const toggleSwitch = event.target.closest('label').querySelector('div');

        // Show loading state
        if (toggleSwitch) {
            toggleSwitch.style.opacity = '0.6';
        }

        const formData = new FormData();
        formData.append('action', 'quick_toggle');
        formData.append('descriptor_id', descriptorId);
        formData.append('field', field);
        formData.append('value', value.toString());

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    // Update the UI immediately after successful API call
                    this.updateToggleUI(descriptorId, field, value);
                    this.showToast('Updated successfully', 'success');
                } else {
                    // Revert the toggle on failure
                    if (toggleElement) {
                        toggleElement.checked = !value;
                    }
                    this.showToast('Failed to update: ' + (data.error || 'Unknown error'), 'error');
                }
                // Remove loading state
                if (toggleSwitch) {
                    toggleSwitch.style.opacity = '1';
                }
            })
            .catch(error => {
                // Revert the toggle on error
                if (toggleElement) {
                    toggleElement.checked = !value;
                }
                if (toggleSwitch) {
                    toggleSwitch.style.opacity = '1';
                }
                this.showToast('Network error: ' + error.message, 'error');
            });
    },

    updateToggleUI(descriptorId, field, value) {
        // Find all toggles for this descriptor (in case there are multiple views)
        const descriptorRows = document.querySelectorAll(`tr[data-id="${descriptorId}"]`);

        descriptorRows.forEach(row => {
            const toggles = row.querySelectorAll('.status-toggle');
            let targetToggle = null;
            let targetIndex = 0;

            if (field === 'enabled') {
                targetIndex = 0;
            } else if (field === 'hidden') {
                targetIndex = 1;
                value = !value; // Invert for hidden field
            } else if (field === 'useForCarousel') {
                targetIndex = 2;
            }

            if (toggles[targetIndex]) {
                const toggle = toggles[targetIndex];
                const label = toggle.closest('label');
                const switchDiv = label.querySelector('div');
                const switchButton = switchDiv.querySelector('span');
                const statusText = label.querySelector('span');

                // Update checkbox state
                toggle.checked = value;

                // Update visual switch
                if (value) {
                    switchDiv.className = switchDiv.className.replace('bg-gray-200', 'bg-green-600');
                    if (field === 'hidden') switchDiv.className = switchDiv.className.replace('bg-green-600', 'bg-blue-600');
                    if (field === 'useForCarousel') switchDiv.className = switchDiv.className.replace('bg-green-600', 'bg-purple-600');
                    switchButton.className = switchButton.className.replace('translate-x-1', 'translate-x-5');
                } else {
                    switchDiv.className = switchDiv.className.replace(/bg-(green|blue|purple)-600/, 'bg-gray-200');
                    switchButton.className = switchButton.className.replace('translate-x-5', 'translate-x-1');
                }

                // Update status text and color
                if (field === 'enabled') {
                    statusText.textContent = value ? 'Enabled' : 'Disabled';
                    statusText.className = `text-xs ${value ? 'text-green-600 font-medium' : 'text-gray-500'}`;
                } else if (field === 'hidden') {
                    statusText.textContent = value ? 'Visible' : 'Hidden';
                    statusText.className = `text-xs ${value ? 'text-blue-600 font-medium' : 'text-gray-500'}`;
                } else if (field === 'useForCarousel') {
                    statusText.textContent = 'Carousel';
                    statusText.className = `text-xs ${value ? 'text-purple-600 font-medium' : 'text-gray-500'}`;
                }
            }
        });
    },

    // Optimized Drag and Drop Functions
    enableDragDrop() {
        const button = document.getElementById('dragToggle');
        const sortableTable = document.getElementById('sortableTable');

        if (!sortableTable) {
            this.showToast('Sortable table not found', 'error');
            return;
        }

        const rows = sortableTable.querySelectorAll('.sortable-item');
        if (rows.length === 0) {
            this.showToast('No items to reorder', 'warning');
            return;
        }

        if (rows.length > 100) {
            if (!confirm(`You have ${rows.length} descriptors. Reordering large lists may take a while. Continue?`)) {
                return;
            }
        }

        if (!this.dragDropEnabled) {
            // Enable drag and drop
            this.sortableInstance = Sortable.create(sortableTable, {
                animation: 150,
                ghostClass: 'sortable-ghost',
                chosenClass: 'sortable-chosen',
                dragClass: 'sortable-drag',
                handle: '.drag-handle',
                onEnd: (evt) => {
                    // Small delay to ensure DOM is updated
                    setTimeout(() => this.updateOrder(), 100);
                }
            });

            this.dragDropEnabled = true;
            button.innerHTML = '<i class="fas fa-save mr-1"></i>Save Order';
            button.className = 'bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm';
            this.showToast(`Drag and drop enabled for ${rows.length} items. Drag rows to reorder, then click "Save Order".`, 'info');
        } else {
            // Save order
            this.updateOrder();
        }
    },

    updateOrder() {
        const rows = document.querySelectorAll('#sortableTable .sortable-item');
        const orderedIds = Array.from(rows).map(row => row.dataset.id);

        console.log('Reordering descriptors:', orderedIds.length, 'items');

        // Check if there are too many items
        if (orderedIds.length > 100) {
            this.showToast('Too many descriptors to reorder at once. Please try with fewer items.', 'warning');
            return;
        }

        // Show loading state
        const button = document.getElementById('dragToggle');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Saving...';
        button.disabled = true;

        // Show progress indicator
        this.showProgressToast('Saving order changes...', orderedIds.length);

        const formData = new FormData();
        formData.append('action', 'reorder_descriptors');
        formData.append('ordered_ids', JSON.stringify(orderedIds));

        console.log('Sending reorder request with', orderedIds.length, 'IDs');

        // Increase timeout for large operations
        const timeoutDuration = Math.max(30000, orderedIds.length * 500); // At least 30 seconds, more for larger lists
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            console.warn(`Reorder request timed out after ${timeoutDuration/1000} seconds`);
            controller.abort();
        }, timeoutDuration);

        fetch(window.location.href, {
            method: 'POST',
            body: formData,
            signal: controller.signal
        })
            .then(response => {
                clearTimeout(timeoutId);
                console.log('Reorder response status:', response.status);

                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                return response.text().then(text => {
                    console.log('Raw response length:', text.length);
                    try {
                        return JSON.parse(text);
                    } catch (e) {
                        console.error('Failed to parse JSON response:', text.substring(0, 500));
                        throw new Error('Invalid JSON response from server');
                    }
                });
            })
            .then(data => {
                console.log('Reorder response data:', data);

                button.innerHTML = originalText;
                button.disabled = false;
                this.hideProgressToast();

                if (data.success) {
                    const updatedCount = data.updated_count || 0;
                    const totalAttempted = data.total_attempted || orderedIds.length;

                    if (updatedCount === totalAttempted) {
                        this.showToast(`Order updated successfully (${updatedCount} descriptors)`, 'success');
                    } else if (updatedCount > 0) {
                        this.showToast(`Partially updated: ${updatedCount}/${totalAttempted} descriptors`, 'warning');
                    } else {
                        this.showToast('No changes were needed - descriptors already in correct order', 'info');
                    }

                    // Reset drag and drop mode
                    this.disableDragDropMode();

                    // Update ordinal positions in the UI
                    this.updateUIPositions(rows);

                    // Show any errors that occurred
                    if (data.errors && data.errors.length > 0) {
                        console.warn('Some errors occurred during reorder:', data.errors);
                        this.showDetailedErrors(data.errors);
                    }

                    // Log the new order if provided
                    if (data.new_order) {
                        console.log('New descriptor order (sample):', data.new_order);
                    }
                } else {
                    this.showToast('Failed to update order: ' + (data.error || 'Unknown error'), 'error');
                    console.error('Order update failed:', data);
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                button.innerHTML = originalText;
                button.disabled = false;
                this.hideProgressToast();

                console.error('Reorder error:', error);

                if (error.name === 'AbortError') {
                    this.showToast('Request timed out. The operation may still be processing in the background. Please refresh the page to see the latest order.', 'warning', 8000);
                    console.warn('Drag & drop request was aborted due to timeout');
                } else if (error.message.includes('Invalid JSON')) {
                    this.showToast('Server returned invalid response. Please try again or refresh the page.', 'error');
                } else {
                    this.showToast('Network error: ' + error.message, 'error');
                }
            });
    },

    // Helper function to disable drag & drop mode
    disableDragDropMode() {
        if (this.sortableInstance) {
            this.sortableInstance.destroy();
            this.sortableInstance = null;
        }
        this.dragDropEnabled = false;
        const button = document.getElementById('dragToggle');
        if (button) {
            button.innerHTML = '<i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop';
            button.className = 'bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm';
        }
    },

    // Helper function to update UI positions
    updateUIPositions(rows) {
        rows.forEach((row, index) => {
            const positionSpan = row.querySelector('.bg-gray-100 span, .font-mono');
            if (positionSpan) {
                positionSpan.textContent = index + 1;
            }
        });
    },

    // Enhanced toast with progress indicator
    showProgressToast(message, totalItems) {
        // Remove any existing progress toast
        this.hideProgressToast();

        const toast = document.createElement('div');
        toast.id = 'progressToast';
        toast.className = 'fixed top-4 right-4 bg-blue-500 text-white px-6 py-4 rounded-lg shadow-lg z-50 max-w-sm';

        toast.innerHTML = `
            <div class="flex items-center gap-3">
                <i class="fas fa-spinner fa-spin"></i>
                <div class="flex-1">
                    <div class="font-medium">${message}</div>
                    <div class="text-sm opacity-90">Processing ${totalItems} items...</div>
                </div>
            </div>
        `;

        document.body.appendChild(toast);
    },

    // Hide progress toast
    hideProgressToast() {
        const toast = document.getElementById('progressToast');
        if (toast) {
            document.body.removeChild(toast);
        }
    },

    // Show detailed errors in a collapsible format
    showDetailedErrors(errors) {
        if (errors.length === 0) return;

        const errorToast = document.createElement('div');
        errorToast.className = 'fixed top-20 right-4 bg-yellow-500 text-white px-6 py-4 rounded-lg shadow-lg z-50 max-w-md';

        errorToast.innerHTML = `
            <div class="flex items-start gap-3">
                <i class="fas fa-exclamation-triangle mt-1"></i>
                <div class="flex-1">
                    <div class="font-medium">Some issues occurred:</div>
                    <div class="text-sm mt-1">
                        ${errors.slice(0, 2).map(error => `<div>• ${error}</div>`).join('')}
                        ${errors.length > 2 ? `<div>• And ${errors.length - 2} more...</div>` : ''}
                    </div>
                    <button onclick="this.parentElement.parentElement.parentElement.remove()" 
                            class="text-xs underline mt-2">Dismiss</button>
                </div>
            </div>
        `;

        document.body.appendChild(errorToast);

        // Auto-remove after 10 seconds
        setTimeout(() => {
            if (document.body.contains(errorToast)) {
                document.body.removeChild(errorToast);
            }
        }, 10000);
    },

    // Selection Functions
    toggleSelectAll() {
        const selectAll = document.getElementById('selectAll');
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');

        checkboxes.forEach(checkbox => {
            checkbox.checked = selectAll.checked;
        });

        this.updateSelection();
    },

    updateSelection() {
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');
        const selectAll = document.getElementById('selectAll');

        this.selectedIds.clear();
        let checkedCount = 0;

        checkboxes.forEach(checkbox => {
            if (checkbox.checked) {
                this.selectedIds.add(checkbox.value);
                checkedCount++;
            }
        });

        // Update select all checkbox
        if (selectAll) {
            selectAll.checked = checkedCount === checkboxes.length && checkboxes.length > 0;
            selectAll.indeterminate = checkedCount > 0 && checkedCount < checkboxes.length;
        }

        // Show/hide batch actions
        const batchActions = document.getElementById('batchActions');
        const selectedCount = document.getElementById('selectedCount');

        if (this.selectedIds.size > 0) {
            batchActions.style.display = 'block';
            selectedCount.textContent = `${this.selectedIds.size} selected`;
        } else {
            batchActions.style.display = 'none';
        }
    },

    // Bulk Actions
    bulkToggle(field, value) {
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');
        let count = 0;

        checkboxes.forEach(checkbox => {
            const row = checkbox.closest('tr');
            const descriptorId = checkbox.value;

            // Find the corresponding toggle in the row
            const toggles = row.querySelectorAll('.status-toggle');
            let targetToggle = null;

            if (field === 'enabled') {
                targetToggle = toggles[0];
            } else if (field === 'hidden') {
                targetToggle = toggles[1];
            } else if (field === 'useForCarousel') {
                targetToggle = toggles[2];
            }

            if (targetToggle && targetToggle.checked !== (field === 'hidden' ? !value : value)) {
                targetToggle.checked = (field === 'hidden' ? !value : value);
                this.quickToggle(descriptorId, field, value);
                count++;
            }
        });

        if (count > 0) {
            this.showToast(`Updated ${count} descriptors`, 'success');
        }
    },

    batchAction(action) {
        if (this.selectedIds.size === 0) {
            this.showToast('Please select at least one descriptor', 'warning');
            return;
        }

        let confirmMessage = '';
        switch (action) {
            case 'delete':
                confirmMessage = `Are you sure you want to delete ${this.selectedIds.size} descriptors? This cannot be undone.`;
                break;
            case 'enable':
                confirmMessage = `Enable ${this.selectedIds.size} selected descriptors?`;
                break;
            case 'disable':
                confirmMessage = `Disable ${this.selectedIds.size} selected descriptors?`;
                break;
            case 'show':
                confirmMessage = `Make ${this.selectedIds.size} selected descriptors visible?`;
                break;
            case 'hide':
                confirmMessage = `Hide ${this.selectedIds.size} selected descriptors?`;
                break;
        }

        if (action === 'delete' && !confirm(confirmMessage)) {
            return;
        }

        // Perform the batch action
        const selectedArray = Array.from(this.selectedIds);
        selectedArray.forEach(id => {
            const checkbox = document.querySelector(`input[value="${id}"]`);
            if (checkbox) {
                switch (action) {
                    case 'enable':
                        this.quickToggle(id, 'enabled', true);
                        break;
                    case 'disable':
                        this.quickToggle(id, 'enabled', false);
                        break;
                    case 'show':
                        this.quickToggle(id, 'hidden', false);
                        break;
                    case 'hide':
                        this.quickToggle(id, 'hidden', true);
                        break;
                    case 'delete':
                        this.deleteDescriptor(id, 'Selected descriptor');
                        break;
                }
            }
        });
    },

    // Group Functions
    groupSelected() {
        if (this.selectedIds.size === 0) {
            this.showToast('Please select at least one descriptor to group', 'warning');
            return;
        }

        document.getElementById('groupModal').classList.remove('hidden');
    },

    closeGroupModal() {
        document.getElementById('groupModal').classList.add('hidden');
        document.getElementById('groupName').value = '';
    },

    createGroup() {
        const groupName = document.getElementById('groupName').value.trim();
        if (!groupName) {
            this.showToast('Please enter a group name', 'warning');
            return;
        }

        const selectedArray = Array.from(this.selectedIds);

        const formData = new FormData();
        formData.append('action', 'group_descriptors');
        formData.append('descriptor_ids', JSON.stringify(selectedArray));
        formData.append('group_name', groupName);

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                if (data.success) {
                    this.showToast('Group created successfully', 'success');
                    this.closeGroupModal();
                    setTimeout(() => location.reload(), 1000);
                } else {
                    this.showToast('Failed to create group: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                this.showToast('Network error: ' + error.message, 'error');
            });
    },

    toggleGroup(groupName) {
        const groupContent = document.getElementById(`group-${groupName}`);
        const toggleIcon = document.querySelector(`[onclick="RapidStorApp.toggleGroup('${groupName}')"] i`);

        if (groupContent && toggleIcon) {
            if (groupContent.style.display === 'none') {
                groupContent.style.display = 'block';
                toggleIcon.className = 'fas fa-chevron-down group-toggle';
            } else {
                groupContent.style.display = 'none';
                toggleIcon.className = 'fas fa-chevron-right group-toggle';
            }
        }
    },

    // Utility Functions
    deleteDescriptor(descriptorId, descriptorName) {
        if (!confirm(`Delete descriptor "${descriptorName}"?`)) {
            return;
        }

        this.showToast('Delete functionality to be implemented', 'info');
    },

    duplicateDescriptor(descriptorId) {
        this.showToast('Duplicate functionality to be implemented', 'info');
    },

    // Enhanced showToast with duration parameter
    showToast(message, type = 'info', duration = 3000) {
        // Create toast notification
        const toast = document.createElement('div');
        const bgColor = type === 'success' ? 'bg-green-500' :
            type === 'error' ? 'bg-red-500' :
                type === 'warning' ? 'bg-yellow-500' : 'bg-blue-500';

        toast.className = `fixed top-4 right-4 ${bgColor} text-white px-6 py-3 rounded-lg shadow-lg z-50 transform transition-transform duration-300 translate-x-full max-w-sm`;

        // Handle long messages
        if (message.length > 100) {
            toast.innerHTML = `
                <div>
                    <div class="font-medium">${message.substring(0, 100)}...</div>
                    <button onclick="this.parentElement.parentElement.remove()" 
                            class="text-xs underline mt-1">Dismiss</button>
                </div>
            `;
        } else {
            toast.textContent = message;
        }

        document.body.appendChild(toast);

        // Slide in
        setTimeout(() => {
            toast.classList.remove('translate-x-full');
        }, 100);

        // Remove after specified duration
        setTimeout(() => {
            if (document.body.contains(toast)) {
                toast.classList.add('translate-x-full');
                setTimeout(() => {
                    if (document.body.contains(toast)) {
                        document.body.removeChild(toast);
                    }
                }, 300);
            }
        }, duration);
    },

    initializeGroupToggles() {
        // Initialize group toggle states
        const groupContents = document.querySelectorAll('.group-content');
        groupContents.forEach(content => {
            content.style.display = 'block'; // Start expanded
        });
    },

    // Export functionality
    exportData(format = 'csv') {
        const selectedData = descriptors.filter(desc =>
            this.selectedIds.size === 0 || this.selectedIds.has(desc._id)
        );

        if (format === 'csv') {
            const csv = this.convertToCSV(selectedData);
            this.downloadFile(csv, 'descriptors.csv', 'text/csv');
        } else if (format === 'json') {
            const json = JSON.stringify(selectedData, null, 2);
            this.downloadFile(json, 'descriptors.json', 'application/json');
        }
    },

    convertToCSV(data) {
        const headers = ['Name', 'Description', 'Enabled', 'Visible', 'Carousel', 'Position', 'Inventory Total', 'Availability %'];
        const csvContent = [
            headers.join(','),
            ...data.map(desc => [
                `"${desc.name || ''}"`,
                `"${desc.description || ''}"`,
                desc.enabled ? 'Yes' : 'No',
                !desc.hidden ? 'Yes' : 'No',
                desc.useForCarousel ? 'Yes' : 'No',
                desc.ordinalPosition || 0,
                desc.inventory?.total || 0,
                desc.inventory?.availability || 0
            ].join(','))
        ].join('\n');

        return csvContent;
    },

    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    },

    trackPerformance() {
        const perfData = {
            descriptorCount: typeof descriptors !== 'undefined' ? descriptors.length : 0,
            renderTime: performance.now(),
            memoryUsage: performance.memory ? {
                used: Math.round(performance.memory.usedJSHeapSize / 1024 / 1024),
                total: Math.round(performance.memory.totalJSHeapSize / 1024 / 1024)
            } : null
        };

        console.log('Performance data:', perfData);
    }
};

// Global functions for backward compatibility
function quickToggle(descriptorId, field, value) {
    RapidStorApp.quickToggle(descriptorId, field, value);
}

function enableDragDrop() {
    RapidStorApp.enableDragDrop();
}

function toggleSelectAll() {
    RapidStorApp.toggleSelectAll();
}

function updateSelection() {
    RapidStorApp.updateSelection();
}

function bulkToggle(field, value) {
    RapidStorApp.bulkToggle(field, value);
}

function batchAction(action) {
    RapidStorApp.batchAction(action);
}

function groupSelected() {
    RapidStorApp.groupSelected();
}

function closeGroupModal() {
    RapidStorApp.closeGroupModal();
}

function createGroup() {
    RapidStorApp.createGroup();
}

function toggleGroup(groupName) {
    RapidStorApp.toggleGroup(groupName);
}

function deleteDescriptor(descriptorId, descriptorName) {
    RapidStorApp.deleteDescriptor(descriptorId, descriptorName);
}

function duplicateDescriptor(descriptorId) {
    RapidStorApp.duplicateDescriptor(descriptorId);
}

/**
 * Enhanced Drag & Drop with Manual Save
 * Replace the drag & drop related methods in your app.js with these
 */

// Add these properties to your RapidStorApp object
const DragDropEnhancements = {
    hasUnsavedChanges: false,
    originalOrder: [],

    // Enhanced enableDragDrop with manual save mode
    enableDragDrop() {
        const button = document.getElementById('dragToggle');
        const sortableTable = document.getElementById('sortableTable');

        if (!sortableTable) {
            this.showToast('Sortable table not found', 'error');
            return;
        }

        const rows = sortableTable.querySelectorAll('.sortable-item');
        if (rows.length === 0) {
            this.showToast('No items to reorder', 'warning');
            return;
        }

        if (rows.length > 100) {
            if (!confirm(`You have ${rows.length} descriptors. Reordering large lists may take a while when saving. Continue?`)) {
                return;
            }
        }

        if (!this.dragDropEnabled) {
            // Store original order for comparison and potential reset
            this.originalOrder = Array.from(rows).map(row => ({
                id: row.dataset.id,
                element: row,
                originalPosition: Array.from(rows).indexOf(row) + 1
            }));

            // Enable drag and drop
            this.sortableInstance = Sortable.create(sortableTable, {
                animation: 150,
                ghostClass: 'sortable-ghost',
                chosenClass: 'sortable-chosen',
                dragClass: 'sortable-drag',
                handle: '.drag-handle',
                onEnd: (evt) => {
                    // Mark as having unsaved changes
                    this.hasUnsavedChanges = true;
                    this.updateDragDropUI();
                    this.updatePositionNumbers();

                    // Show quick feedback
                    this.showToast('Order changed - remember to save when finished', 'info', 2000);
                }
            });

            this.dragDropEnabled = true;
            this.hasUnsavedChanges = false;
            this.updateDragDropUI();

            this.showToast(`Drag and drop enabled for ${rows.length} items. Drag rows to reorder, then click "Save Order" when finished.`, 'info', 4000);
        } else {
            // Save order or cancel
            if (this.hasUnsavedChanges) {
                this.showSaveConfirmation();
            } else {
                this.cancelDragDrop();
            }
        }
    },

    // Update the drag & drop button UI
    updateDragDropUI() {
        const button = document.getElementById('dragToggle');
        if (!button) return;

        if (!this.dragDropEnabled) {
            // Not in drag mode
            button.innerHTML = '<i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop';
            button.className = 'bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm';
        } else if (this.hasUnsavedChanges) {
            // In drag mode with unsaved changes
            button.innerHTML = '<i class="fas fa-save mr-1"></i>Save Order';
            button.className = 'bg-orange-600 hover:bg-orange-700 text-white px-3 py-1 rounded text-sm animate-pulse';
        } else {
            // In drag mode but no changes yet
            button.innerHTML = '<i class="fas fa-times mr-1"></i>Cancel';
            button.className = 'bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm';
        }
    },

    // Update position numbers in real-time
    updatePositionNumbers() {
        const rows = document.querySelectorAll('#sortableTable .sortable-item');
        rows.forEach((row, index) => {
            const positionSpan = row.querySelector('.bg-gray-100 span, .font-mono');
            if (positionSpan) {
                const newPosition = index + 1;
                const originalPosition = this.originalOrder.find(item => item.id === row.dataset.id)?.originalPosition;

                positionSpan.textContent = newPosition;

                // Highlight changed positions
                if (originalPosition && originalPosition !== newPosition) {
                    positionSpan.className = positionSpan.className.replace('bg-gray-100', 'bg-yellow-100 border-yellow-300');
                    positionSpan.style.fontWeight = 'bold';
                } else {
                    positionSpan.className = positionSpan.className.replace('bg-yellow-100 border-yellow-300', 'bg-gray-100');
                    positionSpan.style.fontWeight = 'normal';
                }
            }
        });

        // Update the floating save indicator
        this.updateSaveIndicator();
    },

    // Show floating save indicator
    updateSaveIndicator() {
        const existingIndicator = document.getElementById('saveIndicator');
        if (existingIndicator) {
            existingIndicator.remove();
        }

        if (!this.hasUnsavedChanges || !this.dragDropEnabled) return;

        const changedCount = this.getChangedItemsCount();
        if (changedCount === 0) return;

        const indicator = document.createElement('div');
        indicator.id = 'saveIndicator';
        indicator.className = 'fixed bottom-4 right-4 bg-orange-500 text-white px-4 py-3 rounded-lg shadow-lg z-50 flex items-center gap-3';

        indicator.innerHTML = `
            <i class="fas fa-exclamation-triangle"></i>
            <div>
                <div class="font-medium">${changedCount} item${changedCount !== 1 ? 's' : ''} moved</div>
                <div class="text-sm opacity-90">Click "Save Order" to apply changes</div>
            </div>
            <button onclick="RapidStorApp.saveOrder()" 
                    class="bg-white text-orange-500 px-3 py-1 rounded text-sm font-medium hover:bg-gray-100">
                Save Now
            </button>
        `;

        document.body.appendChild(indicator);
    },

    // Count how many items have changed position
    getChangedItemsCount() {
        const currentRows = document.querySelectorAll('#sortableTable .sortable-item');
        let changedCount = 0;

        currentRows.forEach((row, index) => {
            const currentPosition = index + 1;
            const originalPosition = this.originalOrder.find(item => item.id === row.dataset.id)?.originalPosition;

            if (originalPosition && originalPosition !== currentPosition) {
                changedCount++;
            }
        });

        return changedCount;
    },

    // Show save confirmation dialog
    showSaveConfirmation() {
        const changedCount = this.getChangedItemsCount();

        if (changedCount === 0) {
            this.showToast('No changes to save', 'info');
            this.cancelDragDrop();
            return;
        }

        // Create confirmation modal
        const modal = document.createElement('div');
        modal.id = 'saveConfirmModal';
        modal.className = 'fixed inset-0 bg-gray-600 bg-opacity-50 z-50 flex items-center justify-center px-4';

        modal.innerHTML = `
            <div class="bg-white rounded-lg shadow-xl max-w-md w-full p-6">
                <div class="flex items-center gap-3 mb-4">
                    <i class="fas fa-save text-orange-500 text-xl"></i>
                    <h3 class="text-lg font-semibold">Save Order Changes?</h3>
                </div>
                
                <div class="mb-6">
                    <p class="text-gray-700 mb-3">You have moved <strong>${changedCount}</strong> descriptor${changedCount !== 1 ? 's' : ''}.</p>
                    <p class="text-sm text-gray-600">This will update the ordinal positions in the database.</p>
                </div>
                
                <div class="flex gap-3 justify-end">
                    <button onclick="RapidStorApp.cancelSaveConfirmation()" 
                            class="px-4 py-2 text-gray-600 border border-gray-300 rounded hover:bg-gray-50">
                        Continue Editing
                    </button>
                    <button onclick="RapidStorApp.resetOrder()" 
                            class="px-4 py-2 text-red-600 border border-red-300 rounded hover:bg-red-50">
                        Reset Changes
                    </button>
                    <button onclick="RapidStorApp.confirmSaveOrder()" 
                            class="px-4 py-2 bg-orange-600 text-white rounded hover:bg-orange-700">
                        Save Order
                    </button>
                </div>
            </div>
        `;

        document.body.appendChild(modal);

        // Close on outside click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                this.cancelSaveConfirmation();
            }
        });
    },

    // Cancel save confirmation
    cancelSaveConfirmation() {
        const modal = document.getElementById('saveConfirmModal');
        if (modal) {
            modal.remove();
        }
    },

    // Confirm and save the order
    confirmSaveOrder() {
        this.cancelSaveConfirmation();
        this.saveOrder();
    },

    // Reset to original order
    resetOrder() {
        this.cancelSaveConfirmation();

        // Restore original order
        const sortableTable = document.getElementById('sortableTable');
        if (sortableTable && this.originalOrder.length > 0) {
            // Sort elements back to original order
            this.originalOrder
                .sort((a, b) => a.originalPosition - b.originalPosition)
                .forEach(item => {
                    sortableTable.appendChild(item.element);
                });
        }

        this.hasUnsavedChanges = false;
        this.updatePositionNumbers();
        this.updateDragDropUI();
        this.removeSaveIndicator();

        this.showToast('Order reset to original', 'info');
    },

    // Save the current order
    saveOrder() {
        const rows = document.querySelectorAll('#sortableTable .sortable-item');
        const orderedIds = Array.from(rows).map(row => row.dataset.id);

        console.log('Saving reordered descriptors:', orderedIds.length, 'items');

        // Show loading state
        const button = document.getElementById('dragToggle');
        const originalText = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Saving...';
        button.disabled = true;

        // Remove save indicator
        this.removeSaveIndicator();

        // Show progress indicator
        this.showProgressToast('Saving order changes...', orderedIds.length);

        const formData = new FormData();
        formData.append('action', 'reorder_descriptors');
        formData.append('ordered_ids', JSON.stringify(orderedIds));

        // Increase timeout for large operations
        const timeoutDuration = Math.max(30000, orderedIds.length * 500);
        const controller = new AbortController();
        const timeoutId = setTimeout(() => {
            console.warn(`Save request timed out after ${timeoutDuration/1000} seconds`);
            controller.abort();
        }, timeoutDuration);

        fetch(window.location.href, {
            method: 'POST',
            body: formData,
            signal: controller.signal
        })
            .then(response => {
                clearTimeout(timeoutId);

                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                return response.text().then(text => {
                    try {
                        return JSON.parse(text);
                    } catch (e) {
                        console.error('Failed to parse JSON response:', text.substring(0, 500));
                        throw new Error('Invalid JSON response from server');
                    }
                });
            })
            .then(data => {
                button.innerHTML = originalText;
                button.disabled = false;
                this.hideProgressToast();

                if (data.success) {
                    const updatedCount = data.updated_count || 0;
                    const totalAttempted = data.total_attempted || orderedIds.length;

                    if (updatedCount === totalAttempted) {
                        this.showToast(`Order saved successfully (${updatedCount} descriptors)`, 'success');
                    } else if (updatedCount > 0) {
                        this.showToast(`Partially saved: ${updatedCount}/${totalAttempted} descriptors`, 'warning');
                    } else {
                        this.showToast('No changes were needed - descriptors already in correct order', 'info');
                    }

                    // Successfully saved - exit drag mode
                    this.completeDragDrop();

                    // Show any errors that occurred
                    if (data.errors && data.errors.length > 0) {
                        console.warn('Some errors occurred during save:', data.errors);
                        this.showDetailedErrors(data.errors);
                    }
                } else {
                    this.showToast('Failed to save order: ' + (data.error || 'Unknown error'), 'error');
                    console.error('Order save failed:', data);

                    // Reset button state
                    this.updateDragDropUI();
                }
            })
            .catch(error => {
                clearTimeout(timeoutId);
                button.innerHTML = originalText;
                button.disabled = false;
                this.hideProgressToast();
                this.updateDragDropUI();

                console.error('Save error:', error);

                if (error.name === 'AbortError') {
                    this.showToast('Save timed out. The operation may still be processing. Please refresh to see the latest order.', 'warning', 8000);
                } else if (error.message.includes('Invalid JSON')) {
                    this.showToast('Server returned invalid response. Please try again or refresh the page.', 'error');
                } else {
                    this.showToast('Network error: ' + error.message, 'error');
                }
            });
    },

    // Cancel drag and drop mode
    cancelDragDrop() {
        if (this.hasUnsavedChanges) {
            const changedCount = this.getChangedItemsCount();
            if (changedCount > 0) {
                if (!confirm(`You have ${changedCount} unsaved changes. Are you sure you want to cancel?`)) {
                    return;
                }
            }
        }

        this.completeDragDrop();
        this.showToast('Drag and drop cancelled', 'info');
    },

    // Complete drag and drop (save successful or cancelled)
    completeDragDrop() {
        if (this.sortableInstance) {
            this.sortableInstance.destroy();
            this.sortableInstance = null;
        }

        this.dragDropEnabled = false;
        this.hasUnsavedChanges = false;
        this.originalOrder = [];

        this.updateDragDropUI();
        this.removeSaveIndicator();
        this.resetPositionHighlights();
    },

    // Remove save indicator
    removeSaveIndicator() {
        const indicator = document.getElementById('saveIndicator');
        if (indicator) {
            indicator.remove();
        }
    },

    // Reset position number highlights
    resetPositionHighlights() {
        const positionSpans = document.querySelectorAll('.bg-gray-100 span, .font-mono');
        positionSpans.forEach(span => {
            span.className = span.className.replace('bg-yellow-100 border-yellow-300', 'bg-gray-100');
            span.style.fontWeight = 'normal';
        });
    },

    // Override the original updateOrder to prevent automatic saving
    updateOrder() {
        // This method is now intentionally empty to prevent automatic saving
        // The actual saving is handled by saveOrder() method
        console.log('updateOrder called - using manual save mode');
    }
};

// Merge the enhancements into RapidStorApp
Object.assign(RapidStorApp, DragDropEnhancements);

// Add keyboard shortcut for saving
document.addEventListener('keydown', (e) => {
    // Ctrl/Cmd + S to save order
    if ((e.ctrlKey || e.metaKey) && e.key === 's' && RapidStorApp.dragDropEnabled && RapidStorApp.hasUnsavedChanges) {
        e.preventDefault();
        RapidStorApp.saveOrder();
    }
});

// Add CSS for the visual enhancements
const style = document.createElement('style');
style.textContent = `
    .bg-yellow-100 {
        background-color: #fef3c7 !important;
        border: 1px solid #f59e0b !important;
    }
    
    .animate-pulse {
        animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite;
    }
    
    @keyframes pulse {
        0%, 100% {
            opacity: 1;
        }
        50% {
            opacity: .7;
        }
    }
`;
document.head.appendChild(style);

console.log('Manual save drag & drop loaded');