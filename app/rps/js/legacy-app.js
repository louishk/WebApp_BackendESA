/**
 * Legacy App - Complete Integration
 * This file consolidates all inline JavaScript from PHP files
 * and provides backward compatibility while transitioning to the new modular structure
 */

// Global RapidStorApp object for backward compatibility
window.RapidStorApp = {
    selectedIds: new Set(),

    // Core Methods
    init() {
        console.log('🚀 Legacy RapidStorApp initialized');
        this.setupEventListeners();
        this.initializeSelectionHandling();
        this.setupDragAndDrop();
        this.updateSelection();
    },

    // Selection Management
    updateSelection() {
        const checkboxes = document.querySelectorAll('.descriptor-checkbox:checked');
        this.selectedIds.clear();

        checkboxes.forEach(checkbox => {
            this.selectedIds.add(checkbox.value);
        });

        this.updateSelectionUI();
        this.updateBatchActions();
    },

    updateSelectionUI() {
        const selectedCount = document.getElementById('selectedCount');
        if (selectedCount) {
            const count = this.selectedIds.size;
            selectedCount.textContent = `${count} selected`;
        }

        // Update select all checkbox
        const selectAllCheckbox = document.getElementById('selectAll');
        const allCheckboxes = document.querySelectorAll('.descriptor-checkbox');

        if (selectAllCheckbox && allCheckboxes.length > 0) {
            const checkedCount = this.selectedIds.size;
            selectAllCheckbox.checked = checkedCount === allCheckboxes.length;
            selectAllCheckbox.indeterminate = checkedCount > 0 && checkedCount < allCheckboxes.length;
        }
    },

    updateBatchActions() {
        const batchActionsContainer = document.getElementById('batchActions');
        if (batchActionsContainer) {
            batchActionsContainer.style.display = this.selectedIds.size > 0 ? 'block' : 'none';
        }
    },

    // Batch Actions
    batchAction(action) {
        if (this.selectedIds.size === 0) {
            this.showToast('Please select at least one descriptor', 'warning');
            return;
        }

        const selectedArray = Array.from(this.selectedIds);
        const confirmMessage = this.getBatchConfirmMessage(action, selectedArray.length);

        if (confirmMessage && !confirm(confirmMessage)) {
            return;
        }

        this.showLoadingOverlay(`Processing ${action}...`);

        const formData = new FormData();
        formData.append('action', 'batch_update');
        formData.append('operation', action);
        formData.append('selected_ids', JSON.stringify(selectedArray));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                this.hideLoadingOverlay();

                if (data.success) {
                    this.showToast(data.message || `${action} completed successfully`, 'success');
                    this.selectedIds.clear();
                    this.updateSelection();
                    setTimeout(() => window.location.reload(), 1500);
                } else {
                    this.showToast(`Failed to ${action}: ${data.error}`, 'error');
                }
            })
            .catch(error => {
                this.hideLoadingOverlay();
                this.showToast(`Network error: ${error.message}`, 'error');
            });
    },

    getBatchConfirmMessage(action, count) {
        const descriptor = count === 1 ? 'descriptor' : 'descriptors';

        switch (action) {
            case 'delete':
                return `Are you sure you want to delete ${count} ${descriptor}? This cannot be undone.`;
            case 'enable':
                return `Enable ${count} ${descriptor}?`;
            case 'disable':
                return `Disable ${count} ${descriptor}?`;
            case 'show':
                return `Make ${count} ${descriptor} visible?`;
            case 'hide':
                return `Hide ${count} ${descriptor}?`;
            default:
                return null;
        }
    },

    // Export Functions
    exportData(format) {
        if (this.selectedIds.size === 0) {
            this.showToast('No descriptors selected for export', 'warning');
            return;
        }

        this.showLoadingOverlay('Preparing export...');

        const formData = new FormData();
        formData.append('action', 'export_descriptors');
        formData.append('format', format);
        formData.append('descriptor_ids', JSON.stringify(Array.from(this.selectedIds)));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => {
                if (response.ok) {
                    return response.blob();
                }
                throw new Error('Export failed');
            })
            .then(blob => {
                this.hideLoadingOverlay();
                this.downloadBlob(blob, `descriptors_export.${format}`);
                this.showToast(`Export completed (${format.toUpperCase()})`, 'success');
            })
            .catch(error => {
                this.hideLoadingOverlay();
                this.showToast(`Export failed: ${error.message}`, 'error');
            });
    },

    // Quick Toggle Functions
    quickToggle(descriptorId, field, value) {
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
                    this.updateToggleUI(descriptorId, field, value);
                    this.showToast('Updated successfully', 'success');
                } else {
                    this.showToast(`Failed to update: ${data.error}`, 'error');
                    // Revert the toggle
                    this.revertToggleUI(descriptorId, field, value);
                }
            })
            .catch(error => {
                this.showToast(`Network error: ${error.message}`, 'error');
                this.revertToggleUI(descriptorId, field, value);
            });
    },

    updateToggleUI(descriptorId, field, value) {
        const row = document.querySelector(`tr[data-id="${descriptorId}"]`);
        if (!row) return;

        const toggles = row.querySelectorAll('.status-toggle');
        const fieldIndex = { enabled: 0, hidden: 1, useForCarousel: 2 };
        const toggle = toggles[fieldIndex[field]];

        if (toggle) {
            const actualValue = field === 'hidden' ? !value : value;
            toggle.checked = actualValue;

            const label = toggle.closest('label');
            if (label) {
                this.updateToggleVisualState(label, field, actualValue);
            }
        }
    },

    updateToggleVisualState(label, field, value) {
        const switchDiv = label.querySelector('div');
        const switchButton = switchDiv?.querySelector('span');
        const statusText = label.querySelector('span:last-child');

        if (!switchDiv || !switchButton || !statusText) return;

        const colorMap = { enabled: 'green', hidden: 'blue', useForCarousel: 'purple' };
        const textMap = {
            enabled: value ? 'Enabled' : 'Disabled',
            hidden: value ? 'Visible' : 'Hidden',
            useForCarousel: 'Carousel'
        };

        const colorClass = colorMap[field] || 'gray';

        // Update switch background
        switchDiv.className = switchDiv.className.replace(/bg-\w+-\d+/g, '');
        switchDiv.classList.add(value ? `bg-${colorClass}-600` : 'bg-gray-200');

        // Update switch button position
        switchButton.className = switchButton.className.replace(/translate-x-\d+/g, '');
        switchButton.classList.add(value ? 'translate-x-5' : 'translate-x-1');

        // Update status text
        statusText.textContent = textMap[field];
        statusText.className = `text-xs ${value ? `text-${colorClass}-600 font-medium` : 'text-gray-500'}`;
    },

    revertToggleUI(descriptorId, field, value) {
        // Revert to previous state
        this.updateToggleUI(descriptorId, field, !value);
    },

    // Delete and Duplicate Functions
    deleteDescriptor(descriptorId, descriptorName) {
        if (!confirm(`Are you sure you want to delete "${descriptorName}"? This cannot be undone.`)) {
            return;
        }

        this.showLoadingOverlay('Deleting descriptor...');

        const formData = new FormData();
        formData.append('action', 'delete_descriptor');
        formData.append('descriptor_id', descriptorId);

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                this.hideLoadingOverlay();

                if (data.success) {
                    this.showToast('Descriptor deleted successfully', 'success');
                    // Remove row from DOM
                    const row = document.querySelector(`tr[data-id="${descriptorId}"]`);
                    if (row) row.remove();
                } else {
                    this.showToast(`Failed to delete: ${data.error}`, 'error');
                }
            })
            .catch(error => {
                this.hideLoadingOverlay();
                this.showToast(`Network error: ${error.message}`, 'error');
            });
    },

    duplicateDescriptor(descriptorId, descriptorName) {
        this.showLoadingOverlay('Duplicating descriptor...');

        const formData = new FormData();
        formData.append('action', 'duplicate_descriptor');
        formData.append('descriptor_id', descriptorId);

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                this.hideLoadingOverlay();

                if (data.success) {
                    this.showToast('Descriptor duplicated successfully', 'success');
                    setTimeout(() => window.location.reload(), 1000);
                } else {
                    this.showToast(`Failed to duplicate: ${data.error}`, 'error');
                }
            })
            .catch(error => {
                this.hideLoadingOverlay();
                this.showToast(`Network error: ${error.message}`, 'error');
            });
    },

    // Drag and Drop
    setupDragAndDrop() {
        this.dragDropEnabled = false;
        this.sortableInstance = null;
        this.hasUnsavedChanges = false;
    },

    enableDragDrop() {
        const sortableTable = document.getElementById('sortableTable');
        if (!sortableTable || this.dragDropEnabled) return;

        if (window.Sortable) {
            this.sortableInstance = Sortable.create(sortableTable, {
                handle: '.drag-handle',
                animation: 150,
                ghostClass: 'sortable-ghost',
                chosenClass: 'sortable-chosen',
                dragClass: 'sortable-drag',
                onEnd: (evt) => {
                    if (evt.oldIndex !== evt.newIndex) {
                        this.hasUnsavedChanges = true;
                        this.updateDragDropUI();
                    }
                }
            });

            this.dragDropEnabled = true;
            this.updateDragDropUI();
            this.showToast('Drag & Drop enabled. Use the grip handles to reorder.', 'info');
        } else {
            this.showToast('Sortable library not loaded', 'error');
        }
    },

    disableDragDrop() {
        if (this.sortableInstance) {
            this.sortableInstance.destroy();
            this.sortableInstance = null;
        }

        this.dragDropEnabled = false;
        this.hasUnsavedChanges = false;
        this.updateDragDropUI();
        this.showToast('Drag & Drop disabled', 'info');
    },

    updateDragDropUI() {
        const dragToggle = document.getElementById('dragToggle');
        if (dragToggle) {
            if (this.dragDropEnabled) {
                dragToggle.innerHTML = '<i class="fas fa-times mr-1"></i>Disable Drag & Drop';
                dragToggle.onclick = () => this.disableDragDrop();

                if (this.hasUnsavedChanges) {
                    dragToggle.insertAdjacentHTML('afterend',
                        '<button onclick="RapidStorApp.saveDragOrder()" class="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm ml-2"><i class="fas fa-save mr-1"></i>Save Order</button>'
                    );
                }
            } else {
                dragToggle.innerHTML = '<i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop';
                dragToggle.onclick = () => this.enableDragDrop();

                const saveButton = dragToggle.nextElementSibling;
                if (saveButton && saveButton.textContent.includes('Save Order')) {
                    saveButton.remove();
                }
            }
        }
    },

    saveDragOrder() {
        const sortableTable = document.getElementById('sortableTable');
        if (!sortableTable) return;

        const orderedIds = Array.from(sortableTable.children).map(row => row.dataset.id);

        this.showLoadingOverlay('Saving new order...');

        const formData = new FormData();
        formData.append('action', 'reorder_descriptors');
        formData.append('ordered_ids', JSON.stringify(orderedIds));

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                this.hideLoadingOverlay();

                if (data.success) {
                    this.showToast('Order saved successfully', 'success');
                    this.hasUnsavedChanges = false;
                    this.updateDragDropUI();
                } else {
                    this.showToast(`Failed to save order: ${data.error}`, 'error');
                }
            })
            .catch(error => {
                this.hideLoadingOverlay();
                this.showToast(`Network error: ${error.message}`, 'error');
            });
    },

    // UI Helper Functions
    showToast(message, type = 'info', duration = 5000) {
        const toast = document.createElement('div');
        toast.className = `fixed top-4 right-4 px-4 py-3 rounded-lg shadow-lg z-50 transform transition-all duration-300 ${this.getToastClass(type)}`;
        toast.innerHTML = `
            <div class="flex items-center gap-2">
                <i class="fas ${this.getToastIcon(type)}"></i>
                <span>${message}</span>
                <button onclick="this.parentElement.parentElement.remove()" class="ml-2 text-white hover:text-gray-200">
                    <i class="fas fa-times"></i>
                </button>
            </div>
        `;

        document.body.appendChild(toast);

        setTimeout(() => {
            if (toast.parentElement) {
                toast.style.transform = 'translateX(100%)';
                setTimeout(() => toast.remove(), 300);
            }
        }, duration);
    },

    getToastClass(type) {
        const classes = {
            success: 'bg-green-600 text-white',
            error: 'bg-red-600 text-white',
            warning: 'bg-yellow-600 text-white',
            info: 'bg-blue-600 text-white'
        };
        return classes[type] || classes.info;
    },

    getToastIcon(type) {
        const icons = {
            success: 'fa-check-circle',
            error: 'fa-exclamation-circle',
            warning: 'fa-exclamation-triangle',
            info: 'fa-info-circle'
        };
        return icons[type] || icons.info;
    },

    showLoadingOverlay(message = 'Loading...') {
        this.hideLoadingOverlay();

        const overlay = document.createElement('div');
        overlay.id = 'loadingOverlay';
        overlay.className = 'fixed inset-0 bg-gray-900 bg-opacity-50 flex items-center justify-center z-50';
        overlay.innerHTML = `
            <div class="bg-white rounded-lg p-6 flex items-center gap-3 shadow-xl">
                <div class="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600"></div>
                <span class="text-gray-700 font-medium">${message}</span>
            </div>
        `;

        document.body.appendChild(overlay);
    },

    hideLoadingOverlay() {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) {
            overlay.remove();
        }
    },

    downloadBlob(blob, filename) {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    },

    // Event Listeners Setup
    setupEventListeners() {
        // Handle form submissions
        document.addEventListener('submit', (e) => {
            if (e.target.id === 'descriptorForm') {
                const nameInput = e.target.querySelector('#name');
                if (!nameInput || !nameInput.value.trim()) {
                    e.preventDefault();
                    this.showToast('Name is required', 'error');
                    if (nameInput) nameInput.focus();
                    return false;
                } else {
                    this.showLoadingOverlay('Saving descriptor...');
                }
            }
        });

        // Keyboard shortcuts
        document.addEventListener('keydown', (e) => {
            // Don't trigger shortcuts if user is typing
            if (this.isInputFocused()) return;

            // Ctrl/Cmd + A to select all
            if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
                e.preventDefault();
                this.selectAll();
            }

            // Escape to clear selection
            if (e.key === 'Escape') {
                this.clearSelection();
            }

            // Delete key to delete selected
            if (e.key === 'Delete' && this.selectedIds.size > 0) {
                e.preventDefault();
                this.batchAction('delete');
            }
        });

        // Beforeunload warning for unsaved changes
        window.addEventListener('beforeunload', (e) => {
            if (this.hasUnsavedChanges) {
                e.preventDefault();
                e.returnValue = 'You have unsaved changes. Are you sure you want to leave?';
                return e.returnValue;
            }
        });
    },

    initializeSelectionHandling() {
        // Set up checkbox event listeners
        document.addEventListener('change', (e) => {
            if (e.target.classList.contains('descriptor-checkbox')) {
                this.updateSelection();
            }
        });
    },

    isInputFocused() {
        const activeElement = document.activeElement;
        return activeElement && (
            activeElement.tagName === 'INPUT' ||
            activeElement.tagName === 'TEXTAREA' ||
            activeElement.tagName === 'SELECT' ||
            activeElement.contentEditable === 'true'
        );
    },

    selectAll() {
        const selectAllCheckbox = document.getElementById('selectAll');
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');

        if (selectAllCheckbox) {
            const shouldCheck = !selectAllCheckbox.checked;
            selectAllCheckbox.checked = shouldCheck;

            checkboxes.forEach(checkbox => {
                checkbox.checked = shouldCheck;
            });

            this.updateSelection();
        }
    },

    clearSelection() {
        const selectAllCheckbox = document.getElementById('selectAll');
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');

        if (selectAllCheckbox) selectAllCheckbox.checked = false;
        checkboxes.forEach(checkbox => checkbox.checked = false);

        this.updateSelection();
    }
};

// Global functions for backward compatibility with existing PHP inline scripts
window.updateSelection = () => RapidStorApp.updateSelection();
window.toggleSelectAll = () => RapidStorApp.selectAll();
window.quickToggle = (id, field, value) => RapidStorApp.quickToggle(id, field, value);
window.deleteDescriptor = (id, name) => RapidStorApp.deleteDescriptor(id, name);
window.duplicateDescriptor = (id, name) => RapidStorApp.duplicateDescriptor(id, name);
window.enableDragDrop = () => RapidStorApp.enableDragDrop();
window.batchAction = (action) => RapidStorApp.batchAction(action);
window.groupSelected = () => {
    if (RapidStorApp.selectedIds.size === 0) {
        RapidStorApp.showToast('Please select descriptors first', 'warning');
        return;
    }
    document.getElementById('groupModal').classList.remove('hidden');
};

// Smart Carousel Toggle - from header.php
window.smartCarouselToggle = () => {
    if (!confirm('This will:\n• Turn OFF carousel for fully occupied units (100%)\n• Turn ON carousel for available units (<100%)\n\nContinue?')) {
        return;
    }

    RapidStorApp.showLoadingOverlay('Processing smart carousel...');

    const formData = new FormData();
    formData.append('action', 'smart_carousel_off');

    fetch(window.location.href, {
        method: 'POST',
        body: formData
    })
        .then(response => response.json())
        .then(data => {
            RapidStorApp.hideLoadingOverlay();

            if (data.success) {
                RapidStorApp.showToast(data.message, 'success', 4000);

                if (data.updated_count > 0) {
                    setTimeout(() => {
                        window.location.reload(true);
                    }, 2000);
                }
            } else {
                RapidStorApp.showToast(data.error || 'Failed to update carousel settings', 'error');
            }
        })
        .catch(error => {
            RapidStorApp.hideLoadingOverlay();
            RapidStorApp.showToast('Error: ' + error.message, 'error');
        });
};

// Bulk Toggle Functions - from header.php
window.bulkToggle = (field, value) => {
    const action = field === 'enabled' ? (value ? 'enable' : 'disable') : (value ? 'show' : 'hide');

    // Select all first
    const checkboxes = document.querySelectorAll('.descriptor-checkbox');
    checkboxes.forEach(checkbox => checkbox.checked = true);
    RapidStorApp.updateSelection();

    // Then perform batch action
    RapidStorApp.batchAction(action);
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    RapidStorApp.init();
    console.log('✅ Legacy RapidStorApp initialized successfully');
});

console.log('📦 Legacy RapidStorApp script loaded successfully');