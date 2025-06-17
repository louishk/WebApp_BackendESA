<?php
// includes/group_modal.php - Modal for creating descriptor groups
?>

<div id="groupModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-md w-full transform transition-all">
            <div class="flex justify-between items-center px-6 py-4 border-b border-gray-200">
                <h3 class="text-lg font-semibold text-gray-900 flex items-center gap-2">
                    <i class="fas fa-layer-group text-indigo-600"></i>
                    Create Descriptor Group
                </h3>
                <button onclick="closeGroupModal()" class="text-gray-400 hover:text-gray-600 transition-colors">
                    <i class="fas fa-times"></i>
                </button>
            </div>

            <div class="px-6 py-4">
                <div class="mb-4">
                    <label class="block text-sm font-medium text-gray-700 mb-2">Group Name</label>
                    <input type="text" id="groupName" placeholder="e.g., 8-10 sq ft, Premium Units, etc."
                           class="w-full border border-gray-300 rounded-md px-3 py-2 text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500">
                    <p class="text-sm text-gray-500 mt-2">
                        Selected descriptors will be organized under this group name.
                    </p>
                </div>

                <!-- Group Preview -->
                <div class="mb-4 p-3 bg-gray-50 rounded-lg">
                    <div class="text-sm font-medium text-gray-700 mb-2">Selected Descriptors:</div>
                    <div id="selectedDescriptorsList" class="text-sm text-gray-600 max-h-32 overflow-y-auto">
                        <!-- Will be populated by JavaScript -->
                    </div>
                </div>

                <!-- Group Options -->
                <div class="mb-4">
                    <label class="block text-sm font-medium text-gray-700 mb-2">Group Options</label>
                    <div class="space-y-2">
                        <label class="flex items-center gap-2">
                            <input type="checkbox" id="groupAutoSort" checked
                                   class="w-4 h-4 text-indigo-600 bg-gray-100 border-gray-300 rounded focus:ring-indigo-500">
                            <span class="text-sm text-gray-700">Auto-sort descriptors within group</span>
                        </label>
                        <label class="flex items-center gap-2">
                            <input type="checkbox" id="groupCollapsible" checked
                                   class="w-4 h-4 text-indigo-600 bg-gray-100 border-gray-300 rounded focus:ring-indigo-500">
                            <span class="text-sm text-gray-700">Make group collapsible</span>
                        </label>
                        <label class="flex items-center gap-2">
                            <input type="checkbox" id="groupShowStats" checked
                                   class="w-4 h-4 text-indigo-600 bg-gray-100 border-gray-300 rounded focus:ring-indigo-500">
                            <span class="text-sm text-gray-700">Show group statistics</span>
                        </label>
                    </div>
                </div>
            </div>

            <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
                <button onclick="closeGroupModal()"
                        class="px-4 py-2 text-gray-600 hover:text-gray-800 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors">
                    Cancel
                </button>
                <button onclick="createGroup()"
                        class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-md transition-colors flex items-center gap-2">
                    <i class="fas fa-layer-group"></i>
                    Create Group
                </button>
            </div>
        </div>
    </div>
</div>

<!-- Success Modal -->
<div id="successModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-sm w-full transform transition-all">
            <div class="px-6 py-4 text-center">
                <div class="mx-auto flex items-center justify-center h-12 w-12 rounded-full bg-green-100 mb-4">
                    <i class="fas fa-check text-green-600 text-xl"></i>
                </div>
                <h3 class="text-lg font-semibold text-gray-900 mb-2">Group Created!</h3>
                <p class="text-sm text-gray-600 mb-4" id="successMessage">
                    Your descriptor group has been created successfully.
                </p>
                <button onclick="closeSuccessModal()"
                        class="w-full bg-green-600 hover:bg-green-700 text-white py-2 px-4 rounded-md transition-colors">
                    Continue
                </button>
            </div>
        </div>
    </div>
</div>

<script>
    // Enhanced group modal functionality
    document.addEventListener('DOMContentLoaded', function() {
        // Update selected descriptors list when modal opens
        const groupModal = document.getElementById('groupModal');
        const observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
                    if (!groupModal.classList.contains('hidden')) {
                        updateSelectedDescriptorsList();
                    }
                }
            });
        });
        observer.observe(groupModal, { attributes: true });
    });

    function updateSelectedDescriptorsList() {
        const selectedList = document.getElementById('selectedDescriptorsList');
        const selectedIds = Array.from(RapidStorApp.selectedIds);

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
    }

    function closeSuccessModal() {
        document.getElementById('successModal').classList.add('hidden');
        location.reload(); // Refresh to show the new grouping
    }

    // Enhanced create group function
    function createGroup() {
        const groupName = document.getElementById('groupName').value.trim();
        if (!groupName) {
            RapidStorApp.showToast('Please enter a group name', 'warning');
            return;
        }

        const selectedArray = Array.from(RapidStorApp.selectedIds);
        if (selectedArray.length === 0) {
            RapidStorApp.showToast('Please select at least one descriptor to group', 'warning');
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
        const createButton = document.querySelector('[onclick="createGroup()"]');
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
                    RapidStorApp.showToast('Failed to create group: ' + (data.error || 'Unknown error'), 'error');
                }
            })
            .catch(error => {
                createButton.innerHTML = originalText;
                createButton.disabled = false;
                RapidStorApp.showToast('Network error: ' + error.message, 'error');
            });
    }

    // Auto-suggest group names based on selected descriptors
    function suggestGroupName() {
        const selectedIds = Array.from(RapidStorApp.selectedIds);
        if (selectedIds.length === 0) return;

        // Extract common patterns from selected descriptor names
        const names = selectedIds.map(id => {
            const row = document.querySelector(`tr[data-id="${id}"]`);
            const nameElement = row?.querySelector('.text-sm.font-medium');
            return nameElement ? nameElement.textContent.trim() : '';
        }).filter(name => name);

        // Look for common size patterns
        const sizePattern = /(\d+(?:\.\d+)?(?:\s*x\s*\d+(?:\.\d+)?)?)\s*(?:sq\s*ft|sqft)/i;
        const sizes = names.map(name => {
            const match = name.match(sizePattern);
            return match ? match[1] : null;
        }).filter(size => size);

        if (sizes.length > 0) {
            const commonSize = sizes[0]; // Use first size as base
            const allSameSizes = sizes.every(size => size === commonSize);

            if (allSameSizes) {
                document.getElementById('groupName').value = `${commonSize} sq ft Units`;
            } else {
                document.getElementById('groupName').value = `Mixed Size Units`;
            }
        } else {
            // Look for common type patterns
            const typePattern = /(premium|regular|standard|deluxe|climate|basic)/i;
            const types = names.map(name => {
                const match = name.match(typePattern);
                return match ? match[1].toLowerCase() : null;
            }).filter(type => type);

            if (types.length > 0) {
                const commonType = types[0];
                const allSameTypes = types.every(type => type === commonType);

                if (allSameTypes) {
                    document.getElementById('groupName').value = `${commonType.charAt(0).toUpperCase() + commonType.slice(1)} Units`;
                }
            }
        }
    }

    // Auto-suggest when modal opens and descriptors are selected
    document.addEventListener('DOMContentLoaded', function() {
        const groupNameInput = document.getElementById('groupName');
        groupNameInput.addEventListener('focus', function() {
            if (!this.value) {
                suggestGroupName();
            }
        });
    });
</script>