<?php
// includes/batch_actions.php - Batch action controls
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

        <!-- Management Actions -->
        <div class="flex items-center gap-1">
            <button onclick="groupSelected()"
                    class="bg-indigo-600 hover:bg-indigo-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Group selected descriptors">
                <i class="fas fa-layer-group text-xs"></i>
                Group
            </button>

            <button onclick="exportSelected()"
                    class="bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm transition-colors flex items-center gap-1"
                    title="Export selected descriptors">
                <i class="fas fa-download text-xs"></i>
                Export
            </button>

            <button onclick="batchAction('delete')"
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

<script>
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
</script>