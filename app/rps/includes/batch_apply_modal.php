<?php
// includes/batch_apply_modal.php - Modal for batch applying insurance/deals
?>

<!-- Batch Apply Modal -->
<div id="batchApplyModal" class="fixed inset-0 bg-gray-600 bg-opacity-50 hidden z-50 modal-backdrop">
    <div class="flex items-center justify-center min-h-screen px-4">
        <div class="bg-white rounded-lg shadow-xl max-w-2xl w-full transform transition-all">
            <div class="flex justify-between items-center px-6 py-4 border-b border-gray-200">
                <h3 id="batchApplyTitle" class="text-lg font-semibold text-gray-900 flex items-center gap-2">
                    <!-- Title will be set by JavaScript -->
                </h3>
                <button data-action="close-batch-apply" class="text-gray-400 hover:text-gray-600 transition-colors">
                    <i class="fas fa-times"></i>
                </button>
            </div>

            <div id="batchApplyContent" class="px-6 py-4">
                <!-- Content will be populated by JavaScript -->
            </div>

            <div class="flex justify-end gap-3 px-6 py-4 border-t border-gray-200 bg-gray-50">
                <button data-action="close-batch-apply"
                        class="px-4 py-2 text-gray-600 hover:text-gray-800 border border-gray-300 rounded-md hover:bg-gray-50 transition-colors">
                    Cancel
                </button>
                <button data-action="execute-batch-apply"
                        class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md transition-colors flex items-center gap-2">
                    <i class="fas fa-check"></i>
                    Apply Selected
                </button>
            </div>
        </div>
    </div>
</div>