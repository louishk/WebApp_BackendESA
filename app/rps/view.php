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

<!-- JavaScript -->
<script src="js/app.js"></script>
<script>
    // Make data available to JavaScript
    const descriptors = <?= json_encode($data['descriptors']) ?>;
    const dealsLookup = <?= json_encode($data['lookups']['deals']) ?>;
    const insuranceLookup = <?= json_encode($data['lookups']['insurance']) ?>;
    const unitTypesLookup = <?= json_encode($data['lookups']['unitTypes']) ?>;
    const appStats = <?= json_encode($data['stats']) ?>;

    // Initialize application
    document.addEventListener('DOMContentLoaded', function() {
        RapidStorApp.init();
    });
</script>

</body>
</html>