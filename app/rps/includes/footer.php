<?php
// includes/footer.php - Footer with statistics and navigation
?>

<div class="flex justify-between items-center text-sm text-gray-600">
    <div>
        <div class="flex items-center gap-4">
            <span>
                Showing <strong><?= count($data['descriptors']) ?></strong> descriptors
                for location <strong><?= htmlspecialchars($selectedLocation) ?></strong>
                <?php if ($searchTerm): ?>
                    (filtered by "<em><?= htmlspecialchars($searchTerm) ?></em>")
                <?php endif; ?>
            </span>

            <?php if (!empty($data['stats'])): ?>
                <span class="text-gray-400">•</span>
                <span>
                <strong><?= $data['stats']['descriptors']['enabled'] ?></strong> enabled,
                <strong><?= $data['stats']['descriptors']['visible'] ?></strong> visible,
                <strong><?= $data['stats']['descriptors']['carousel'] ?></strong> in carousel
            </span>
            <?php endif; ?>
        </div>

        <div class="text-xs text-gray-500 mt-1 flex items-center gap-4">
            <span>
                Data loaded:
                <?= count($data['deals']) ?> deals,
                <?= count($data['insurance']) ?> insurance options,
                <?= count($data['unitTypes']) ?> unit types
            </span>

            <?php if (!empty($data['stats']['inventory'])): ?>
                <span class="text-gray-400">•</span>
                <span>
                Total inventory:
                <strong><?= $data['stats']['inventory']['total_units'] ?></strong> units,
                <strong><?= $data['stats']['inventory']['average_availability'] ?>%</strong> average availability
            </span>
            <?php endif; ?>
        </div>
    </div>

    <div class="flex items-center gap-4">
        <!-- Quick Actions -->
        <div class="flex items-center gap-2">
            <a href="?" class="text-blue-600 hover:text-blue-800 text-sm">
                <i class="fas fa-sync-alt mr-1"></i>
                Refresh
            </a>

            <button onclick="RapidStorApp.exportData('csv')" class="text-green-600 hover:text-green-800 text-sm">
                <i class="fas fa-download mr-1"></i>
                Export CSV
            </button>

            <?php if ($debug): ?>
                <span class="text-gray-400">•</span>
                <span class="text-orange-600 text-xs">
                <i class="fas fa-bug mr-1"></i>
                Debug Mode
            </span>
            <?php endif; ?>
        </div>

        <!-- Location Info -->
        <div class="text-right">
            <div class="font-medium">
                <?= htmlspecialchars(Config::getLocationName($selectedLocation)) ?>
            </div>
            <div class="text-xs text-gray-500">
                <?= htmlspecialchars($selectedLocation) ?>
            </div>
        </div>
    </div>
</div>

<!-- Performance Info (debug mode) -->
<?php if ($debug && !empty($data['stats'])): ?>
    <div class="mt-3 pt-3 border-t border-gray-200">
        <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-4 text-xs">
            <div class="text-center">
                <div class="font-medium text-gray-700">Load Time</div>
                <div class="text-gray-500" id="loadTime">-</div>
            </div>
            <div class="text-center">
                <div class="font-medium text-gray-700">Memory Usage</div>
                <div class="text-gray-500"><?= round(memory_get_peak_usage() / 1024 / 1024, 1) ?> MB</div>
            </div>
            <div class="text-center">
                <div class="font-medium text-gray-700">DB Queries</div>
                <div class="text-gray-500">-</div>
            </div>
            <div class="text-center">
                <div class="font-medium text-gray-700">API Calls</div>
                <div class="text-gray-500">~<?= count($data['unitTypes']) > 0 ? '4' : '3' ?></div>
            </div>
            <div class="text-center">
                <div class="font-medium text-gray-700">Cache Status</div>
                <div class="text-gray-500">
                    <?php if (function_exists('apcu_enabled') && apcu_enabled()): ?>
                        <span class="text-green-600">APCu</span>
                    <?php elseif (extension_loaded('redis')): ?>
                        <span class="text-blue-600">Redis</span>
                    <?php else: ?>
                        <span class="text-red-600">None</span>
                    <?php endif; ?>
                </div>
            </div>
            <div class="text-center">
                <div class="font-medium text-gray-700">PHP Version</div>
                <div class="text-gray-500"><?= PHP_VERSION ?></div>
            </div>
        </div>
    </div>

    <script>
        // Calculate and display load time
        document.addEventListener('DOMContentLoaded', function() {
            const loadTime = performance.now();
            document.getElementById('loadTime').textContent = Math.round(loadTime) + ' ms';
        });
    </script>
<?php endif; ?>

<!-- Quick Navigation -->
<div class="mt-3 pt-3 border-t border-gray-200">
    <div class="flex justify-between items-center text-xs text-gray-500">
        <div class="flex items-center gap-3">
            <span>Quick Actions:</span>
            <a href="?create=1" class="text-blue-600 hover:text-blue-800">
                <i class="fas fa-plus mr-1"></i>New
            </a>
            <a href="?view=<?= $viewMode === 'table' ? 'grouped' : 'table' ?>" class="text-purple-600 hover:text-purple-800">
                <i class="fas fa-exchange-alt mr-1"></i>Switch to <?= $viewMode === 'table' ? 'Grouped' : 'Table' ?> View
            </a>
            <?php if ($searchTerm): ?>
                <a href="?" class="text-orange-600 hover:text-orange-800">
                    <i class="fas fa-times mr-1"></i>Clear Search
                </a>
            <?php endif; ?>
        </div>

        <div class="flex items-center gap-3">
            <span>Last updated: <?= date('H:i:s') ?></span>
            <span>•</span>
            <span>RapidStor Manager v2.0</span>
        </div>
    </div>
</div>