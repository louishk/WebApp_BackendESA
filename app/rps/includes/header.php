<?php
// includes/header.php - Enhanced header with .env support and environment status
?>

<!-- Environment Status Alert (show if no .env file or missing JWT token) -->
<?php if (!Config::isEnvFileLoaded() || !Config::getJwtToken()): ?>
    <div class="mb-6 p-4 bg-amber-50 border border-amber-200 rounded-lg">
        <div class="flex items-start gap-3">
            <i class="fas fa-exclamation-triangle text-amber-600 mt-1"></i>
            <div class="flex-1">
                <h3 class="text-lg font-semibold text-amber-800 mb-2">Environment Configuration</h3>

                <?php if (!Config::isEnvFileLoaded()): ?>
                    <div class="mb-3">
                        <p class="text-amber-700 mb-2">No .env file found. The application is using default configuration.</p>
                        <form method="post" class="inline">
                            <input type="hidden" name="action" value="create_env_file">
                            <button type="submit" class="bg-amber-600 hover:bg-amber-700 text-white px-4 py-2 rounded-md text-sm">
                                <i class="fas fa-file-plus mr-2"></i>Create .env File
                            </button>
                        </form>
                        <span class="text-amber-600 text-sm ml-3">This will create a sample .env file that you can customize.</span>
                    </div>
                <?php endif; ?>

                <?php if (!Config::getJwtToken()): ?>
                    <div class="mb-3">
                        <p class="text-amber-700 mb-2">JWT token not configured. Please set it in your .env file or enter it below:</p>
                        <form method="post" class="flex gap-2">
                            <input type="text" name="jwt_token" placeholder="Enter your JWT token..."
                                   value="<?= htmlspecialchars($jwtToken) ?>"
                                   class="flex-1 border border-amber-300 rounded-md px-3 py-2 text-sm max-w-md">
                            <button type="submit" class="bg-amber-600 hover:bg-amber-700 text-white px-4 py-2 rounded-md">
                                <i class="fas fa-key mr-2"></i>Set Token
                            </button>
                        </form>
                    </div>
                <?php endif; ?>

                <?php if (Config::isEnvFileLoaded()): ?>
                    <div class="text-sm text-amber-600">
                        <i class="fas fa-info-circle mr-1"></i>
                        Environment loaded from: <code class="bg-amber-100 px-1 rounded"><?= htmlspecialchars(Config::getEnvFilePath()) ?></code>
                    </div>
                <?php endif; ?>
            </div>
        </div>
    </div>
<?php endif; ?>

<!-- JWT Token Input (show if not authenticated and no token in env) -->
<?php if (!$api->hasValidToken() && !Config::getJwtToken()): ?>
    <div class="mb-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg">
        <h3 class="text-lg font-semibold text-yellow-800 mb-2">Authentication Required</h3>
        <p class="text-yellow-700 mb-3">Please enter your JWT token to access the RapidStor API:</p>
        <form method="post" class="flex gap-2">
            <input type="text" name="jwt_token" placeholder="Enter your JWT token..."
                   value="<?= htmlspecialchars($jwtToken) ?>"
                   class="flex-1 border border-yellow-300 rounded-md px-3 py-2 text-sm">
            <button type="submit" class="bg-yellow-600 hover:bg-yellow-700 text-white px-4 py-2 rounded">
                <i class="fas fa-key mr-2"></i>Authenticate
            </button>
        </form>
    </div>
<?php endif; ?>

<div class="flex justify-between items-center mb-4">
    <h1 class="text-3xl font-bold text-gray-900 flex items-center gap-3">
        Enhanced RapidStor Manager
        <?php if (Config::isEnvFileLoaded()): ?>
            <span class="bg-green-100 text-green-800 text-xs px-2 py-1 rounded-full flex items-center gap-1">
                <i class="fas fa-check-circle"></i>
                ENV Loaded
            </span>
        <?php endif; ?>
        <?php if (Config::isDebugMode()): ?>
            <span class="bg-purple-100 text-purple-800 text-xs px-2 py-1 rounded-full flex items-center gap-1">
                <i class="fas fa-bug"></i>
                Debug Mode
            </span>
        <?php endif; ?>
    </h1>
    <div class="flex gap-2">
        <?php if ($api->hasValidToken()): ?>
            <form method="post" class="inline">
                <input type="hidden" name="action" value="login">
                <button type="submit" class="bg-green-600 hover:bg-green-700 text-white px-4 py-2 rounded-lg">
                    <i class="fas fa-sign-in-alt mr-2"></i>Login to RapidStor
                </button>
            </form>
            <a href="?create=1" class="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded-lg inline-flex items-center">
                <i class="fas fa-plus mr-2"></i>Create New
            </a>
            <a href="debug_inventory.php?location=<?= $selectedLocation ?>" target="_blank"
               class="bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded-lg inline-flex items-center">
                <i class="fas fa-bug mr-2"></i>Debug
            </a>

            <!-- Environment Management Dropdown -->
            <div class="relative group">
                <button class="bg-gray-600 hover:bg-gray-700 text-white px-4 py-2 rounded-lg inline-flex items-center">
                    <i class="fas fa-cog mr-2"></i>Config
                    <i class="fas fa-chevron-down ml-1 text-xs"></i>
                </button>
                <div class="absolute right-0 mt-2 w-48 bg-white rounded-md shadow-lg border border-gray-200 z-10 hidden group-hover:block">
                    <div class="py-1">
                        <?php if (Config::isEnvFileLoaded()): ?>
                            <form method="post" class="block">
                                <input type="hidden" name="action" value="reload_env">
                                <button type="submit" class="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
                                    <i class="fas fa-sync mr-2"></i>Reload Environment
                                </button>
                            </form>
                        <?php endif; ?>

                        <a href="?debug=1<?= $selectedLocation ? "&location=$selectedLocation" : '' ?>"
                           class="block px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
                            <i class="fas fa-info-circle mr-2"></i>Environment Info
                        </a>

                        <?php if (!Config::isEnvFileLoaded()): ?>
                            <form method="post" class="block">
                                <input type="hidden" name="action" value="create_env_file">
                                <button type="submit" class="block w-full text-left px-4 py-2 text-sm text-gray-700 hover:bg-gray-100">
                                    <i class="fas fa-file-plus mr-2"></i>Create .env File
                                </button>
                            </form>
                        <?php endif; ?>
                    </div>
                </div>
            </div>

            <form method="post" class="inline">
                <input type="hidden" name="jwt_token" value="">
                <button type="submit" class="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg">
                    <i class="fas fa-sign-out-alt mr-2"></i>Logout
                </button>
            </form>
        <?php endif; ?>
    </div>
</div>

<!-- Enhanced Controls -->
<?php if ($api->hasValidToken()): ?>
    <div class="space-y-4">
        <!-- First row: Location, Search, View Mode -->
        <form method="get" class="flex flex-wrap gap-4 items-center">
            <!-- Location Selector -->
            <div class="flex items-center gap-2">
                <label class="text-sm font-medium text-gray-700">Location:</label>
                <select name="location" onchange="this.form.submit()" class="border border-gray-300 rounded-md px-3 py-2 text-sm">
                    <?php foreach (Config::LOCATIONS as $code => $name): ?>
                        <option value="<?= htmlspecialchars($code) ?>" <?= $selectedLocation === $code ? 'selected' : '' ?>>
                            <?= htmlspecialchars("$code - $name") ?>
                        </option>
                    <?php endforeach; ?>
                </select>
            </div>

            <!-- Search -->
            <div class="flex items-center gap-2 flex-1 min-w-64">
                <i class="fas fa-search text-gray-400"></i>
                <input type="text" name="search" placeholder="Search descriptors..."
                       value="<?= htmlspecialchars($searchTerm) ?>"
                       class="border border-gray-300 rounded-md px-3 py-2 text-sm flex-1">
                <button type="submit" class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-2 rounded text-sm">
                    Search
                </button>
            </div>

            <!-- View Mode Toggle -->
            <div class="flex items-center gap-2">
                <label class="text-sm font-medium text-gray-700">View:</label>
                <select name="view" onchange="this.form.submit()" class="border border-gray-300 rounded-md px-3 py-2 text-sm">
                    <option value="table" <?= $viewMode === 'table' ? 'selected' : '' ?>>Table View</option>
                    <option value="grouped" <?= $viewMode === 'grouped' ? 'selected' : '' ?>>Grouped by Size</option>
                </select>
            </div>

            <!-- Hidden fields to preserve state -->
            <input type="hidden" name="sort" value="<?= htmlspecialchars($sortBy) ?>">
            <input type="hidden" name="order" value="<?= htmlspecialchars($sortOrder) ?>">
        </form>

        <!-- Second row: Sort controls and Quick Actions -->
        <div class="flex justify-between items-center">
            <div class="flex items-center gap-4">
                <span class="text-sm text-gray-600">Sort by:</span>
                <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'ordinalPosition', 'order' => $sortBy === 'ordinalPosition' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>"
                   class="text-sm text-blue-600 hover:text-blue-800">
                    Position <?= $sortBy === 'ordinalPosition' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                </a>
                <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'name', 'order' => $sortBy === 'name' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>"
                   class="text-sm text-blue-600 hover:text-blue-800">
                    Name <?= $sortBy === 'name' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                </a>
                <a href="?<?= http_build_query(array_merge($_GET, ['sort' => 'enabled', 'order' => $sortBy === 'enabled' && $sortOrder === 'asc' ? 'desc' : 'asc'])) ?>"
                   class="text-sm text-blue-600 hover:text-blue-800">
                    Status <?= $sortBy === 'enabled' ? ($sortOrder === 'asc' ? '↑' : '↓') : '' ?>
                </a>
            </div>

            <div class="flex items-center gap-2">
                <button onclick="enableDragDrop()" id="dragToggle" class="bg-purple-600 hover:bg-purple-700 text-white px-3 py-1 rounded text-sm">
                    <i class="fas fa-arrows-alt mr-1"></i>Enable Drag & Drop
                </button>
                <button onclick="smartCarouselToggle()" class="bg-orange-600 hover:bg-orange-700 text-white px-3 py-1 rounded text-sm"
                        title="Auto-manage carousel based on occupancy">
                    <i class="fas fa-sync-alt mr-1"></i>Smart Carousel
                </button>
                <button onclick="bulkToggle('enabled', true)" class="bg-green-600 hover:bg-green-700 text-white px-3 py-1 rounded text-sm">
                    <i class="fas fa-toggle-on mr-1"></i>Enable All
                </button>
                <button onclick="bulkToggle('enabled', false)" class="bg-gray-600 hover:bg-gray-700 text-white px-3 py-1 rounded text-sm">
                    <i class="fas fa-toggle-off mr-1"></i>Disable All
                </button>
            </div>
        </div>

        <!-- Enhanced Stats Dashboard with Occupancy -->
        <?php if (!empty($data['stats'])): ?>
            <div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-4 p-4 bg-gray-50 rounded-lg">
                <div class="text-center">
                    <div class="text-2xl font-bold text-blue-600"><?= $data['stats']['counts']['descriptors'] ?></div>
                    <div class="text-xs text-gray-600">Descriptors</div>
                </div>
                <div class="text-center">
                    <div class="text-2xl font-bold text-green-600"><?= $data['stats']['descriptors']['enabled'] ?></div>
                    <div class="text-xs text-gray-600">Enabled</div>
                </div>
                <div class="text-center">
                    <div class="text-2xl font-bold text-blue-600"><?= $data['stats']['descriptors']['visible'] ?></div>
                    <div class="text-xs text-gray-600">Visible</div>
                </div>
                <div class="text-center">
                    <div class="text-2xl font-bold text-purple-600"><?= $data['stats']['descriptors']['carousel'] ?></div>
                    <div class="text-xs text-gray-600">Carousel</div>
                </div>
                <?php if (!empty($data['stats']['inventory'])): ?>
                    <div class="text-center">
                        <div class="text-2xl font-bold text-orange-600"><?= $data['stats']['inventory']['total_units'] ?></div>
                        <div class="text-xs text-gray-600">Total Units</div>
                    </div>
                    <div class="text-center">
                        <div class="text-2xl font-bold text-red-600"><?= $data['stats']['inventory']['average_occupancy'] ?? 0 ?>%</div>
                        <div class="text-xs text-gray-600">Avg Occupancy</div>
                    </div>
                    <div class="text-center">
                        <div class="text-2xl font-bold text-green-600"><?= $data['stats']['inventory']['average_availability'] ?>%</div>
                        <div class="text-xs text-gray-600">Avg Available</div>
                    </div>
                    <div class="text-center">
                        <div class="text-2xl font-bold text-emerald-600"><?= $data['stats']['descriptors']['with_inventory'] ?></div>
                        <div class="text-xs text-gray-600">With Inventory</div>
                    </div>
                <?php endif; ?>
            </div>

            <!-- Occupancy Alert -->
            <?php if (!empty($data['stats']['inventory']['average_occupancy']) && $data['stats']['inventory']['average_occupancy'] > 85): ?>
                <div class="p-3 bg-red-50 border border-red-200 rounded-lg">
                    <div class="flex items-center gap-2 text-red-800">
                        <i class="fas fa-exclamation-triangle"></i>
                        <span class="font-medium">High Occupancy Alert</span>
                    </div>
                    <p class="text-sm text-red-700 mt-1">
                        Average occupancy is <?= $data['stats']['inventory']['average_occupancy'] ?>%. Consider promoting alternative unit types or reviewing pricing.
                    </p>
                </div>
            <?php elseif (!empty($data['stats']['inventory']['average_occupancy']) && $data['stats']['inventory']['average_occupancy'] < 50): ?>
                <div class="p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
                    <div class="flex items-center gap-2 text-yellow-800">
                        <i class="fas fa-info-circle"></i>
                        <span class="font-medium">Low Occupancy Notice</span>
                    </div>
                    <p class="text-sm text-yellow-700 mt-1">
                        Average occupancy is <?= $data['stats']['inventory']['average_occupancy'] ?>%. Consider promotional campaigns or reviewing unit availability.
                    </p>
                </div>
            <?php endif; ?>
        <?php endif; ?>
    </div>
<?php endif; ?>

<!-- Message -->
<?php if ($message): ?>
    <div class="mt-4 p-3 rounded-md <?= $messageType === 'success' ? 'bg-green-50 text-green-800 border border-green-200' :
        ($messageType === 'error' ? 'bg-red-50 text-red-800 border border-red-200' : 'bg-yellow-50 text-yellow-800 border border-yellow-200') ?>">
        <?= htmlspecialchars($message) ?>
    </div>
<?php endif; ?>

<!-- Enhanced Debug Section with Environment Info -->
<?php if ($debug): ?>
    <div class="mt-4 p-4 bg-gray-100 rounded-md">
        <h4 class="font-semibold mb-2 flex items-center gap-2">
            <i class="fas fa-info-circle text-blue-600"></i>
            Debug Information
        </h4>

        <!-- Environment Status -->
        <div class="mb-4 p-3 bg-white rounded border">
            <h5 class="font-medium mb-2 text-gray-800">Environment Configuration</h5>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
                <div>
                    <strong>Environment File:</strong>
                    <?php if (Config::isEnvFileLoaded()): ?>
                        <span class="text-green-600">✅ Loaded</span>
                        <div class="text-xs text-gray-500 mt-1">
                            Path: <?= htmlspecialchars(Config::getEnvFilePath()) ?>
                        </div>
                    <?php else: ?>
                        <span class="text-red-600">❌ Not found</span>
                        <div class="text-xs text-gray-500 mt-1">Using default configuration</div>
                    <?php endif; ?>
                </div>

                <div>
                    <strong>API Configuration:</strong>
                    <div class="text-xs space-y-1 mt-1">
                        <div>Base URL: <?= htmlspecialchars(Config::getApiBaseUrl()) ?></div>
                        <div>Timeout: <?= Config::getRequestTimeout() ?>s</div>
                        <div>Debug Mode: <?= Config::isDebugMode() ? 'Yes' : 'No' ?></div>
                    </div>
                </div>

                <div>
                    <strong>JWT Token:</strong>
                    <?php if ($api->hasValidToken()): ?>
                        <span class="text-green-600">✅ Valid</span>
                        <div class="text-xs text-gray-500 mt-1">
                            Source: <?= Config::getJwtToken() ? 'Environment' : 'Session' ?>
                        </div>
                    <?php else: ?>
                        <span class="text-red-600">❌ Missing/Invalid</span>
                    <?php endif; ?>
                </div>

                <div>
                    <strong>PHP Settings:</strong>
                    <div class="text-xs space-y-1 mt-1">
                        <div>Memory: <?= ini_get('memory_limit') ?></div>
                        <div>Max Execution: <?= ini_get('max_execution_time') ?>s</div>
                        <div>Error Display: <?= ini_get('display_errors') ? 'On' : 'Off' ?></div>
                    </div>
                </div>
            </div>
        </div>

        <div class="text-sm space-y-2">
            <div><strong>Selected Location:</strong> <?= htmlspecialchars($selectedLocation) ?></div>
            <div><strong>Descriptor Count:</strong> <?= count($data['descriptors']) ?></div>
            <div><strong>Unit Types Count:</strong> <?= count($data['unitTypes']) ?></div>

            <?php if (!empty($data['stats'])): ?>
                <div><strong>App Stats:</strong>
                    <pre class="mt-1 p-2 bg-white rounded text-xs overflow-auto max-h-20"><?= htmlspecialchars(json_encode($data['stats'], JSON_PRETTY_PRINT)) ?></pre>
                </div>
            <?php endif; ?>

            <?php if (Config::isEnvFileLoaded()): ?>
                <div><strong>Environment Variables:</strong>
                    <pre class="mt-1 p-2 bg-white rounded text-xs overflow-auto max-h-32"><?= htmlspecialchars(json_encode(Config::getAllEnvVars(), JSON_PRETTY_PRINT)) ?></pre>
                </div>
            <?php endif; ?>
        </div>

        <!-- Test API Endpoints -->
        <div class="mt-3 space-y-2">
            <h5 class="font-medium">Test API Endpoints:</h5>
            <div class="grid grid-cols-3 gap-2">
                <form method="post" class="inline-block">
                    <input type="hidden" name="action" value="test_unittypes">
                    <button type="submit" class="w-full bg-blue-500 hover:bg-blue-600 text-white px-2 py-1 rounded text-xs">
                        Test Unit Types
                    </button>
                </form>
                <form method="post" class="inline-block">
                    <input type="hidden" name="action" value="test_descriptors">
                    <button type="submit" class="w-full bg-green-500 hover:bg-green-600 text-white px-2 py-1 rounded text-xs">
                        Test Descriptors
                    </button>
                </form>
                <form method="post" class="inline-block">
                    <input type="hidden" name="action" value="test_connection">
                    <button type="submit" class="w-full bg-purple-500 hover:bg-purple-600 text-white px-2 py-1 rounded text-xs">
                        Test Connection
                    </button>
                </form>
            </div>
        </div>
    </div>
<?php endif; ?>

<script>
    // Enhanced smart carousel toggle with environment awareness
    function smartCarouselToggle() {
        if (!confirm('This will:\n• Turn OFF carousel for fully occupied units (100%)\n• Turn ON carousel for available units (<100%)\n\nContinue?')) {
            return;
        }

        // Show loading state
        const button = event.target.closest('button');
        const originalHTML = button.innerHTML;
        button.innerHTML = '<i class="fas fa-spinner fa-spin mr-1"></i>Processing...';
        button.disabled = true;

        // Use the AJAX handler
        const formData = new FormData();
        formData.append('action', 'smart_carousel_off');

        fetch(window.location.href, {
            method: 'POST',
            body: formData
        })
            .then(response => response.json())
            .then(data => {
                button.innerHTML = originalHTML;
                button.disabled = false;

                if (data.success) {
                    // Build detailed message
                    let detailMessage = data.message;

                    if (data.updated_details && data.updated_details.length > 0) {
                        const turnedOff = data.updated_details.filter(d => d.action === 'turned_off');
                        const turnedOn = data.updated_details.filter(d => d.action === 'turned_on');

                        if (turnedOff.length > 0) {
                            detailMessage += '\n\nTurned OFF for:';
                            turnedOff.forEach(d => {
                                detailMessage += `\n• ${d.name} (${d.occupancy}% occupied)`;
                            });
                        }

                        if (turnedOn.length > 0) {
                            detailMessage += '\n\nTurned ON for:';
                            turnedOn.forEach(d => {
                                detailMessage += `\n• ${d.name} (${d.occupancy}% occupied)`;
                            });
                        }
                    }

                    // Show success message
                    if (window.RapidStorApp && window.RapidStorApp.showToast) {
                        window.RapidStorApp.showToast(data.message, 'success', 4000);
                    }

                    // Update UI for specific descriptors
                    if (data.updated_details && data.updated_details.length > 0) {
                        data.updated_details.forEach(detail => {
                            if (window.RapidStorApp && window.RapidStorApp.updateToggleUI) {
                                window.RapidStorApp.updateToggleUI(
                                    detail.id,
                                    'useForCarousel',
                                    detail.action === 'turned_on'
                                );
                            }
                        });
                    }

                    // Show detailed changes in console
                    console.log('Smart Carousel Updates:', data.updated_details);

                    // Reload page after a delay to ensure everything is synced
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
                button.innerHTML = originalHTML;
                button.disabled = false;
                if (window.RapidStorApp && window.RapidStorApp.showToast) {
                    window.RapidStorApp.showToast('Error: ' + error.message, 'error');
                }
                console.error('Full error:', error);
            });
    }

    // Enhanced bulk toggle functions
    function bulkToggle(field, value) {
        const action = field === 'enabled' ? (value ? 'enable' : 'disable') : (value ? 'show' : 'hide');

        // Select all first
        const checkboxes = document.querySelectorAll('.descriptor-checkbox');
        checkboxes.forEach(checkbox => checkbox.checked = true);

        if (window.RapidStorApp && window.RapidStorApp.updateSelection) {
            window.RapidStorApp.updateSelection();
        }

        // Then perform batch action
        setTimeout(() => {
            if (window.RapidStorApp && window.RapidStorApp.batchAction) {
                window.RapidStorApp.batchAction(action);
            }
        }, 100);
    }

    // Environment reload function
    function reloadEnvironment() {
        if (confirm('This will reload the environment configuration from the .env file. Continue?')) {
            const form = document.createElement('form');
            form.method = 'post';
            form.style.display = 'none';

            const input = document.createElement('input');
            input.type = 'hidden';
            input.name = 'action';
            input.value = 'reload_env';

            form.appendChild(input);
            document.body.appendChild(form);
            form.submit();
        }
    }

    // Show/hide debug info toggle
    function toggleDebugInfo() {
        const debugSection = document.querySelector('.bg-gray-100.rounded-md');
        if (debugSection) {
            debugSection.style.display = debugSection.style.display === 'none' ? 'block' : 'none';
        }
    }
</script>