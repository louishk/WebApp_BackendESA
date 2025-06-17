<?php
// includes/header.php - Header section with authentication, controls, and navigation
?>

<!-- JWT Token Input (show if not authenticated) -->
<?php if (!$api->hasValidToken()): ?>
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
    <h1 class="text-3xl font-bold text-gray-900">Enhanced RapidStor Manager</h1>
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

<!-- Debug Section -->
<?php if ($debug): ?>
    <div class="mt-4 p-4 bg-gray-100 rounded-md">
        <h4 class="font-semibold mb-2">Debug Information:</h4>
        <div class="text-sm space-y-2">
            <div><strong>API Base URL:</strong> <?= htmlspecialchars(Config::API_BASE_URL) ?></div>
            <div><strong>JWT Token:</strong>
                <?php if ($api->hasValidToken()): ?>
                    <?= htmlspecialchars(substr($jwtToken, 0, 20)) ?>...
                    <span class="text-green-600">(Valid)</span>
                <?php else: ?>
                    <span class="text-red-600">NOT PROVIDED</span>
                <?php endif; ?>
            </div>
            <div><strong>Selected Location:</strong> <?= htmlspecialchars($selectedLocation) ?></div>
            <div><strong>Descriptor Count:</strong> <?= count($data['descriptors']) ?></div>
            <div><strong>Unit Types Count:</strong> <?= count($data['unitTypes']) ?></div>

            <?php if (!empty($data['stats'])): ?>
                <div><strong>App Stats:</strong>
                    <pre class="mt-1 p-2 bg-white rounded text-xs overflow-auto max-h-20"><?= htmlspecialchars(json_encode($data['stats'], JSON_PRETTY_PRINT)) ?></pre>
                </div>
            <?php endif; ?>

            <?php if (!empty($data['unitTypes'])): ?>
                <div><strong>Sample Unit Type:</strong>
                    <pre class="mt-1 p-2 bg-white rounded text-xs overflow-auto max-h-32"><?= htmlspecialchars(json_encode(array_slice($data['unitTypes'], 0, 1), JSON_PRETTY_PRINT)) ?></pre>
                </div>
            <?php endif; ?>

            <?php if (!empty($data['descriptors'])): ?>
                <div><strong>Sample Descriptor Inventory:</strong>
                    <pre class="mt-1 p-2 bg-white rounded text-xs overflow-auto max-h-32"><?= htmlspecialchars(json_encode([
                            'name' => $data['descriptors'][0]['name'] ?? 'N/A',
                            'inventory' => $data['descriptors'][0]['inventory'] ?? [],
                            'keywords' => $data['descriptors'][0]['inventory']['keywords'] ?? []
                        ], JSON_PRETTY_PRINT)) ?></pre>
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

<div class="mt-2">
    <a href="?debug=1<?= $selectedLocation ? "&location=$selectedLocation" : '' ?><?= $searchTerm ? "&search=" . urlencode($searchTerm) : '' ?><?= $viewMode !== 'table' ? "&view=$viewMode" : '' ?>"
       class="text-xs text-gray-500 hover:text-gray-700">Show Debug Info</a>
</div>