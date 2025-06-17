
        <?php
require_once __DIR__ . '/../../config.php';

// Handle AJAX requests
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    header('Content-Type: application/json');
    
    $input = json_decode(file_get_contents('php://input'), true);
    $action = $input['action'] ?? '';
    
    try {
        switch ($action) {
            case 'search':
                $result = searchEntities($input['query'], $input['entityType']);
                echo json_encode($result);
                break;
                
            case 'view_contact':
                $result = viewContact($input['id'], $input['include'] ?? '');
                echo json_encode($result);
                break;
                
            case 'view_deal':
                $result = viewDeal($input['id'], $input['include'] ?? '');
                echo json_encode($result);
                break;
                
            case 'update_contact':
                $result = updateContact($input['id'], $input['data']);
                echo json_encode($result);
                break;
                
            case 'update_deal':
                $result = updateDeal($input['id'], $input['data']);
                echo json_encode($result);
                break;
                
            default:
                throw new Exception('Invalid action');
        }
    } catch (Exception $e) {
        http_response_code(500);
        echo json_encode(['error' => $e->getMessage()]);
    }
    exit;
}

// API Functions
function makeApiRequest($endpoint, $method = 'GET', $data = null) {
    global $fssApiKey, $fssApiBaseUrl;
    
    if (empty($fssApiKey)) {
        throw new Exception('FSS_API_KEY not configured');
    }
    
    $url = rtrim($fssApiBaseUrl, '/') . '/' . ltrim($endpoint, '/');
    
    $ch = curl_init();
    curl_setopt_array($ch, [
        CURLOPT_URL => $url,
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_HTTPHEADER => [
            'Authorization: Bearer ' . $fssApiKey,
            'Content-Type: application/json'
        ],
        CURLOPT_CUSTOMREQUEST => $method,
        CURLOPT_SSL_VERIFYPEER => false,
        CURLOPT_TIMEOUT => 30
    ]);
    
    if ($data && in_array($method, ['POST', 'PUT', 'PATCH'])) {
        curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));
    }
    
    $response = curl_exec($ch);
    $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $error = curl_error($ch);
    curl_close($ch);
    
    if ($error) {
        throw new Exception('cURL Error: ' . $error);
    }
    
    if ($httpCode >= 400) {
        throw new Exception('API Error: HTTP ' . $httpCode . ' - ' . $response);
    }
    
    return json_decode($response, true);
}

function searchEntities($query, $entityType) {
    // Use the general search endpoint with include parameter
    $include = $entityType === 'contact' ? 'contact' : 'deal';
    $endpoint = '/search?q=' . urlencode($query) . '&include=' . urlencode($include);
    return makeApiRequest($endpoint);
}

function viewContact($id, $include = '') {
    $endpoint = '/contacts/' . $id;
    if ($include) {
        $endpoint .= '?include=' . urlencode($include);
    }
    return makeApiRequest($endpoint);
}

function viewDeal($id, $include = '') {
    $endpoint = '/deals/' . $id;
    if ($include) {
        $endpoint .= '?include=' . urlencode($include);
    }
    return makeApiRequest($endpoint);
}

function updateContact($id, $data) {
    // Wrap the data in a "contact" key as expected by the API
    $payload = ['contact' => $data];
    return makeApiRequest('/contacts/' . $id, 'PUT', $payload);
}

function updateDeal($id, $data) {
    // Wrap the data in a "deal" key as expected by the API
    $payload = ['deal' => $data];
    return makeApiRequest('/deals/' . $id, 'PUT', $payload);
}

// Handle GET request for main page
$entityType = $_GET['entity'] ?? 'contact';
$searchQuery = $_GET['q'] ?? '';
$searchResults = [];
$allColumns = [];

// Perform initial search if query provided
if (!empty($searchQuery)) {
    try {
        $data = searchEntities($searchQuery, $entityType);
        $searchResults = is_array($data) ? $data : ($data['results'] ?? []);
        if (!empty($searchResults)) {
            $allColumns = array_keys($searchResults[0]);
        }
    } catch (Exception $e) {
        $error = $e->getMessage();
    }
}

// Configuration arrays
$excludedFields = ['id', 'created_at', 'updated_at', 'is_deleted', 'external_id', 'links', 'mcr_id', 'lead_score', 'medium', 'state', 'web_form_ids'];

$editGroups = [
    'Contact Details' => ['first_name', 'last_name', 'display_name', 'email', 'phone_numbers', 'city', 'country', 'job_title', 'mobile_number', 'cf_wechat_id', 'cf_xhs_id'],
    'Marketing' => ['first_campaign', 'first_medium', 'latest_campaign', 'latest_medium', 'first_seen_chat', 'cf_referrer_email', 'cf_referrer_id', 'cf_language', 'cf_gacid', 'cf_gclid', 'cf_csat_mi', 'cf_csat_mo'],
    'Social' => ['facebook', 'linkedin', 'twitter'],
    'Sitelink' => ['cf_unitnames', 'cf_unitids', 'cf_totalarea', 'cf_tenantids', 'cf_siteids', 'cf_nbofsites', 'cf_nbofunits', 'cf_ledgerids', 'cf_kycstatus', 'cf_allmovein_dates', 'cf_allpaid_through_dates', 'cf_autobill', 'cf_firstmovein_date'],
    'Deal' => ['name', 'value', 'stage']
];

function formatField($value) {
    if (is_array($value) && isset($value['name'])) {
        return $value['name'];
    }
    return $value ?: '-';
}

// Pagination
$currentPage = max(1, (int)($_GET['page'] ?? 1));
$itemsPerPage = 10;
$totalItems = count($searchResults);
$totalPages = max(1, ceil($totalItems / $itemsPerPage));
$startIndex = ($currentPage - 1) * $itemsPerPage;
$paginatedResults = array_slice($searchResults, $startIndex, $itemsPerPage);
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Advanced Contact & Deal Search & Edit</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script defer src="https://unpkg.com/alpinejs@3.x.x/dist/cdn.min.js"></script>
    <style>
        [x-cloak] { display: none !important; }
        .loading { opacity: 0.5; pointer-events: none; }
    </style>
</head>
<body class="bg-gray-100 text-gray-900">
<div class="w-full px-4 py-4" x-data="searchPage()">

    <h1 class="text-3xl font-bold mb-4 text-center">Search & Edit: Contacts & Deals</h1>
    
    <?php if (isset($error)): ?>
        <div class="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
            Error: <?= htmlspecialchars($error) ?>
        </div>
    <?php endif; ?>
    
    <!-- Search Form -->
    <section class="mb-4">
        <form method="GET" class="flex flex-col md:flex-row gap-2">
            <select name="entity" class="border border-gray-300 rounded p-2">
                <option value="contact" <?= $entityType === 'contact' ? 'selected' : '' ?>>Contact</option>
                <option value="deal" <?= $entityType === 'deal' ? 'selected' : '' ?>>Deal</option>
            </select>
            <input
                type="text"
                name="q"
                value="<?= htmlspecialchars($searchQuery) ?>"
                placeholder="Search..."
                class="flex-1 border border-gray-300 rounded p-2"
            />
            <button
                type="submit"
                class="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700"
            >
                Search
            </button>
        </form>
    </section>

    <?php if (!empty($searchResults)): ?>
    <!-- Column Selection Menu -->
    <section class="mb-4">
        <div class="relative inline-block text-left" x-data="{ open: false }">
            <button
                @click="open = !open"
                type="button"
                class="inline-flex justify-center rounded-md border border-gray-300 shadow-sm px-4 py-2 bg-white text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
                Select Columns
                <svg class="-mr-1 ml-2 h-5 w-5" xmlns="http://www.w3.org/2000/svg"
                     fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M19 9l-7 7-7-7" />
                </svg>
            </button>
            <div x-show="open" @click.outside="open = false" x-cloak
                 class="absolute left-0 mt-2 w-80 max-h-96 overflow-y-auto rounded-md shadow-lg bg-white ring-1 ring-black ring-opacity-5 z-50">
                <div class="py-2">
                    <div class="px-4 py-2 text-xs font-medium text-gray-500 uppercase tracking-wide border-b">
                        Toggle Columns (<?= count($allColumns) ?> total)
                    </div>
                    <div class="max-h-80 overflow-y-auto">
                        <?php foreach ($allColumns as $col): ?>
                        <label class="flex items-center px-4 py-2 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer">
                            <input type="checkbox" class="mr-3 h-4 w-4 text-blue-600 rounded border-gray-300" value="<?= htmlspecialchars($col) ?>" x-model="visibleColumns" />
                            <span class="flex-1 truncate" title="<?= htmlspecialchars($col) ?>"><?= htmlspecialchars($col) ?></span>
                        </label>
                        <?php endforeach; ?>
                    </div>
                    <div class="border-t px-4 py-2">
                        <div class="flex gap-2">
                            <button @click="visibleColumns = [...allColumns]" class="text-xs px-2 py-1 bg-blue-100 text-blue-700 rounded hover:bg-blue-200">
                                Select All
                            </button>
                            <button @click="visibleColumns = []" class="text-xs px-2 py-1 bg-gray-100 text-gray-700 rounded hover:bg-gray-200">
                                Clear All
                            </button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </section>

    <!-- Search Results Table -->
    <div class="overflow-x-auto border border-gray-200 rounded-lg shadow-sm mb-4">
        <table class="min-w-full text-sm">
            <thead class="bg-gray-50">
                <tr>
                    <th class="p-3">#</th>
                    <template x-for="col in visibleColumns" :key="col">
                        <th class="p-3">
                            <div class="flex flex-col">
                                <span x-text="col"></span>
                                <input type="text" placeholder="Filter"
                                       class="mt-1 border border-gray-300 rounded p-1 text-xs"
                                       x-model="filters[col]" />
                            </div>
                        </th>
                    </template>
                    <th class="p-3">Actions</th>
                </tr>
            </thead>
            <tbody>
                <template x-for="(result, rowIndex) in filteredResults" :key="result.id">
                    <tr class="border-t hover:bg-blue-50">
                        <td class="p-3" x-text="rowIndex + 1"></td>
                        <template x-for="col in visibleColumns" :key="col">
                            <td class="p-3" x-text="formatField(result[col])"></td>
                        </template>
                        <td class="p-3">
                            <button @click="openModal(result.id)"
                                    class="text-blue-600 hover:underline text-xs">
                                Edit
                            </button>
                        </td>
                    </tr>
                </template>
            </tbody>
        </table>
    </div>

    <!-- Pagination Controls -->
    <?php if ($totalPages > 1): ?>
    <div class="flex justify-between items-center mb-4">
        <div class="text-sm text-gray-600">
            Page <?= $currentPage ?> of <?= $totalPages ?> (<?= $totalItems ?> total results)
        </div>
        <div class="space-x-2">
            <?php if ($currentPage > 1): ?>
                <a href="?<?= http_build_query(array_merge($_GET, ['page' => $currentPage - 1])) ?>"
                   class="px-3 py-1 border rounded bg-white hover:bg-gray-50">
                    Previous
                </a>
            <?php endif; ?>
            <?php if ($currentPage < $totalPages): ?>
                <a href="?<?= http_build_query(array_merge($_GET, ['page' => $currentPage + 1])) ?>"
                   class="px-3 py-1 border rounded bg-white hover:bg-gray-50">
                    Next
                </a>
            <?php endif; ?>
        </div>
    </div>
    <?php endif; ?>
    <?php endif; ?>

    <!-- Modal for Full Entity Details -->
    <div x-show="modalOpen" x-cloak class="fixed inset-0 flex items-center justify-center z-50">
        <div class="fixed inset-0 bg-black bg-opacity-50" @click="closeModal"></div>
        <div class="bg-white p-6 rounded shadow-lg z-50 w-full max-w-3xl max-h-[90vh] overflow-y-auto relative">
            <button @click="closeModal" class="absolute top-2 right-2 text-gray-600 hover:text-gray-900 text-2xl">
                &times;
            </button>
            <h2 class="text-xl font-bold mb-4">Edit <span x-text="entityType === 'contact' ? 'Contact' : 'Deal'"></span></h2>
            
            <div x-show="saveMessage" class="text-green-600 text-sm mb-2" x-text="saveMessage"></div>
            <div x-show="loading" class="text-gray-600 text-sm mb-2">Loading...</div>
            
            <template x-if="!loading && modalData.id">
                <form @submit.prevent="saveEntity" :class="{'loading': saving}">
                    <!-- Contact Fields -->
                    <template x-if="entityType === 'contact'">
                        <div>
                            <?php foreach (['Contact Details', 'Marketing', 'Social', 'Sitelink'] as $groupName): ?>
                            <?php if (isset($editGroups[$groupName])): ?>
                            <div class="mb-4">
                                <h3 class="text-lg font-bold mb-2"><?= $groupName ?></h3>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <?php foreach ($editGroups[$groupName] as $field): ?>
                                    <div>
                                        <label class="block text-sm font-medium" for="<?= $field ?>">
                                            <?= htmlspecialchars($field) ?>
                                        </label>
                                        <input id="<?= $field ?>"
                                               type="text"
                                               class="w-full border border-gray-300 p-2 rounded"
                                               x-model="modalData['<?= $field ?>']" />
                                    </div>
                                    <?php endforeach; ?>
                                </div>
                            </div>
                            <?php endif; ?>
                            <?php endforeach; ?>
                        </div>
                    </template>

                    <!-- Deal Fields -->
                    <template x-if="entityType === 'deal'">
                        <div>
                            <div class="mb-4">
                                <h3 class="text-lg font-bold mb-2">Deal Details</h3>
                                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                    <?php foreach ($editGroups['Deal'] as $field): ?>
                                    <div>
                                        <label class="block text-sm font-medium" for="<?= $field ?>">
                                            <?= htmlspecialchars($field) ?>
                                        </label>
                                        <input id="<?= $field ?>"
                                               type="text"
                                               class="w-full border border-gray-300 p-2 rounded"
                                               x-model="modalData['<?= $field ?>']" />
                                    </div>
                                    <?php endforeach; ?>
                                </div>
                            </div>
                        </div>
                    </template>

                    <!-- Other Fields -->
                    <template x-if="otherFields().length > 0">
                        <div class="mb-4">
                            <h3 class="text-lg font-bold mb-2">Other Fields</h3>
                            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                                <template x-for="[key, value] of otherFields()" :key="key">
                                    <div>
                                        <label class="block text-sm font-medium" :for="key" x-text="key"></label>
                                        <input :id="key"
                                               type="text"
                                               class="w-full border border-gray-300 p-2 rounded"
                                               x-model="modalData[key]" />
                                    </div>
                                </template>
                            </div>
                        </div>
                    </template>

                    <div class="mt-4 flex justify-end space-x-2">
                        <button type="button" @click="closeModal"
                                class="px-4 py-2 bg-gray-300 rounded">
                            Cancel
                        </button>
                        <button type="submit"
                                :disabled="saving"
                                class="px-4 py-2 bg-blue-600 text-white rounded disabled:opacity-50">
                            <span x-show="!saving">Save</span>
                            <span x-show="saving">Saving...</span>
                        </button>
                    </div>
                </form>
            </template>
        </div>
    </div>
</div>

<script>
function searchPage() {
    return {
        // Initialize with PHP data
        entityType: '<?= $entityType ?>',
        searchResults: <?= json_encode($searchResults) ?>,
        allColumns: <?= json_encode($allColumns) ?>,
        visibleColumns: <?= json_encode($allColumns) ?>,
        filters: {},
        
        // Modal state
        modalOpen: false,
        loading: false,
        saving: false,
        modalData: {},
        modalOriginalData: {},
        saveMessage: '',
        
        // Configuration
        excludedFields: <?= json_encode($excludedFields) ?>,
        editGroups: <?= json_encode($editGroups) ?>,
        
        init() {
            // Initialize filters
            this.allColumns.forEach(col => {
                this.filters[col] = '';
            });
        },
        
        formatField(value) {
            if (typeof value === 'object' && value !== null && value.name) {
                return value.name;
            }
            return value || '-';
        },
        
        get filteredResults() {
            return this.searchResults.filter(result => {
                return this.visibleColumns.every(col => {
                    const filterVal = (this.filters[col] || '').toLowerCase();
                    let fieldVal = result[col] || '';
                    if (typeof fieldVal === 'object' && fieldVal !== null && fieldVal.name) {
                        fieldVal = fieldVal.name;
                    }
                    return fieldVal.toString().toLowerCase().includes(filterVal);
                });
            });
        },
        
        groupedFields() {
            let fields = [];
            for (let group in this.editGroups) {
                if (this.entityType === 'contact' && group !== 'Deal') {
                    fields = fields.concat(this.editGroups[group]);
                } else if (this.entityType === 'deal' && group === 'Deal') {
                    fields = fields.concat(this.editGroups[group]);
                }
            }
            return fields;
        },
        
        otherFields() {
            const grouped = this.groupedFields();
            const others = [];
            for (const [key, value] of Object.entries(this.modalData)) {
                if (!grouped.includes(key) && !this.excludedFields.includes(key)) {
                    if (!(typeof value === 'object' && value !== null)) {
                        others.push([key, value]);
                    }
                }
            }
            return others;
        },
        
        async apiRequest(action, data = {}) {
            const response = await fetch(window.location.href, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action, ...data })
            });
            
            if (!response.ok) {
                const error = await response.json();
                throw new Error(error.error || 'API request failed');
            }
            
            return response.json();
        },
        
        async openModal(entityId) {
            try {
                this.modalOpen = true;
                this.loading = true;
                
                const action = this.entityType === 'contact' ? 'view_contact' : 'view_deal';
                const include = this.entityType === 'contact' ? 'custom_field,emails,phone_numbers' : 'custom_field';
                
                const data = await this.apiRequest(action, { id: entityId, include });
                this.modalData = data.contact || data.deal || data;
                this.modalOriginalData = JSON.parse(JSON.stringify(this.modalData));
            } catch (err) {
                console.error('Error fetching details:', err);
                alert('Failed to load details: ' + err.message);
                this.closeModal();
            } finally {
                this.loading = false;
            }
        },
        
        async saveEntity() {
            try {
                this.saving = true;
                
                // Move cf_ fields to custom_field for contacts
                if (this.entityType === 'contact') {
                    if (!this.modalData.custom_field) {
                        this.modalData.custom_field = {};
                    }
                    for (const key in this.modalData) {
                        if (key.startsWith('cf_')) {
                            this.modalData.custom_field[key] = this.modalData[key];
                            delete this.modalData[key];
                        }
                    }
                }
                
                const diff = this.getDiff(this.modalOriginalData, this.modalData);
                const action = this.entityType === 'contact' ? 'update_contact' : 'update_deal';
                
                await this.apiRequest(action, { id: this.modalData.id, data: diff });
                
                this.saveMessage = 'Saved successfully!';
                setTimeout(() => { this.saveMessage = ''; }, 3000);
                
                // Refresh the page to show updated data
                setTimeout(() => { window.location.reload(); }, 1000);
                
            } catch (err) {
                console.error('Error saving:', err);
                alert('Failed to save: ' + err.message);
            } finally {
                this.saving = false;
            }
        },
        
        getDiff(original, updated) {
            const diff = {};
            for (const key in updated) {
                if (key === 'custom_field') {
                    const nestedDiff = {};
                    const origNested = original.custom_field || {};
                    for (const subKey in updated.custom_field) {
                        if (updated.custom_field[subKey] !== origNested[subKey]) {
                            nestedDiff[subKey] = updated.custom_field[subKey];
                        }
                    }
                    if (Object.keys(nestedDiff).length > 0) {
                        diff.custom_field = nestedDiff;
                    }
                } else if (updated[key] !== original[key]) {
                    diff[key] = updated[key];
                }
            }
            return diff;
        },
        
        closeModal() {
            this.modalOpen = false;
            this.modalData = {};
            this.modalOriginalData = {};
            this.loading = false;
            this.saving = false;
            this.saveMessage = '';
        }
    };
}
</script>
</body>
</html>      