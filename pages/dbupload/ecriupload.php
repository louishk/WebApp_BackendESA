<?php
require_once __DIR__ . '/../../config.php';

// Check if user is logged in and has appropriate role
require_role(['admin', 'chat']);

// Get RBS API configuration
$apiBaseUrl = RBS_API_BASE;
$apiBearer = RBS_API_BEARER;

// Page title and configuration
$pageTitle = 'Excel File Processor';
$maxFileSize = 50; // 50MB
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title><?php echo htmlspecialchars($pageTitle); ?></title>
    <script src="https://cdn.tailwindcss.com"></script>
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    animation: {
                        'spin-slow': 'spin 1s linear infinite',
                    }
                }
            }
        }
    </script>
    <style>
        @keyframes gradient-shift {
            0%, 100% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
        }
        
        .gradient-bg {
            background: linear-gradient(-45deg, #667eea, #764ba2, #6b73ff, #9068be);
            background-size: 400% 400%;
            animation: gradient-shift 15s ease infinite;
        }
        
        .glass-effect {
            backdrop-filter: blur(10px);
            background: rgba(255, 255, 255, 0.1);
        }
    </style>
</head>
<body class="gradient-bg min-h-screen flex items-center justify-center p-5">
    <a href="../" class="fixed top-5 left-5 text-white no-underline font-medium py-2 px-5 glass-effect rounded-full transition-all duration-300 hover:bg-white hover:bg-opacity-20 hover:-translate-y-0.5">
        ← Back to Dashboard
    </a>

    <div class="bg-white rounded-3xl shadow-2xl p-10 w-full max-w-2xl">
        <!-- Header -->
        <div class="text-center mb-8">
            <h1 class="text-3xl font-bold text-gray-800 mb-3"><?php echo htmlspecialchars($pageTitle); ?></h1>
            <p class="text-gray-600 text-lg">Upload and process Excel files with automated data validation</p>
        </div>

        <!-- User Info -->
        <?php if (isset($_SESSION['user'])): ?>
        <div class="bg-gray-50 rounded-xl p-4 mb-5 flex justify-between items-center">
            <span class="font-semibold text-gray-800">
                Welcome, <?php echo htmlspecialchars($_SESSION['user']['name'] ?? $_SESSION['user']['email'] ?? 'User'); ?>
            </span>
            <span class="bg-blue-500 text-white px-3 py-1 rounded-full text-sm">
                <?php echo htmlspecialchars($_SESSION['user']['role'] ?? 'User'); ?>
            </span>
        </div>
        <?php endif; ?>

        <form id="uploadForm" enctype="multipart/form-data">
            <!-- Configuration Selection -->
            <div class="mb-5">
                <label for="configType" class="block mb-3 font-semibold text-gray-800">Select Processing Type:</label>
                <select id="configType" name="configType" class="w-full p-3 border-2 border-gray-200 rounded-lg text-base bg-white transition-colors duration-300 focus:outline-none focus:border-blue-500">
                    <option value="">Choose processing configuration...</option>
                    <option value="tenant_rate_export">Tenant Rate Export (RM_ECRIbatch) - ~5-10 mins</option>
                    <option value="ecri_raw">ECRI Raw Data (RM_ECRIraw) - ~25-35 mins</option>
                </select>

                <div id="configInfo" class="hidden bg-blue-50 border border-blue-200 rounded-lg p-4 mt-3">
                    <h4 id="configTitle" class="text-blue-800 font-semibold mb-2"></h4>
                    <p id="configDescription" class="text-blue-700 text-sm mb-1"></p>
                    <p id="configTable" class="text-blue-700 text-sm mb-1"></p>
                    <p id="configSheet" class="text-blue-700 text-sm mb-1"></p>
                    <p id="configTiming" class="text-blue-600 text-xs font-medium"></p>
                </div>
            </div>

            <!-- File Upload Area -->
            <div class="relative mb-5">
                <div id="uploadArea" class="border-3 border-dashed border-gray-300 rounded-2xl p-10 text-center cursor-pointer transition-all duration-300 hover:border-blue-500 hover:bg-blue-50">
                    <button type="button" id="clearFileBtn" class="hidden absolute top-3 right-3 bg-red-500 text-white border-none rounded-full w-8 h-8 cursor-pointer text-lg items-center justify-center transition-all duration-300 hover:bg-red-600 hover:scale-110">×</button>
                    <div class="text-6xl text-gray-300 mb-4">📊</div>
                    <div class="text-gray-600 text-lg mb-3">Click to select file or drag and drop</div>
                    <div class="text-gray-400 text-sm">Supports .xlsx and .xls files (max <?php echo $maxFileSize; ?>MB)</div>
                    <input type="file" id="fileInput" name="excel_file" accept=".xlsx,.xls" class="hidden">
                </div>
            </div>

            <!-- File Info -->
            <div id="fileInfo" class="hidden bg-gray-50 rounded-xl p-4 mb-5 border-l-4 border-green-500">
                <div id="fileName" class="font-semibold text-gray-800 mb-1"></div>
                <div id="fileSize" class="text-gray-600 text-sm"></div>
            </div>

            <!-- Progress -->
            <div id="progress" class="hidden mb-5">
                <div class="bg-gray-200 rounded-xl h-3 overflow-hidden mb-3">
                    <div id="progressFill" class="bg-gradient-to-r from-blue-500 to-purple-600 h-full rounded-xl w-0 transition-all duration-500"></div>
                </div>
                <div id="progressText" class="text-center text-gray-600 font-medium">Initializing...</div>
                <div class="flex justify-between items-center mt-3 text-sm text-gray-600">
                    <span id="progressStatus">Preparing upload...</span>
                    <span id="progressTime">--:--</span>
                </div>
                <div id="progressStats" class="hidden grid grid-cols-3 gap-3 mt-3">
                    <div class="text-center p-2 bg-blue-50 rounded-lg">
                        <div id="statProcessed" class="font-semibold text-lg text-gray-800">0</div>
                        <div class="text-xs text-gray-600 mt-1">Processed</div>
                    </div>
                    <div class="text-center p-2 bg-blue-50 rounded-lg">
                        <div id="statTotal" class="font-semibold text-lg text-gray-800">0</div>
                        <div class="text-xs text-gray-600 mt-1">Total</div>
                    </div>
                    <div class="text-center p-2 bg-blue-50 rounded-lg">
                        <div id="statErrors" class="font-semibold text-lg text-gray-800">0</div>
                        <div class="text-xs text-gray-600 mt-1">Errors</div>
                    </div>
                </div>
            </div>

            <!-- Submit Button -->
            <button type="submit" id="submitBtn" disabled class="w-full bg-gradient-to-r from-blue-500 to-purple-600 text-white border-none py-4 px-8 rounded-xl text-lg font-semibold cursor-pointer transition-all duration-300 mt-5 disabled:opacity-60 disabled:cursor-not-allowed hover:enabled:-translate-y-0.5 hover:enabled:shadow-lg">
                Upload and Process File
            </button>

            <!-- Processing Indicator -->
            <div id="processingIndicator" class="hidden text-center text-blue-500 font-medium mt-3">
                <div class="inline-block w-5 h-5 border-2 border-gray-200 border-t-2 border-t-blue-500 rounded-full animate-spin-slow mr-3"></div>
                Processing data, please wait...
            </div>
        </form>

        <!-- Results -->
        <div id="result" class="hidden mt-5 p-4 rounded-xl">
            <div id="resultTitle" class="font-semibold mb-3"></div>
            <div id="resultMessage"></div>
            <div id="resultStats" class="hidden grid grid-cols-2 lg:grid-cols-4 gap-3 mt-4"></div>
        </div>
    </div>

    <script>
        // Configuration from PHP
        const API_BASE_URL = '<?php echo addslashes($apiBaseUrl); ?>';
        const API_BEARER_TOKEN = '<?php echo addslashes($apiBearer); ?>';
        const MAX_FILE_SIZE = <?php echo $maxFileSize; ?> * 1024 * 1024; // Convert MB to bytes

        // Configuration descriptions
        const CONFIG_DESCRIPTIONS = {
            'tenant_rate_export': {
                title: 'Tenant Rate Export Processing',
                description: 'Processes tenant rate export files with comprehensive validation and MERGE operations.',
                table: 'Target Table: RM_ECRIbatch',
                sheet: 'Sheet: First sheet (default)',
                timing: 'Expected processing time: 5-10 minutes for typical files'
            },
            'ecri_raw': {
                title: 'ECRI Raw Data Processing',
                description: 'Processes ECRI raw data with custom ID generation and specific validation rules.',
                table: 'Target Table: RM_ECRIraw',
                sheet: 'Sheet: "raw" worksheet',
                timing: 'Expected processing time: 25-35 minutes for large datasets (100+ batches)'
            }
        };

        // DOM elements
        const uploadArea = document.getElementById('uploadArea');
        const fileInput = document.getElementById('fileInput');
        const clearFileBtn = document.getElementById('clearFileBtn');
        const configType = document.getElementById('configType');
        const configInfo = document.getElementById('configInfo');
        const configTitle = document.getElementById('configTitle');
        const configDescription = document.getElementById('configDescription');
        const configTable = document.getElementById('configTable');
        const configSheet = document.getElementById('configSheet');
        const configTiming = document.getElementById('configTiming');
        const fileInfo = document.getElementById('fileInfo');
        const fileName = document.getElementById('fileName');
        const fileSize = document.getElementById('fileSize');
        const submitBtn = document.getElementById('submitBtn');
        const progress = document.getElementById('progress');
        const progressFill = document.getElementById('progressFill');
        const progressText = document.getElementById('progressText');
        const progressStatus = document.getElementById('progressStatus');
        const progressTime = document.getElementById('progressTime');
        const progressStats = document.getElementById('progressStats');
        const statProcessed = document.getElementById('statProcessed');
        const statTotal = document.getElementById('statTotal');
        const statErrors = document.getElementById('statErrors');
        const processingIndicator = document.getElementById('processingIndicator');
        const result = document.getElementById('result');
        const resultTitle = document.getElementById('resultTitle');
        const resultMessage = document.getElementById('resultMessage');
        const resultStats = document.getElementById('resultStats');
        const uploadForm = document.getElementById('uploadForm');

        // Progress tracking
        let progressStartTime = null;
        let progressUpdateInterval = null;

        // Event listeners
        fileInput.addEventListener('change', handleFileSelect);
        uploadArea.addEventListener('click', (e) => {
            if (e.target !== clearFileBtn) {
                fileInput.click();
            }
        });
        clearFileBtn.addEventListener('click', clearFileSelection);
        configType.addEventListener('change', handleConfigChange);

        // Drag and drop handlers
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('border-blue-500', 'bg-blue-50', 'scale-105');
        });

        uploadArea.addEventListener('dragleave', () => {
            uploadArea.classList.remove('border-blue-500', 'bg-blue-50', 'scale-105');
        });

        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('border-blue-500', 'bg-blue-50', 'scale-105');
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                fileInput.files = files;
                handleFileSelect();
            }
        });

        function handleConfigChange() {
            const selectedConfig = configType.value;

            if (selectedConfig && CONFIG_DESCRIPTIONS[selectedConfig]) {
                const config = CONFIG_DESCRIPTIONS[selectedConfig];
                configTitle.textContent = config.title;
                configDescription.textContent = config.description;
                configTable.textContent = config.table;
                configSheet.textContent = config.sheet;
                configTiming.textContent = config.timing;
                configInfo.classList.remove('hidden');
            } else {
                configInfo.classList.add('hidden');
            }

            checkFormValidity();
        }

        function clearFileSelection() {
            fileInput.value = '';
            fileInfo.classList.add('hidden');
            uploadArea.classList.remove('border-green-500', 'bg-green-50');
            clearFileBtn.classList.add('hidden');
            result.classList.add('hidden');
            checkFormValidity();
        }

        function handleFileSelect() {
            const file = fileInput.files[0];
            if (file) {
                // Validate file type
                const validTypes = ['.xlsx', '.xls'];
                const fileExtension = '.' + file.name.split('.').pop().toLowerCase();

                if (!validTypes.includes(fileExtension)) {
                    showResult('error', 'Invalid File Type', 'Please select an Excel file (.xlsx or .xls)');
                    clearFileSelection();
                    return;
                }

                // Validate file size
                if (file.size > MAX_FILE_SIZE) {
                    showResult('error', 'File Too Large', `File size must be less than ${MAX_FILE_SIZE / (1024*1024)}MB`);
                    clearFileSelection();
                    return;
                }

                // Show file info
                fileName.textContent = file.name;
                fileSize.textContent = formatFileSize(file.size);
                fileInfo.classList.remove('hidden');
                uploadArea.classList.add('border-green-500', 'bg-green-50');
                clearFileBtn.classList.remove('hidden');
                clearFileBtn.classList.add('flex');

                // Check form validity
                checkFormValidity();

                // Hide previous results
                result.classList.add('hidden');
            }
        }

        function checkFormValidity() {
            const hasFile = fileInput.files.length > 0;
            const hasConfig = configType.value !== '';
            submitBtn.disabled = !(hasFile && hasConfig);
        }

        function startProgressTracking() {
            progressStartTime = Date.now();
            progressUpdateInterval = setInterval(() => {
                const elapsed = Date.now() - progressStartTime;
                const minutes = Math.floor(elapsed / 60000);
                const seconds = Math.floor((elapsed % 60000) / 1000);
                progressTime.textContent = `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
            }, 1000);
        }

        function stopProgressTracking() {
            if (progressUpdateInterval) {
                clearInterval(progressUpdateInterval);
                progressUpdateInterval = null;
            }
        }

        function updateProgress(percentage, status, details = null) {
            progressFill.style.width = percentage + '%';
            progressText.textContent = `${Math.round(percentage)}%`;
            progressStatus.textContent = status;
            
            if (details) {
                statProcessed.textContent = details.processed || 0;
                statTotal.textContent = details.total || 0;
                statErrors.textContent = details.errors || 0;
                progressStats.classList.remove('hidden');
                progressStats.classList.add('grid');
            }
        }

        // Enhanced form submission handler with timeout handling
        uploadForm.addEventListener('submit', async (e) => {
            e.preventDefault();

            const file = fileInput.files[0];
            const selectedConfig = configType.value;

            if (!file || !selectedConfig) {
                showResult('error', 'Missing Information', 'Please select both a file and processing type');
                return;
            }

            // Show progress and disable form
            progress.classList.remove('hidden');
            processingIndicator.classList.remove('hidden');
            submitBtn.disabled = true;
            configType.disabled = true;
            clearFileBtn.classList.add('hidden');
            result.classList.add('hidden');

            // Start progress tracking
            startProgressTracking();

            // Create form data
            const formData = new FormData();
            formData.append('file', file);

            // Set different timeout based on config type
            const isECRIRaw = selectedConfig === 'ecri_raw';
            const timeoutDuration = isECRIRaw ? 35 * 60 * 1000 : 10 * 60 * 1000; // 35 min for ECRI Raw, 10 min for others

            try {
                // Phase 1: File upload (0-20%)
                updateProgress(5, 'Uploading file...');
                
                const apiUrl = `${API_BASE_URL}/dbupload/process-excel/${selectedConfig}`;
                
                updateProgress(10, 'Connecting to server...');

                // Create AbortController for timeout handling
                const controller = new AbortController();
                const timeoutId = setTimeout(() => {
                    controller.abort();
                }, timeoutDuration);

                // Make API request with extended timeout and progress monitoring
                const response = await fetch(apiUrl, {
                    method: 'POST',
                    headers: {
                        'Authorization': `Bearer ${API_BEARER_TOKEN}`
                    },
                    body: formData,
                    signal: controller.signal
                });

                clearTimeout(timeoutId);
                updateProgress(20, 'File uploaded, processing data...');

                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                // Phase 2: Server processing simulation - adjusted for ECRI Raw
                let processingSteps;
                if (isECRIRaw) {
                    // Longer, more detailed steps for ECRI Raw processing
                    processingSteps = [
                        { progress: 25, status: 'Reading Excel file...', delay: 2000 },
                        { progress: 30, status: 'Validating data format...', delay: 1500 },
                        { progress: 35, status: 'Creating ID columns...', delay: 2000 },
                        { progress: 40, status: 'Processing data transformations...', delay: 3000 },
                        { progress: 45, status: 'Connecting to database...', delay: 1500 },
                        { progress: 50, status: 'Starting database operations...', delay: 2000 },
                        { progress: 60, status: 'Processing batch 1-20...', delay: 4000 },
                        { progress: 70, status: 'Processing batch 21-40...', delay: 4000 },
                        { progress: 80, status: 'Processing batch 41-60...', delay: 4000 },
                        { progress: 85, status: 'Processing batch 61-80...', delay: 4000 },
                        { progress: 90, status: 'Processing final batches...', delay: 4000 },
                        { progress: 95, status: 'Completing processing...', delay: 2000 }
                    ];
                } else {
                    // Standard steps for other processing types
                    processingSteps = [
                        { progress: 30, status: 'Reading Excel file...', delay: 800 },
                        { progress: 40, status: 'Validating data format...', delay: 800 },
                        { progress: 50, status: 'Processing data transformations...', delay: 800 },
                        { progress: 60, status: 'Connecting to database...', delay: 800 },
                        { progress: 70, status: 'Starting database operations...', delay: 800 },
                        { progress: 80, status: 'Processing data batches...', delay: 800 },
                        { progress: 90, status: 'Finalizing operations...', delay: 800 },
                        { progress: 95, status: 'Completing processing...', delay: 800 }
                    ];
                }

                // Simulate processing steps with appropriate delays
                for (const step of processingSteps) {
                    updateProgress(step.progress, step.status);
                    await new Promise(resolve => setTimeout(resolve, step.delay));
                }

                const data = await response.json();

                // Phase 3: Complete (100%)
                updateProgress(100, 'Processing complete!');

                setTimeout(() => {
                    stopProgressTracking();
                    progress.classList.add('hidden');
                    processingIndicator.classList.add('hidden');

                    if (data.success) {
                        showResult('success', 'Processing Complete!', data.message);
                        if (data.data) {
                            showStats(data.data);
                        }
                    } else {
                        showResult('error', 'Processing Failed', data.error || 'An unknown error occurred');
                    }

                    // Re-enable form
                    submitBtn.disabled = false;
                    configType.disabled = false;
                    clearFileBtn.classList.remove('hidden');
                    clearFileBtn.classList.add('flex');
                }, 1000);

            } catch (error) {
                console.error('Processing error:', error);
                stopProgressTracking();
                progress.classList.add('hidden');
                processingIndicator.classList.add('hidden');

                let errorMessage = 'Network error: ' + error.message;

                if (error.name === 'AbortError') {
                    // Handle timeout specifically
                    if (isECRIRaw) {
                        errorMessage = `Processing is taking longer than expected (35+ minutes). The operation may still be running in the background. Please check the database or contact support to verify completion.`;
                        showResult('warning', 'Processing Timeout', errorMessage);
                    } else {
                        errorMessage = `Processing timeout after 10 minutes. Please try again or contact support.`;
                        showResult('error', 'Processing Timeout', errorMessage);
                    }
                } else if (error.message.includes('Failed to fetch')) {
                    errorMessage = `Connection failed. Please check your network connection and try again.`;
                    showResult('error', 'Connection Failed', errorMessage);
                } else if (error.message.includes('HTTP 404')) {
                    errorMessage = 'API endpoint not found. Please contact support.';
                    showResult('error', 'API Error', errorMessage);
                } else if (error.message.includes('HTTP 403')) {
                    errorMessage = 'Access denied. Please check your permissions.';
                    showResult('error', 'Access Denied', errorMessage);
                } else if (error.message.includes('HTTP 500')) {
                    errorMessage = 'Server error. Please try again later or contact support.';
                    showResult('error', 'Server Error', errorMessage);
                } else {
                    showResult('error', 'Upload Failed', errorMessage);
                }
                
                // Re-enable form
                submitBtn.disabled = false;
                configType.disabled = false;
                clearFileBtn.classList.remove('hidden');
                clearFileBtn.classList.add('flex');
            }
        });

        function showResult(type, title, message) {
            let bgClass, borderClass, textClass;
            
            switch(type) {
                case 'success':
                    bgClass = 'bg-green-100';
                    borderClass = 'border-green-300';
                    textClass = 'text-green-800';
                    break;
                case 'warning':
                    bgClass = 'bg-yellow-100';
                    borderClass = 'border-yellow-300';
                    textClass = 'text-yellow-800';
                    break;
                case 'error':
                default:
                    bgClass = 'bg-red-100';
                    borderClass = 'border-red-300';
                    textClass = 'text-red-800';
                    break;
            }
            
            result.className = `p-4 rounded-xl mt-5 border ${bgClass} ${borderClass} ${textClass}`;
            result.classList.remove('hidden');
            resultTitle.textContent = title;
            resultMessage.textContent = message;
            resultStats.classList.add('hidden');
        }

        function showStats(data) {
            if (data.processed_rows !== undefined) {
                resultStats.innerHTML = `
                    <div class="text-center p-3 bg-white bg-opacity-70 rounded-lg">
                        <div class="text-2xl font-bold text-gray-800">${data.processed_rows}</div>
                        <div class="text-sm text-gray-600 mt-1">Processed Rows</div>
                    </div>
                    <div class="text-center p-3 bg-white bg-opacity-70 rounded-lg">
                        <div class="text-2xl font-bold text-gray-800">${data.total_rows}</div>
                        <div class="text-sm text-gray-600 mt-1">Total Rows</div>
                    </div>
                    <div class="text-center p-3 bg-white bg-opacity-70 rounded-lg">
                        <div class="text-2xl font-bold text-gray-800">${data.error_count}</div>
                        <div class="text-sm text-gray-600 mt-1">Errors</div>
                    </div>
                    <div class="text-center p-3 bg-white bg-opacity-70 rounded-lg lg:col-span-1">
                        <div class="text-lg font-bold text-gray-800">${data.table_name}</div>
                        <div class="text-sm text-gray-600 mt-1">Target Table</div>
                    </div>
                `;
                resultStats.classList.remove('hidden');
                resultStats.classList.add('grid');
            }
        }

        function formatFileSize(bytes) {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        }
    </script>
</body>
</html>