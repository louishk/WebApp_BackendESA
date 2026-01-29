<?php
require_once dirname(__DIR__, 2) . '/config.php';

// Database connection - uses global $pdo from config.php (PostgreSQL)
function getDbConnection() {
    global $pdo;
    return $pdo;
}

// Handle AJAX requests
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    header('Content-Type: application/json');

    try {
        $action = $_POST['action'] ?? '';
        $pdo = getDbConnection();

        switch ($action) {
            case 'save':
                $name = $_POST['name'] ?? '';
                $schema_type = $_POST['schema_type'] ?? '';
                $schema_data = $_POST['schema_data'] ?? '';
                $form_data = $_POST['form_data'] ?? '';

                if (empty($name) || empty($schema_type) || empty($schema_data)) {
                    throw new Exception('Missing required fields');
                }

                $stmt = $pdo->prepare("
                    INSERT INTO schema_markups (name, schema_type, schema_data, form_data, created_at, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ");
                $stmt->execute([$name, $schema_type, $schema_data, $form_data]);

                echo json_encode([
                    'success' => true,
                    'id' => $pdo->lastInsertId(),
                    'message' => 'Schema saved successfully'
                ]);
                break;

            case 'load':
                $id = $_POST['id'] ?? 0;

                if (!$id) {
                    throw new Exception('Schema ID required');
                }

                $stmt = $pdo->prepare("SELECT * FROM schema_markups WHERE id = ?");
                $stmt->execute([$id]);
                $schema = $stmt->fetch();

                if (!$schema) {
                    throw new Exception('Schema not found');
                }

                echo json_encode(['success' => true, 'schema' => $schema]);
                break;

            case 'copy':
                $id = $_POST['id'] ?? 0;

                if (!$id) {
                    throw new Exception('Schema ID required');
                }

                $stmt = $pdo->prepare("SELECT * FROM schema_markups WHERE id = ?");
                $stmt->execute([$id]);
                $originalSchema = $stmt->fetch();

                if (!$originalSchema) {
                    throw new Exception('Schema not found');
                }

                // Create a copy with modified name
                $newName = $originalSchema['name'] . ' (Copy)';
                $stmt = $pdo->prepare("
                    INSERT INTO schema_markups (name, schema_type, schema_data, form_data, created_at, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                ");
                $stmt->execute([$newName, $originalSchema['schema_type'], $originalSchema['schema_data'], $originalSchema['form_data']]);

                echo json_encode([
                    'success' => true,
                    'id' => $pdo->lastInsertId(),
                    'message' => 'Schema copied successfully'
                ]);
                break;

            case 'validate':
                $schema_data = $_POST['schema_data'] ?? '';
                
                if (empty($schema_data)) {
                    throw new Exception('Schema data required for validation');
                }

                // Prepare the script tag format for validation
                $scriptTag = '<script type="application/ld+json">' . "\n" . $schema_data . "\n" . '</script>';
                
                // Call schema.org validator
                $validationResult = validateSchema($scriptTag);
                
                echo json_encode([
                    'success' => true,
                    'validation' => $validationResult
                ]);
                break;

            case 'delete':
                $id = $_POST['id'] ?? 0;

                if (!$id) {
                    throw new Exception('Schema ID required');
                }

                $stmt = $pdo->prepare("DELETE FROM schema_markups WHERE id = ?");
                $stmt->execute([$id]);

                echo json_encode(['success' => true, 'message' => 'Schema deleted successfully']);
                break;

            case 'list':
                $stmt = $pdo->query("
                    SELECT id, name, schema_type, created_at, updated_at
                    FROM schema_markups
                    ORDER BY updated_at DESC
                ");
                $schemas = $stmt->fetchAll();

                echo json_encode(['success' => true, 'schemas' => $schemas]);
                break;

            case 'update':
                $id = $_POST['id'] ?? 0;
                $name = $_POST['name'] ?? '';
                $schema_type = $_POST['schema_type'] ?? '';
                $schema_data = $_POST['schema_data'] ?? '';
                $form_data = $_POST['form_data'] ?? '';

                if (!$id || empty($name) || empty($schema_type) || empty($schema_data)) {
                    throw new Exception('Missing required fields');
                }

                $stmt = $pdo->prepare("
                    UPDATE schema_markups
                    SET name = ?, schema_type = ?, schema_data = ?, form_data = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                ");
                $stmt->execute([$name, $schema_type, $schema_data, $form_data, $id]);

                echo json_encode(['success' => true, 'message' => 'Schema updated successfully']);
                break;

            default:
                throw new Exception('Invalid action');
        }

    } catch (Exception $e) {
        http_response_code(400);
        echo json_encode(['success' => false, 'error' => $e->getMessage()]);
    }
    exit;
}

// Schema validation function
function validateSchema($schemaHtml) {
    $url = 'https://validator.schema.org/validate';
    
    $postData = http_build_query([
        'html' => $schemaHtml,
        'format' => 'json'
    ]);
    
    $options = [
        'http' => [
            'header' => "Content-type: application/x-www-form-urlencoded\r\n",
            'method' => 'POST',
            'content' => $postData,
            'timeout' => 30
        ]
    ];
    
    $context = stream_context_create($options);
    $result = @file_get_contents($url, false, $context);
    
    if ($result === false) {
        return [
            'success' => false,
            'error' => 'Failed to connect to schema.org validator'
        ];
    }
    
    $validationData = json_decode($result, true);
    
    if (json_last_error() !== JSON_ERROR_NONE) {
        return [
            'success' => false,
            'error' => 'Invalid response from validator'
        ];
    }
    
    return [
        'success' => true,
        'data' => $validationData
    ];
}

// Load saved schemas for the page
try {
    $pdo = getDbConnection();
    $stmt = $pdo->query("
        SELECT id, name, schema_type, created_at, updated_at
        FROM schema_markups
        ORDER BY updated_at DESC
    ");
    $savedSchemas = $stmt->fetchAll();
} catch (Exception $e) {
    $savedSchemas = [];
    error_log("Failed to load schemas: " . $e->getMessage());
}
?>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Advanced Schema Markup Generator</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }

        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 0;
            text-align: center;
            margin-bottom: 30px;
            border-radius: 10px;
        }

        .header h1 {
            font-size: 2.5rem;
            margin-bottom: 10px;
        }

        .header p {
            font-size: 1.1rem;
            opacity: 0.9;
        }

        .main-content {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
        }

        .form-section {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            height: fit-content;
        }

        .preview-section {
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            position: sticky;
            top: 20px;
        }

        .schema-type-selector {
            margin-bottom: 30px;
        }

        .schema-type-selector label {
            display: block;
            margin-bottom: 10px;
            font-weight: bold;
            color: #555;
        }

        .schema-type-selector select {
            width: 100%;
            padding: 12px;
            border: 2px solid #ddd;
            border-radius: 5px;
            font-size: 16px;
            background: white;
        }

        .fieldset {
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            padding: 20px;
            margin-bottom: 20px;
            background: #fafafa;
        }

        .fieldset legend {
            background: #667eea;
            color: white;
            padding: 8px 15px;
            border-radius: 5px;
            font-weight: bold;
            font-size: 14px;
        }

        .form-group {
            margin-bottom: 15px;
        }

        .form-group label {
            display: block;
            margin-bottom: 5px;
            font-weight: 600;
            color: #555;
        }

        .form-group input,
        .form-group textarea,
        .form-group select {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 5px;
            font-size: 14px;
        }

        .form-group textarea {
            height: 80px;
            resize: vertical;
            font-family: monospace;
        }

        .form-row {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
        }

        .checkbox-group {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-top: 10px;
        }

        .checkbox-item {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 5px;
            border-radius: 4px;
            transition: background-color 0.2s;
        }

        .checkbox-item:hover {
            background-color: #f8f9fa;
        }

        .checkbox-item input[type="checkbox"] {
            width: auto;
            margin: 0;
        }

        .checkbox-item label {
            margin: 0;
            cursor: pointer;
            font-weight: normal;
        }

        .btn-group {
            display: flex;
            gap: 10px;
            margin: 20px 0;
            flex-wrap: wrap;
        }

        .btn {
            padding: 12px 24px;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s ease;
        }

        .btn-primary {
            background: #667eea;
            color: white;
        }

        .btn-secondary {
            background: #6c757d;
            color: white;
        }

        .btn-success {
            background: #28a745;
            color: white;
        }

        .btn-info {
            background: #17a2b8;
            color: white;
        }

        .btn-danger {
            background: #dc3545;
            color: white;
        }

        .btn-warning {
            background: #ffc107;
            color: #212529;
        }

        .btn:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }

        .btn-sm {
            padding: 6px 12px;
            font-size: 12px;
        }

        .btn-validate {
            background: #28a745;
            color: white;
            border: 2px solid #28a745;
        }

        .btn-validate:hover {
            background: #218838;
            border-color: #1e7e34;
        }

        .preview-content {
            background: #f8f9fa;
            border: 1px solid #dee2e6;
            border-radius: 5px;
            padding: 20px;
            margin-top: 20px;
        }

        .preview-content h3 {
            margin-bottom: 15px;
            color: #333;
        }

        .json-preview {
            background: #2d3748;
            color: #e2e8f0;
            padding: 20px;
            border-radius: 5px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            overflow-x: auto;
            white-space: pre-wrap;
            word-break: break-all;
            max-height: 400px;
            overflow-y: auto;
        }

        .snippet-preview {
            background: #fff;
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 15px;
            margin-bottom: 15px;
        }

        .snippet-title {
            color: #1a0dab;
            font-size: 18px;
            margin-bottom: 5px;
            text-decoration: none;
        }

        .snippet-url {
            color: #006621;
            font-size: 14px;
            margin-bottom: 5px;
        }

        .snippet-description {
            color: #545454;
            font-size: 14px;
            line-height: 1.4;
        }

        .rating-stars {
            color: #fbbc04;
            margin: 5px 0;
        }

        .saved-schemas {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            margin-top: 20px;
        }

        .saved-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 15px;
            border: 1px solid #ddd;
            border-radius: 5px;
            margin-bottom: 10px;
            background: #f9f9f9;
        }

        .saved-item-info {
            flex-grow: 1;
        }

        .saved-item-info h4 {
            margin: 0 0 5px 0;
            color: #333;
        }

        .saved-item-info p {
            margin: 0;
            color: #666;
            font-size: 14px;
        }

        .saved-item-actions {
            display: flex;
            gap: 10px;
        }

        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.5);
        }

        .modal-content {
            background-color: white;
            margin: 15% auto;
            padding: 20px;
            border-radius: 10px;
            width: 90%;
            max-width: 600px;
            max-height: 70vh;
            overflow-y: auto;
        }

        .modal-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }

        .close {
            color: #aaa;
            font-size: 28px;
            font-weight: bold;
            cursor: pointer;
        }

        .close:hover {
            color: black;
        }

        .loading {
            opacity: 0.6;
            pointer-events: none;
        }

        .alert {
            padding: 15px;
            margin-bottom: 20px;
            border: 1px solid transparent;
            border-radius: 4px;
        }

        .alert-success {
            color: #155724;
            background-color: #d4edda;
            border-color: #c3e6cb;
        }

        .alert-danger {
            color: #721c24;
            background-color: #f8d7da;
            border-color: #f5c6cb;
        }

        .alert-info {
            color: #0c5460;
            background-color: #d1ecf1;
            border-color: #bee5eb;
        }

        .alert-warning {
            color: #856404;
            background-color: #fff3cd;
            border-color: #ffeaa7;
        }

        .validation-result {
            margin-top: 20px;
            padding: 15px;
            border-radius: 5px;
            border: 1px solid #ddd;
        }

        .validation-success {
            background-color: #d4edda;
            border-color: #c3e6cb;
            color: #155724;
        }

        .validation-error {
            background-color: #f8d7da;
            border-color: #f5c6cb;
            color: #721c24;
        }

        .validation-details {
            margin-top: 10px;
            font-size: 14px;
        }

        .error-item {
            margin: 5px 0;
            padding: 5px;
            background: rgba(220, 53, 69, 0.1);
            border-left: 3px solid #dc3545;
        }

        .warning-item {
            margin: 5px 0;
            padding: 5px;
            background: rgba(255, 193, 7, 0.1);
            border-left: 3px solid #ffc107;
        }

        .image-preview-container {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 10px;
            padding: 10px;
            border: 1px dashed #ddd;
            border-radius: 5px;
            min-height: 80px;
            align-items: center;
        }

        @media (max-width: 768px) {
            .main-content {
                grid-template-columns: 1fr;
            }

            .form-row {
                grid-template-columns: 1fr;
            }

            .header h1 {
                font-size: 2rem;
            }

            .checkbox-group {
                grid-template-columns: 1fr;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Advanced Schema Markup Generator</h1>
            <p>Generate, preview, validate, and manage comprehensive schema markup for better SEO</p>
        </div>

        <div id="alertContainer"></div>

        <div class="main-content">
            <div class="form-section">
                <div class="schema-type-selector">
                    <label for="schemaType">Schema Type</label>
                    <select id="schemaType" onchange="switchSchemaType()">
                        <option value="LocalBusiness">Local Business</option>
                        <option value="Restaurant">Restaurant</option>
                        <option value="Organization">Organization</option>
                        <option value="Person">Person</option>
                        <option value="Product">Product</option>
                        <option value="Service">Service</option>
                        <option value="Article">Article</option>
                        <option value="BlogPosting">Blog Post</option>
                        <option value="NewsArticle">News Article</option>
                        <option value="Recipe">Recipe</option>
                        <option value="Course">Course</option>
                        <option value="Event">Event</option>
                        <option value="Movie">Movie</option>
                        <option value="Book">Book</option>
                        <option value="SoftwareApplication">Software Application</option>
                        <option value="WebSite">Website</option>
                        <option value="WebPage">Web Page</option>
                        <option value="FAQPage">FAQ Page</option>
                        <option value="HowTo">How-To</option>
                        <option value="JobPosting">Job Posting</option>
                    </select>
                </div>

                <div class="btn-group">
                    <button class="btn btn-primary" onclick="generateSchema()">Generate Schema</button>
                    <button class="btn btn-success" onclick="saveSchema()">Save Schema</button>
                    <button class="btn btn-info" onclick="showLoadModal()">Load Saved</button>
                    <button class="btn btn-secondary" onclick="clearForm()">Clear Form</button>
                    <button class="btn btn-warning" onclick="fillRedBoxDemo()">Demo Data</button>
                </div>

                <form id="schemaForm">
                    <input type="hidden" id="currentSchemaId" value="">

                    <!-- Basic Information -->
                    <fieldset class="fieldset">
                        <legend>Basic Information</legend>
                        <div class="form-group">
                            <label for="name">Name *</label>
                            <input type="text" id="name" placeholder="Business/Item Name" oninput="updatePreview()">
                        </div>
                        <div class="form-group">
                            <label for="description">Description</label>
                            <textarea id="description" placeholder="Brief description" oninput="updatePreview()"></textarea>
                        </div>
                        <div class="form-group">
                            <label for="url">URL</label>
                            <input type="url" id="url" placeholder="https://example.com" oninput="updatePreview()">
                        </div>
                        <div class="form-group">
                            <label for="image">Image URL</label>
                            <input type="url" id="image" placeholder="https://example.com/image.jpg" oninput="updatePreview()">
                        </div>
                    </fieldset>

                    <!-- Contact Information -->
                    <fieldset class="fieldset" id="contactSection">
                        <legend>Contact Information</legend>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="telephone">Telephone</label>
                                <input type="tel" id="telephone" placeholder="+1-555-123-4567" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="email">Email</label>
                                <input type="email" id="email" placeholder="contact@example.com" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Address -->
                    <fieldset class="fieldset" id="addressSection">
                        <legend>Address</legend>
                        <div class="form-group">
                            <label for="streetAddress">Street Address</label>
                            <input type="text" id="streetAddress" placeholder="123 Main Street" oninput="updatePreview()">
                        </div>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="addressLocality">City</label>
                                <input type="text" id="addressLocality" placeholder="New York" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="addressRegion">State/Region</label>
                                <input type="text" id="addressRegion" placeholder="NY" oninput="updatePreview()">
                            </div>
                        </div>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="postalCode">Postal Code</label>
                                <input type="text" id="postalCode" placeholder="10001" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="addressCountry">Country</label>
                                <input type="text" id="addressCountry" placeholder="US" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Geo Coordinates -->
                    <fieldset class="fieldset" id="geoSection">
                        <legend>Geographic Coordinates</legend>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="latitude">Latitude</label>
                                <input type="number" step="any" id="latitude" placeholder="40.7128" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="longitude">Longitude</label>
                                <input type="number" step="any" id="longitude" placeholder="-74.0060" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Contact Point Section -->
                    <fieldset class="fieldset" id="contactPointSection">
                        <legend>Contact Point</legend>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="contactTelephone">Contact Telephone</label>
                                <input type="tel" id="contactTelephone" placeholder="+852-2556-1116" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="contactType">Contact Type</label>
                                <select id="contactType" onchange="updatePreview()">
                                    <option value="">Select Type</option>
                                    <option value="customer service">Customer Service</option>
                                    <option value="technical support">Technical Support</option>
                                    <option value="billing support">Billing Support</option>
                                    <option value="emergency">Emergency</option>
                                    <option value="sales">Sales</option>
                                </select>
                            </div>
                        </div>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="areaServed">Area Served</label>
                                <input type="text" id="areaServed" placeholder="HK" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="availableLanguage">Available Languages (comma-separated)</label>
                                <input type="text" id="availableLanguage" placeholder="English, 中文, Cantonese" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Map & Location Section -->
                    <fieldset class="fieldset" id="mapSection">
                        <legend>Map & Location</legend>
                        <div class="form-group">
                            <label for="hasMap">Google Maps URL</label>
                            <input type="url" id="hasMap" placeholder="https://www.google.com/maps/place/22.3842865,114.2061466" oninput="updatePreview()">
                        </div>
                        <div class="form-group">
                            <label>Quick Actions</label>
                            <div class="btn-group">
                                <button type="button" class="btn btn-secondary btn-sm" onclick="generateMapUrl()">Generate from Coordinates</button>
                                <button type="button" class="btn btn-secondary btn-sm" onclick="generateMapFromAddress()">Generate from Address</button>
                            </div>
                        </div>
                    </fieldset>

                    <!-- Amenity Features Section -->
                    <fieldset class="fieldset" id="amenitySection">
                        <legend>Amenity Features</legend>
                        <div class="form-group">
                            <label>Standard Features</label>
                            <div class="checkbox-group">
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenity24Access" value="24/7 Access" onchange="updatePreview()">
                                    <label for="amenity24Access">24/7 Access</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenityClimate" value="Climate-Controlled Units" onchange="updatePreview()">
                                    <label for="amenityClimate">Climate-Controlled</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenityCCTV" value="CCTV Surveillance" onchange="updatePreview()">
                                    <label for="amenityCCTV">CCTV Surveillance</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenitySecurity" value="Security Guards" onchange="updatePreview()">
                                    <label for="amenitySecurity">Security Guards</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenityElevator" value="Elevator Access" onchange="updatePreview()">
                                    <label for="amenityElevator">Elevator Access</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="amenityParking" value="Free Parking" onchange="updatePreview()">
                                    <label for="amenityParking">Free Parking</label>
                                </div>
                            </div>
                        </div>
                        <div class="form-group">
                            <label for="customAmenities">Custom Amenities (one per line)</label>
                            <textarea id="customAmenities" placeholder="Wi-Fi Available&#10;Loading Dock&#10;Storage Supplies" oninput="updatePreview()"></textarea>
                        </div>
                    </fieldset>

                    <!-- Payment & Currency Section -->
                    <fieldset class="fieldset" id="paymentSection">
                        <legend>Payment & Currency</legend>
                        <div class="form-group">
                            <label>Payment Methods Accepted</label>
                            <div class="checkbox-group">
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentCash" value="Cash" onchange="updatePreview()">
                                    <label for="paymentCash">Cash</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentCredit" value="Credit Card" onchange="updatePreview()">
                                    <label for="paymentCredit">Credit Card</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentDebit" value="Debit Card" onchange="updatePreview()">
                                    <label for="paymentDebit">Debit Card</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentOctopus" value="Octopus" onchange="updatePreview()">
                                    <label for="paymentOctopus">Octopus</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentFPS" value="FPS" onchange="updatePreview()">
                                    <label for="paymentFPS">FPS (Faster Payment)</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentAlipay" value="Alipay" onchange="updatePreview()">
                                    <label for="paymentAlipay">Alipay</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="paymentWechat" value="WeChat Pay" onchange="updatePreview()">
                                    <label for="paymentWechat">WeChat Pay</label>
                                </div>
                            </div>
                        </div>
                        <div class="form-group">
                            <label for="currenciesAccepted">Currencies Accepted</label>
                            <input type="text" id="currenciesAccepted" placeholder="HKD" value="HKD" oninput="updatePreview()">
                        </div>
                    </fieldset>

                    <!-- Enhanced Media Section -->
                    <fieldset class="fieldset" id="mediaSection">
                        <legend>Logo & Images</legend>
                        <div class="form-group">
                            <label for="logo">Logo URL</label>
                            <input type="url" id="logo" placeholder="https://www.redboxstorage.com.hk/images/logo-redbox.png" oninput="updatePreview()">
                        </div>
                        <div class="form-group">
                            <label for="additionalImages">Additional Images (one URL per line)</label>
                            <textarea id="additionalImages" placeholder="https://www.redboxstorage.com.hk/images/exterior.jpg&#10;https://www.redboxstorage.com.hk/images/interior.jpg&#10;https://www.redboxstorage.com.hk/images/units.jpg" oninput="updatePreview()"></textarea>
                        </div>
                        <div class="form-group">
                            <label>Image Preview</label>
                            <div id="imagePreview" class="image-preview-container">
                                <p style="color: #666; font-style: italic;">No images added yet</p>
                            </div>
                        </div>
                    </fieldset>

                    <!-- Opening Hours -->
                    <fieldset class="fieldset" id="hoursSection">
                        <legend>Opening Hours</legend>
                        <div class="form-group">
                            <label>Days of Week</label>
                            <div class="checkbox-group">
                                <div class="checkbox-item">
                                    <input type="checkbox" id="monday" value="Monday" checked onchange="updatePreview()">
                                    <label for="monday">Monday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="tuesday" value="Tuesday" checked onchange="updatePreview()">
                                    <label for="tuesday">Tuesday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="wednesday" value="Wednesday" checked onchange="updatePreview()">
                                    <label for="wednesday">Wednesday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="thursday" value="Thursday" checked onchange="updatePreview()">
                                    <label for="thursday">Thursday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="friday" value="Friday" checked onchange="updatePreview()">
                                    <label for="friday">Friday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="saturday" value="Saturday" onchange="updatePreview()">
                                    <label for="saturday">Saturday</label>
                                </div>
                                <div class="checkbox-item">
                                    <input type="checkbox" id="sunday" value="Sunday" onchange="updatePreview()">
                                    <label for="sunday">Sunday</label>
                                </div>
                            </div>
                        </div>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="opens">Opens</label>
                                <input type="time" id="opens" value="09:00" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="closes">Closes</label>
                                <input type="time" id="closes" value="17:00" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Rating -->
                    <fieldset class="fieldset" id="ratingSection">
                        <legend>Rating</legend>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="ratingValue">Rating Value (1-5)</label>
                                <input type="number" step="0.1" min="1" max="5" id="ratingValue" placeholder="4.5" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="reviewCount">Review Count</label>
                                <input type="number" id="reviewCount" placeholder="150" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Price Range -->
                    <fieldset class="fieldset" id="priceSection">
                        <legend>Pricing</legend>
                        <div class="form-row">
                            <div class="form-group">
                                <label for="priceRange">Price Range</label>
                                <input type="text" id="priceRange" placeholder="$10-$50" oninput="updatePreview()">
                            </div>
                            <div class="form-group">
                                <label for="priceCurrency">Currency</label>
                                <input type="text" id="priceCurrency" placeholder="USD" oninput="updatePreview()">
                            </div>
                        </div>
                    </fieldset>

                    <!-- Social Media -->
                    <fieldset class="fieldset" id="socialSection">
                        <legend>Social Media</legend>
                        <div class="form-group">
                            <label for="sameAs">Social Media URLs (one per line)</label>
                            <textarea id="sameAs" placeholder="https://facebook.com/example&#10;https://twitter.com/example&#10;https://instagram.com/example" oninput="updatePreview()"></textarea>
                        </div>
                    </fieldset>

                    <!-- Schema-specific sections will be dynamically added here -->
                    <div id="dynamicSections"></div>
                </form>
            </div>

            <div class="preview-section">
                <h2>Live Preview</h2>

                <div class="preview-content">
                    <h3>Search Result Preview</h3>
                    <div id="snippetPreview" class="snippet-preview">
                        <div class="snippet-title">Your Business Name</div>
                        <div class="snippet-url">https://yourwebsite.com</div>
                        <div class="snippet-description">Your business description will appear here...</div>
                        <div class="rating-stars" style="display: none;">
                            ★★★★★ <span class="rating-text">4.5 (150 reviews)</span>
                        </div>
                    </div>
                </div>

                <div class="preview-content">
                    <h3>JSON-LD Schema</h3>
                    <div id="jsonPreview" class="json-preview">
                        Click "Generate Schema" to see the JSON-LD markup
                    </div>
                </div>

                <div class="btn-group">
                    <button class="btn btn-secondary" onclick="copyToClipboard()">Copy JSON-LD</button>
                    <button class="btn btn-info" onclick="downloadSchema()">Download</button>
                    <button class="btn btn-validate" onclick="validateCurrentSchema()">Validate Schema</button>
                </div>

                <!-- Validation Results -->
                <div id="validationResults" style="display: none;">
                    <div class="preview-content">
                        <h3>Validation Results</h3>
                        <div id="validationContent"></div>
                    </div>
                </div>
            </div>
        </div>

        <div class="saved-schemas">
            <h2>Saved Schemas</h2>
            <div id="savedList">
                <?php if (empty($savedSchemas)): ?>
                    <p>No saved schemas yet. Create and save your first schema above!</p>
                <?php else: ?>
                    <?php foreach ($savedSchemas as $schema): ?>
                        <div class="saved-item">
                            <div class="saved-item-info">
                                <h4><?php echo htmlspecialchars($schema['name'], ENT_QUOTES, 'UTF-8'); ?></h4>
                                <p>Type: <?php echo htmlspecialchars($schema['schema_type'], ENT_QUOTES, 'UTF-8'); ?> |
                                   Created: <?php echo date('M j, Y g:i A', strtotime($schema['created_at'])); ?></p>
                            </div>
                            <div class="saved-item-actions">
                                <button class="btn btn-info btn-sm" onclick="loadSchema(<?php echo intval($schema['id']); ?>)">Load</button>
                                <button class="btn btn-secondary btn-sm" onclick="copySchema(<?php echo intval($schema['id']); ?>)">Copy</button>
                                <button class="btn btn-danger btn-sm" onclick="deleteSchema(<?php echo intval($schema['id']); ?>)">Delete</button>
                            </div>
                        </div>
                    <?php endforeach; ?>
                <?php endif; ?>
            </div>
        </div>
    </div>

    <!-- Load Modal -->
    <div id="loadModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>Load Saved Schema</h2>
                <span class="close" onclick="closeLoadModal()">&times;</span>
            </div>
            <div id="modalSchemaList">
                <!-- Schema list will be populated here -->
            </div>
        </div>
    </div>

    <!-- Validation Modal -->
    <div id="validationModal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <h2>Schema Validation Results</h2>
                <span class="close" onclick="closeValidationModal()">&times;</span>
            </div>
            <div id="modalValidationContent">
                <!-- Validation results will be populated here -->
            </div>
        </div>
    </div>

    <script>
        var currentSchema = {};
        var currentSchemaId = null;

        // Enhanced Schema type configurations
        var schemaConfigs = {
            LocalBusiness: {
                sections: ['contact', 'address', 'geo', 'contactPoint', 'map', 'amenity', 'payment', 'media', 'hours', 'rating', 'price', 'social'],
                required: ['name', 'address']
            },
            Restaurant: {
                sections: ['contact', 'address', 'geo', 'contactPoint', 'map', 'amenity', 'payment', 'media', 'hours', 'rating', 'price', 'social'],
                additional: ['servesCuisine', 'menu']
            },
            Product: {
                sections: ['rating', 'price', 'media'],
                additional: ['brand', 'model', 'sku', 'gtin']
            },
            Article: {
                sections: ['social', 'media'],
                additional: ['author', 'publisher', 'datePublished', 'headline']
            },
            Recipe: {
                sections: ['rating', 'media'],
                additional: ['cookTime', 'prepTime', 'recipeYield', 'recipeIngredient', 'recipeInstructions']
            },
            Event: {
                sections: ['address', 'geo', 'contactPoint', 'map', 'media', 'price'],
                additional: ['startDate', 'endDate', 'eventStatus', 'organizer']
            }
        };

        // Helper functions
        function getValue(id) {
            const element = document.getElementById(id);
            return element ? element.value.trim() : '';
        }

        function isValidUrl(string) {
            try {
                new URL(string);
                return true;
            } catch (_) {
                return false;
            }
        }

        function showAlert(message, type = 'success') {
            const container = document.getElementById('alertContainer');
            const alert = document.createElement('div');
            alert.className = `alert alert-${type}`;
            alert.textContent = message;
            container.appendChild(alert);

            setTimeout(() => {
                alert.remove();
            }, 5000);
        }

        function setLoading(loading) {
            document.body.classList.toggle('loading', loading);
        }

        function switchSchemaType() {
            var schemaType = document.getElementById('schemaType').value;
            var config = schemaConfigs[schemaType] || schemaConfigs.LocalBusiness;

            // Hide all sections first
            var sections = [
                'contactSection', 'addressSection', 'geoSection', 'hoursSection', 'ratingSection', 
                'priceSection', 'socialSection', 'contactPointSection', 'mapSection', 
                'amenitySection', 'paymentSection', 'mediaSection'
            ];
            
            for (var i = 0; i < sections.length; i++) {
                var element = document.getElementById(sections[i]);
                if (element) {
                    element.style.display = 'none';
                }
            }

            // Show relevant sections
            if (config.sections) {
                var sectionMap = {
                    'contact': 'contactSection',
                    'address': 'addressSection',
                    'geo': 'geoSection',
                    'hours': 'hoursSection',
                    'rating': 'ratingSection',
                    'price': 'priceSection',
                    'social': 'socialSection',
                    'contactPoint': 'contactPointSection',
                    'map': 'mapSection',
                    'amenity': 'amenitySection',
                    'payment': 'paymentSection',
                    'media': 'mediaSection'
                };

                for (var j = 0; j < config.sections.length; j++) {
                    var section = config.sections[j];
                    var elementId = sectionMap[section];
                    var element = document.getElementById(elementId);
                    if (element) {
                        element.style.display = 'block';
                    }
                }
            }

            // Add schema-specific fields
            addDynamicFields(schemaType, config);

            // Update preview
            updatePreview();
        }

        function addDynamicFields(schemaType, config) {
            const dynamicSection = document.getElementById('dynamicSections');
            dynamicSection.innerHTML = '';

            if (config.additional) {
                const fieldset = document.createElement('fieldset');
                fieldset.className = 'fieldset';
                fieldset.innerHTML = `<legend>${schemaType} Specific Fields</legend>`;

                config.additional.forEach(field => {
                    const formGroup = document.createElement('div');
                    formGroup.className = 'form-group';

                    const label = document.createElement('label');
                    label.textContent = formatFieldName(field);
                    label.setAttribute('for', field);

                    let input;
                    if (field.includes('Date')) {
                        input = document.createElement('input');
                        input.type = 'datetime-local';
                    } else if (field.includes('Time')) {
                        input = document.createElement('input');
                        input.type = 'time';
                    } else if (['recipeIngredient', 'recipeInstructions'].includes(field)) {
                        input = document.createElement('textarea');
                        input.placeholder = field === 'recipeIngredient' ? 'One ingredient per line' : 'One instruction per line';
                    } else {
                        input = document.createElement('input');
                        input.type = 'text';
                    }

                    input.id = field;
                    input.addEventListener('input', updatePreview);

                    formGroup.appendChild(label);
                    formGroup.appendChild(input);
                    fieldset.appendChild(formGroup);
                });

                dynamicSection.appendChild(fieldset);
            }
        }

        function formatFieldName(field) {
            return field.replace(/([A-Z])/g, ' $1').replace(/^./, str => str.toUpperCase());
        }

        // Enhanced generateSchema function
        function generateSchema() {
            var schemaType = document.getElementById('schemaType').value;
            var schema = {
                '@context': 'https://schema.org',
                '@type': schemaType
            };

            // Basic fields
            var basicFields = ['name', 'description', 'url'];
            for (var i = 0; i < basicFields.length; i++) {
                var field = basicFields[i];
                var element = document.getElementById(field);
                var value = element ? element.value.trim() : '';
                if (value) schema[field] = value;
            }

            // Contact information
            var contactFields = ['telephone', 'email'];
            for (var i = 0; i < contactFields.length; i++) {
                var field = contactFields[i];
                var element = document.getElementById(field);
                var value = element ? element.value.trim() : '';
                if (value) schema[field] = value;
            }

            // Address
            var addressFields = ['streetAddress', 'addressLocality', 'addressRegion', 'postalCode', 'addressCountry'];
            var address = {};
            var hasAddress = false;

            for (var i = 0; i < addressFields.length; i++) {
                var field = addressFields[i];
                var element = document.getElementById(field);
                var value = element ? element.value.trim() : '';
                if (value) {
                    address[field] = value;
                    hasAddress = true;
                }
            }

            if (hasAddress) {
                schema.address = {
                    '@type': 'PostalAddress'
                };
                for (var key in address) {
                    schema.address[key] = address[key];
                }
            }

            // Geo coordinates
            var latElement = document.getElementById('latitude');
            var lngElement = document.getElementById('longitude');
            var lat = latElement ? latElement.value.trim() : '';
            var lng = lngElement ? lngElement.value.trim() : '';

            if (lat && lng) {
                schema.geo = {
                    '@type': 'GeoCoordinates',
                    latitude: parseFloat(lat),
                    longitude: parseFloat(lng)
                };
            }

            // Contact Point
            const contactTelephone = getValue('contactTelephone');
            const contactType = getValue('contactType');
            const areaServed = getValue('areaServed');
            const availableLanguage = getValue('availableLanguage');
            
            if (contactTelephone || contactType || areaServed || availableLanguage) {
                schema.contactPoint = {
                    '@type': 'ContactPoint'
                };
                if (contactTelephone) schema.contactPoint.telephone = contactTelephone;
                if (contactType) schema.contactPoint.contactType = contactType;
                if (areaServed) schema.contactPoint.areaServed = areaServed;
                if (availableLanguage) {
                    const languages = availableLanguage.split(',').map(lang => lang.trim()).filter(Boolean);
                    if (languages.length > 0) schema.contactPoint.availableLanguage = languages;
                }
            }

            // Map
            const hasMap = getValue('hasMap');
            if (hasMap) schema.hasMap = hasMap;

            // Amenity Features
            const amenityFeatures = [];
            const amenityCheckboxes = [
                'amenity24Access', 'amenityClimate', 'amenityCCTV', 
                'amenitySecurity', 'amenityElevator', 'amenityParking'
            ];
            
            amenityCheckboxes.forEach(id => {
                const checkbox = document.getElementById(id);
                if (checkbox && checkbox.checked) {
                    amenityFeatures.push({
                        '@type': 'LocationFeatureSpecification',
                        name: checkbox.value,
                        value: true
                    });
                }
            });
            
            // Add custom amenities
            const customAmenities = getValue('customAmenities');
            if (customAmenities) {
                const custom = customAmenities.split('\n').filter(line => line.trim());
                custom.forEach(amenity => {
                    amenityFeatures.push({
                        '@type': 'LocationFeatureSpecification',
                        name: amenity.trim(),
                        value: true
                    });
                });
            }
            
            if (amenityFeatures.length > 0) {
                schema.amenityFeature = amenityFeatures;
            }

            // Payment Methods
            const paymentMethods = [];
            const paymentCheckboxes = [
                'paymentCash', 'paymentCredit', 'paymentDebit', 
                'paymentOctopus', 'paymentFPS', 'paymentAlipay', 'paymentWechat'
            ];
            
            paymentCheckboxes.forEach(id => {
                const checkbox = document.getElementById(id);
                if (checkbox && checkbox.checked) {
                    paymentMethods.push(checkbox.value);
                }
            });
            
            if (paymentMethods.length > 0) {
                schema.paymentAccepted = paymentMethods;
            }
            
            const currenciesAccepted = getValue('currenciesAccepted');
            if (currenciesAccepted) schema.currenciesAccepted = currenciesAccepted;

            // Enhanced Images & Logo
            const logo = getValue('logo');
            if (logo) schema.logo = logo;
            
            // Handle main image and additional images
            const mainImage = getValue('image');
            const additionalImages = getValue('additionalImages');
            
            if (mainImage || additionalImages) {
                const imageArray = [];
                if (mainImage) imageArray.push(mainImage);
                if (additionalImages) {
                    const additional = additionalImages.split('\n')
                        .map(url => url.trim())
                        .filter(url => url && isValidUrl(url));
                    imageArray.push(...additional);
                }
                
                if (imageArray.length === 1) {
                    schema.image = imageArray[0];
                } else if (imageArray.length > 1) {
                    schema.image = imageArray;
                }
            }

            // Opening hours
            var selectedDays = [];
            var dayNames = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'];

            for (var i = 0; i < dayNames.length; i++) {
                var checkbox = document.getElementById(dayNames[i]);
                if (checkbox && checkbox.checked) {
                    selectedDays.push(dayNames[i].charAt(0).toUpperCase() + dayNames[i].slice(1));
                }
            }

            var opensElement = document.getElementById('opens');
            var closesElement = document.getElementById('closes');
            var opens = opensElement ? opensElement.value : '';
            var closes = closesElement ? closesElement.value : '';

            if (selectedDays.length && opens && closes) {
                schema.openingHoursSpecification = {
                    '@type': 'OpeningHoursSpecification',
                    dayOfWeek: selectedDays,
                    opens: opens,
                    closes: closes
                };
            }

            // Rating
            var ratingValueElement = document.getElementById('ratingValue');
            var reviewCountElement = document.getElementById('reviewCount');
            var ratingValue = ratingValueElement ? ratingValueElement.value : '';
            var reviewCount = reviewCountElement ? reviewCountElement.value : '';

            if (ratingValue || reviewCount) {
                var aggregateRating = { '@type': 'AggregateRating' };
                if (ratingValue) aggregateRating.ratingValue = parseFloat(ratingValue);
                if (reviewCount) aggregateRating.reviewCount = parseInt(reviewCount);
                schema.aggregateRating = aggregateRating;
            }

            // Price range
            var priceRangeElement = document.getElementById('priceRange');
            var priceRange = priceRangeElement ? priceRangeElement.value.trim() : '';
            if (priceRange) schema.priceRange = priceRange;

            // Social media
            var sameAsElement = document.getElementById('sameAs');
            var sameAsText = sameAsElement ? sameAsElement.value.trim() : '';
            if (sameAsText) {
                var sameAsUrls = sameAsText.split('\n');
                var cleanUrls = [];
                for (var i = 0; i < sameAsUrls.length; i++) {
                    var url = sameAsUrls[i].trim();
                    if (url) cleanUrls.push(url);
                }
                if (cleanUrls.length) schema.sameAs = cleanUrls;
            }

            // Dynamic fields
            var dynamicInputs = document.querySelectorAll('#dynamicSections input, #dynamicSections textarea');
            for (var i = 0; i < dynamicInputs.length; i++) {
                var input = dynamicInputs[i];
                var value = input.value.trim();
                if (value) {
                    if (input.id.includes('Ingredient') || input.id.includes('Instructions')) {
                        var lines = value.split('\n');
                        var cleanLines = [];
                        for (var j = 0; j < lines.length; j++) {
                            var line = lines[j].trim();
                            if (line) cleanLines.push(line);
                        }
                        schema[input.id] = cleanLines;
                    } else {
                        schema[input.id] = value;
                    }
                }
            }

            currentSchema = schema;

            // Update displays
            document.getElementById('jsonPreview').textContent = JSON.stringify(schema, null, 2);
            updateSnippetPreview(schema);
            updateImagePreview();
            
            // Hide validation results when schema changes
            document.getElementById('validationResults').style.display = 'none';
        }

        // Validation function
        async function validateCurrentSchema() {
            const jsonText = document.getElementById('jsonPreview').textContent;
            
            if (jsonText.includes('Click') && jsonText.includes('Generate Schema')) {
                showAlert('Please generate a schema first', 'danger');
                return;
            }

            setLoading(true);
            showAlert('Validating schema with schema.org validator...', 'info');

            try {
                const formData = new FormData();
                formData.append('action', 'validate');
                formData.append('schema_data', jsonText);

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success && result.validation.success) {
                    displayValidationResults(result.validation.data);
                } else {
                    showAlert(result.validation?.error || result.error || 'Validation failed', 'danger');
                }
            } catch (error) {
                console.error('Validation error:', error);
                showAlert('Error during validation: ' + error.message, 'danger');
            } finally {
                setLoading(false);
            }
        }

        function displayValidationResults(validationData) {
            const validationContainer = document.getElementById('validationContent');
            const validationResults = document.getElementById('validationResults');
            
            let html = '';
            
            if (validationData.totalNumErrors === 0 && validationData.totalNumWarnings === 0) {
                html = `
                    <div class="validation-result validation-success">
                        <h4>✅ Schema Validation Passed!</h4>
                        <p>Your schema markup is valid and ready to use. No errors or warnings found.</p>
                        <div class="validation-details">
                            <strong>Objects processed:</strong> ${validationData.numObjects}<br>
                            <strong>Schema type:</strong> ${validationData.tripleGroups?.[0]?.type || 'Unknown'}
                        </div>
                    </div>
                `;
            } else {
                html = `
                    <div class="validation-result validation-error">
                        <h4>⚠️ Schema Validation Issues Found</h4>
                        <p><strong>Errors:</strong> ${validationData.totalNumErrors} | <strong>Warnings:</strong> ${validationData.totalNumWarnings}</p>
                `;
                
                if (validationData.tripleGroups && validationData.tripleGroups.length > 0) {
                    validationData.tripleGroups.forEach(group => {
                        if (group.errors && group.errors.length > 0) {
                            html += '<div class="validation-details"><h5>Errors:</h5>';
                            group.errors.forEach(error => {
                                html += `<div class="error-item">${error}</div>`;
                            });
                            html += '</div>';
                        }
                        
                        if (group.nodes && group.nodes.length > 0) {
                            group.nodes.forEach(node => {
                                if (node.errors && node.errors.length > 0) {
                                    html += '<div class="validation-details"><h5>Node Errors:</h5>';
                                    node.errors.forEach(error => {
                                        html += `<div class="error-item">${error}</div>`;
                                    });
                                    html += '</div>';
                                }
                            });
                        }
                    });
                }
                
                html += '</div>';
            }
            
            validationContainer.innerHTML = html;
            validationResults.style.display = 'block';
            
            // Show success/error message
            if (validationData.totalNumErrors === 0) {
                showAlert('Schema validation completed successfully!', 'success');
            } else {
                showAlert(`Schema validation found ${validationData.totalNumErrors} error(s)`, 'warning');
            }
        }

        function updatePreview() {
            // Debounce the update to avoid excessive calls
            clearTimeout(updatePreview.timeout);
            updatePreview.timeout = setTimeout(generateSchema, 300);
        }

        function updateSnippetPreview(schema) {
            const preview = document.getElementById('snippetPreview');
            const title = preview.querySelector('.snippet-title');
            const url = preview.querySelector('.snippet-url');
            const description = preview.querySelector('.snippet-description');
            const rating = preview.querySelector('.rating-stars');

            title.textContent = schema.name || 'Your Business Name';
            url.textContent = schema.url || 'https://yourwebsite.com';
            description.textContent = schema.description || 'Your business description will appear here...';

            if (schema.aggregateRating) {
                const stars = '★'.repeat(Math.floor(schema.aggregateRating.ratingValue || 0)) +
                             '☆'.repeat(5 - Math.floor(schema.aggregateRating.ratingValue || 0));
                rating.innerHTML = stars + ' <span class="rating-text">' + (schema.aggregateRating.ratingValue || '') + ' (' + (schema.aggregateRating.reviewCount || '') + ' reviews)</span>';
                rating.style.display = 'block';
            } else {
                rating.style.display = 'none';
            }
        }

        // Update image preview
        function updateImagePreview() {
            const imageUrls = [];
            
            // Add main image
            const mainImage = getValue('image');
            if (mainImage) imageUrls.push(mainImage);
            
            // Add logo
            const logo = getValue('logo');
            if (logo) imageUrls.push(logo);
            
            // Add additional images
            const additionalImages = getValue('additionalImages');
            if (additionalImages) {
                const urls = additionalImages.split('\n').filter(url => url.trim());
                imageUrls.push(...urls);
            }
            
            const previewContainer = document.getElementById('imagePreview');
            if (!previewContainer) return;
            
            previewContainer.innerHTML = '';
            
            imageUrls.forEach((url, index) => {
                if (url.trim()) {
                    const imgContainer = document.createElement('div');
                    imgContainer.style.cssText = 'position: relative; display: inline-block;';
                    
                    const img = document.createElement('img');
                    img.src = url.trim();
                    img.style.cssText = 'width: 80px; height: 60px; object-fit: cover; border: 1px solid #ddd; border-radius: 4px; display: block;';
                    img.title = url.trim();
                    
                    img.onerror = function() {
                        this.style.border = '1px solid #dc3545';
                        this.alt = 'Failed to load';
                        this.title = 'Failed to load: ' + url.trim();
                    };
                    
                    // Add image type label
                    const label = document.createElement('div');
                    label.style.cssText = 'position: absolute; bottom: 0; left: 0; right: 0; background: rgba(0,0,0,0.7); color: white; font-size: 10px; padding: 2px; text-align: center; border-radius: 0 0 4px 4px;';
                    
                    if (index === 0 && mainImage) {
                        label.textContent = 'Main';
                    } else if (url === logo) {
                        label.textContent = 'Logo';
                    } else {
                        label.textContent = `Image ${index + 1}`;
                    }
                    
                    imgContainer.appendChild(img);
                    imgContainer.appendChild(label);
                    previewContainer.appendChild(imgContainer);
                }
            });
            
            if (imageUrls.length === 0) {
                previewContainer.innerHTML = '<p style="color: #666; font-style: italic;">No images added yet</p>';
            }
        }

        // Generate Google Maps URL from coordinates
        function generateMapUrl() {
            const lat = document.getElementById('latitude').value;
            const lng = document.getElementById('longitude').value;
            
            if (lat && lng) {
                const mapUrl = `https://www.google.com/maps/place/${lat},${lng}`;
                document.getElementById('hasMap').value = mapUrl;
                updatePreview();
                showAlert('Map URL generated from coordinates!');
            } else {
                showAlert('Please enter latitude and longitude first', 'danger');
            }
        }

        // Generate Google Maps URL from address
        function generateMapFromAddress() {
            const streetAddress = document.getElementById('streetAddress').value;
            const city = document.getElementById('addressLocality').value;
            const region = document.getElementById('addressRegion').value;
            
            if (streetAddress || city) {
                const address = [streetAddress, city, region].filter(Boolean).join(', ');
                const mapUrl = `https://www.google.com/maps/search/${encodeURIComponent(address)}`;
                document.getElementById('hasMap').value = mapUrl;
                updatePreview();
                showAlert('Map URL generated from address!');
            } else {
                showAlert('Please enter an address first', 'danger');
            }
        }

        // Quick fill for RedBox Storage demo
        function fillRedBoxDemo() {
            // Basic info
            document.getElementById('name').value = 'RedBox Storage - Sha Tin';
            document.getElementById('description').value = 'Self storage facility in Sha Tin offering 24/7 access, climate-controlled units, and comprehensive security features.';
            document.getElementById('url').value = 'https://www.redboxstorage.com.hk/locations/shatin';
            
            // Address
            document.getElementById('streetAddress').value = 'Shop 107, 1/F, CityOne Plaza';
            document.getElementById('addressLocality').value = 'Sha Tin';
            document.getElementById('addressRegion').value = 'New Territories';
            document.getElementById('postalCode').value = '';
            document.getElementById('addressCountry').value = 'HK';
            
            // Coordinates
            document.getElementById('latitude').value = '22.3842865';
            document.getElementById('longitude').value = '114.2061466';
            
            // Contact
            document.getElementById('telephone').value = '+852-2556-1116';
            document.getElementById('email').value = 'info@redboxstorage.com.hk';
            
            // Contact Point
            document.getElementById('contactTelephone').value = '+852-2556-1116';
            document.getElementById('contactType').value = 'customer service';
            document.getElementById('areaServed').value = 'HK';
            document.getElementById('availableLanguage').value = 'English, 中文';
            
            // Hours
            ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'].forEach(day => {
                document.getElementById(day).checked = true;
            });
            document.getElementById('opens').value = '00:00';
            document.getElementById('closes').value = '23:59';
            
            // Amenities
            ['amenity24Access', 'amenityClimate', 'amenityCCTV', 'amenitySecurity'].forEach(id => {
                document.getElementById(id).checked = true;
            });
            
            // Payment
            ['paymentCash', 'paymentCredit', 'paymentOctopus', 'paymentFPS'].forEach(id => {
                document.getElementById(id).checked = true;
            });
            document.getElementById('currenciesAccepted').value = 'HKD';
            
            // Images
            document.getElementById('logo').value = 'https://www.redboxstorage.com.hk/images/logo-redbox.png';
            document.getElementById('image').value = 'https://www.redboxstorage.com.hk/images/shatin-cityone-exterior.jpg';
            document.getElementById('additionalImages').value = 'https://www.redboxstorage.com.hk/images/shatin-cityone-interior.jpg\nhttps://www.redboxstorage.com.hk/images/shatin-cityone-units.jpg';
            
            // Rating
            document.getElementById('ratingValue').value = '4.5';
            document.getElementById('reviewCount').value = '127';
            
            // Generate map URL
            generateMapUrl();
            
            // Update preview
            updatePreview();
            showAlert('RedBox Storage demo data filled!');
        }

        async function saveSchema() {
            if (!currentSchema || !currentSchema.name) {
                showAlert('Please generate a schema first and ensure it has a name.', 'danger');
                return;
            }

            setLoading(true);

            try {
                const formData = new FormData();
                formData.append('action', currentSchemaId ? 'update' : 'save');
                if (currentSchemaId) {
                    formData.append('id', currentSchemaId);
                }
                formData.append('name', currentSchema.name);
                formData.append('schema_type', currentSchema['@type']);
                formData.append('schema_data', JSON.stringify(currentSchema));
                formData.append('form_data', JSON.stringify(getFormData()));

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success) {
                    showAlert(result.message);
                    if (!currentSchemaId) {
                        currentSchemaId = result.id;
                        document.getElementById('currentSchemaId').value = currentSchemaId;
                    }
                    await loadSavedList();
                } else {
                    showAlert(result.error, 'danger');
                }
            } catch (error) {
                showAlert('Error saving schema: ' + error.message, 'danger');
            } finally {
                setLoading(false);
            }
        }

        function getFormData() {
            const formData = {};
            const inputs = document.querySelectorAll('#schemaForm input, #schemaForm textarea, #schemaForm select');
            inputs.forEach(input => {
                if (input.type === 'checkbox') {
                    formData[input.id] = input.checked;
                } else {
                    formData[input.id] = input.value;
                }
            });
            return formData;
        }

        async function loadSchema(id) {
            setLoading(true);

            try {
                const formData = new FormData();
                formData.append('action', 'load');
                formData.append('id', id);

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success) {
                    const schema = result.schema;
                    currentSchemaId = schema.id;
                    document.getElementById('currentSchemaId').value = currentSchemaId;

                    // Set schema type first
                    document.getElementById('schemaType').value = schema.schema_type;
                    switchSchemaType();

                    // Load form data
                    const formData = JSON.parse(schema.form_data);
                    Object.keys(formData).forEach(key => {
                        const element = document.getElementById(key);
                        if (element) {
                            if (element.type === 'checkbox') {
                                element.checked = formData[key];
                            } else {
                                element.value = formData[key];
                            }
                        }
                    });

                    // Load and display schema
                    currentSchema = JSON.parse(schema.schema_data);
                    document.getElementById('jsonPreview').textContent = JSON.stringify(currentSchema, null, 2);
                    updateSnippetPreview(currentSchema);
                    updateImagePreview();

                    showAlert('Schema loaded successfully');
                    closeLoadModal();
                } else {
                    showAlert(result.error, 'danger');
                }
            } catch (error) {
                showAlert('Error loading schema: ' + error.message, 'danger');
            } finally {
                setLoading(false);
            }
        }

        // Copy schema function
        async function copySchema(id) {
            if (!confirm('This will create a copy of the selected schema. Continue?')) {
                return;
            }

            setLoading(true);

            try {
                const formData = new FormData();
                formData.append('action', 'copy');
                formData.append('id', id);

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success) {
                    showAlert(result.message);
                    await loadSavedList();
                } else {
                    showAlert(result.error, 'danger');
                }
            } catch (error) {
                showAlert('Error copying schema: ' + error.message, 'danger');
            } finally {
                setLoading(false);
            }
        }

        async function deleteSchema(id) {
            if (!confirm('Are you sure you want to delete this schema?')) {
                return;
            }

            setLoading(true);

            try {
                const formData = new FormData();
                formData.append('action', 'delete');
                formData.append('id', id);

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success) {
                    showAlert('Schema deleted successfully');
                    await loadSavedList();

                    // Clear current if it was the deleted one
                    if (currentSchemaId == id) {
                        clearForm();
                    }
                } else {
                    showAlert(result.error, 'danger');
                }
            } catch (error) {
                showAlert('Error deleting schema: ' + error.message, 'danger');
            } finally {
                setLoading(false);
            }
        }

        async function loadSavedList() {
            try {
                const formData = new FormData();
                formData.append('action', 'list');

                const response = await fetch('', {
                    method: 'POST',
                    body: formData
                });

                const result = await response.json();

                if (result.success) {
                    const savedList = document.getElementById('savedList');

                    if (result.schemas.length === 0) {
                        savedList.innerHTML = '<p>No saved schemas yet. Create and save your first schema above!</p>';
                    } else {
                        var htmlContent = '';
                        for (var i = 0; i < result.schemas.length; i++) {
                            var schema = result.schemas[i];
                            htmlContent += '<div class="saved-item">' +
                                '<div class="saved-item-info">' +
                                    '<h4>' + escapeHtml(schema.name) + '</h4>' +
                                    '<p>Type: ' + escapeHtml(schema.schema_type) + ' | ' +
                                       'Created: ' + formatDate(schema.created_at) + '</p>' +
                                '</div>' +
                                '<div class="saved-item-actions">' +
                                    '<button class="btn btn-info btn-sm" onclick="loadSchema(' + schema.id + ')">Load</button>' +
                                    '<button class="btn btn-secondary btn-sm" onclick="copySchema(' + schema.id + ')">Copy</button>' +
                                    '<button class="btn btn-danger btn-sm" onclick="deleteSchema(' + schema.id + ')">Delete</button>' +
                                '</div>' +
                            '</div>';
                        }
                        savedList.innerHTML = htmlContent;
                    }
                }
            } catch (error) {
                console.error('Error loading saved list:', error);
            }
        }

        function showLoadModal() {
            document.getElementById('loadModal').style.display = 'block';
        }

        function closeLoadModal() {
            document.getElementById('loadModal').style.display = 'none';
        }

        function closeValidationModal() {
            document.getElementById('validationModal').style.display = 'none';
        }

        function clearForm() {
            document.getElementById('schemaForm').reset();
            currentSchema = {};
            currentSchemaId = null;
            document.getElementById('currentSchemaId').value = '';
            document.getElementById('jsonPreview').textContent = 'Click "Generate Schema" to see the JSON-LD markup';

            // Reset snippet preview
            const preview = document.getElementById('snippetPreview');
            preview.querySelector('.snippet-title').textContent = 'Your Business Name';
            preview.querySelector('.snippet-url').textContent = 'https://yourwebsite.com';
            preview.querySelector('.snippet-description').textContent = 'Your business description will appear here...';
            preview.querySelector('.rating-stars').style.display = 'none';

            // Reset image preview
            const imagePreview = document.getElementById('imagePreview');
            if (imagePreview) {
                imagePreview.innerHTML = '<p style="color: #666; font-style: italic;">No images added yet</p>';
            }

            // Hide validation results
            document.getElementById('validationResults').style.display = 'none';

            // Reset schema type
            document.getElementById('schemaType').value = 'LocalBusiness';
            switchSchemaType();
        }

        function copyToClipboard() {
            const jsonText = document.getElementById('jsonPreview').textContent;

            if (jsonText.includes('Click') && jsonText.includes('Generate Schema')) {
                showAlert('Please generate a schema first', 'danger');
                return;
            }

            try {
                // Validate JSON and reformat it cleanly
                const parsed = JSON.parse(jsonText);
                const cleanJson = JSON.stringify(parsed, null, 2);

                const openTag = '<script type="application/ld+json">';
                const closeTag = '<' + '/script>';
                const scriptTag = openTag + '\n' + cleanJson + '\n' + closeTag;

                navigator.clipboard.writeText(scriptTag)
                    .then(() => showAlert('Schema markup copied to clipboard!'))
                    .catch(() => showAlert('Failed to copy to clipboard', 'danger'));

            } catch (error) {
                console.error('Invalid JSON:', error);
                showAlert('Invalid JSON. Please regenerate the schema.', 'danger');
            }
        }

        function downloadSchema() {
            const jsonText = document.getElementById('jsonPreview').textContent;
            if (jsonText.includes('Click') && jsonText.includes('Generate Schema')) {
                showAlert('Please generate a schema first', 'danger');
                return;
            }

            try {
                const parsed = JSON.parse(jsonText);
                const cleanJson = JSON.stringify(parsed, null, 2);
                
                const openTag = '<script type="application/ld+json">';
                const closeTag = '<' + '/script>';
                const scriptTag = openTag + '\n' + cleanJson + '\n' + closeTag;
                
                const blob = new Blob([scriptTag], { type: 'text/html' });
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'schema-' + (currentSchema.name || 'markup') + '.html';
                document.body.appendChild(a);
                a.click();
                document.body.removeChild(a);
                URL.revokeObjectURL(url);
            } catch (error) {
                showAlert('Error creating download', 'danger');
            }
        }

        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        function formatDate(dateString) {
            return new Date(dateString).toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric',
                hour: '2-digit',
                minute: '2-digit'
            });
        }

        // Initialize
        document.addEventListener('DOMContentLoaded', function() {
            switchSchemaType();

            // Add image preview updates
            ['image', 'logo', 'additionalImages'].forEach(id => {
                const element = document.getElementById(id);
                if (element) {
                    element.addEventListener('input', updateImagePreview);
                }
            });

            // Close modal when clicking outside
            window.onclick = function(event) {
                const loadModal = document.getElementById('loadModal');
                const validationModal = document.getElementById('validationModal');
                
                if (event.target === loadModal) {
                    closeLoadModal();
                }
                if (event.target === validationModal) {
                    closeValidationModal();
                }
            };
        });
    </script>
</body>
</html>      