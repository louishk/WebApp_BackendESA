<?php
require_once 'config.php';

/**
 * RapidStor API Handler Class
 */
class RapidStorAPI
{
    private $baseUrl;
    private $token;
    private $debug;

    public function __construct($token = null, $debug = false)
    {
        $this->baseUrl = Config::API_BASE_URL;
        $this->token = $token;
        $this->debug = $debug;

        if (empty($this->baseUrl)) {
            throw new Exception("API Base URL not configured.");
        }
    }

    public function setToken($token)
    {
        $this->token = $token;
    }

    public function hasValidToken()
    {
        return !empty($this->token) && $this->token !== 'your_jwt_token_here';
    }

    private function log($message)
    {
        if ($this->debug) {
            error_log("[RapidStorAPI] " . $message);
        }
    }

    private function makeRequest($endpoint, $method = 'GET', $data = null)
    {
        if (empty($this->token) && !in_array($endpoint, ['/auth/login', '/rapidstor/status'])) {
            throw new Exception("Authentication required. Please provide a JWT token.");
        }

        $url = $this->baseUrl . $endpoint;
        $this->log("Making {$method} request to: {$url}");

        $headers = [
            'Content-Type: application/json',
            'Accept: application/json',
            'User-Agent: RapidStor-PHP-Client/1.0'
        ];

        if (!empty($this->token)) {
            $headers[] = 'Authorization: Bearer ' . trim($this->token);
            $this->log("Using JWT token: " . substr($this->token, 0, 20) . "...");
        }

        $ch = curl_init();
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_HTTPHEADER => $headers,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_SSL_VERIFYPEER => false,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_VERBOSE => false
        ]);

        if ($data && in_array($method, ['POST', 'PUT', 'PATCH'])) {
            $jsonData = json_encode($data);
            curl_setopt($ch, CURLOPT_POSTFIELDS, $jsonData);
            $this->log("Request data: " . substr($jsonData, 0, 500));
        }

        $response = curl_exec($ch);
        $httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $error = curl_error($ch);
        $info = curl_getinfo($ch);
        curl_close($ch);

        if ($error) {
            $this->log("cURL Error: {$error}");
            throw new Exception("cURL Error: $error");
        }

        $this->log("Response HTTP {$httpCode}: " . substr($response, 0, 500));

        $decoded = json_decode($response, true);
        if ($response && $decoded === null && json_last_error() !== JSON_ERROR_NONE) {
            $this->log("JSON Decode Error: " . json_last_error_msg());
        }

        return [
            'status' => $httpCode,
            'data' => $decoded,
            'raw' => $response,
            'url' => $url,
            'curl_info' => $info,
            'headers_sent' => $headers
        ];
    }

    public function getDescriptors($location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        $endpoint = "/rapidstor/api/descriptors?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function saveDescriptor($descriptorData, $location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        // Ensure required fields
        $descriptorData['sLocationCode'] = $location;
        if (empty($descriptorData['sCorpCode'])) {
            $descriptorData['sCorpCode'] = 'CNCK';
        }

        return $this->makeRequest("/rapidstor/api/descriptors/save?location={$location}", 'POST', $descriptorData);
    }

    public function deleteDescriptor($descriptorData, $location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        return $this->makeRequest("/rapidstor/api/descriptors/delete?location={$location}", 'POST', $descriptorData);
    }

    public function batchUpdate($operation, $descriptors, $location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        $data = [
            'operation' => $operation,
            'descriptors' => $descriptors,
            'location' => $location
        ];
        return $this->makeRequest("/rapidstor/api/descriptors/batch", 'POST', $data);
    }

    public function getStatus()
    {
        return $this->makeRequest("/rapidstor/status");
    }

    public function login($forceRefresh = false)
    {
        $data = ['force_refresh' => $forceRefresh];
        return $this->makeRequest("/rapidstor/login", 'POST', $data);
    }

    public function getDeals($location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        $endpoint = "/rapidstor/api/deals?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function getInsurance($location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        $endpoint = "/rapidstor/api/insurance?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function getUnitTypes($location = 'L004')
    {
        if (!Config::isValidLocation($location)) {
            throw new Exception("Invalid location: {$location}");
        }

        $endpoint = "/rapidstor/api/unittypes?location=" . urlencode($location);
        return $this->makeRequest($endpoint);
    }

    public function testEndpoint($endpoint, $params = [])
    {
        $queryString = !empty($params) ? '?' . http_build_query($params) : '';
        return $this->makeRequest($endpoint . $queryString);
    }
}
?>