<?php
/**
 * PHP Secrets Vault Reader
 * Reads and decrypts secrets stored by the Python vault
 *
 * Compatible with Python's cryptography.fernet.Fernet
 */

namespace App\Vault;

class SecretsVault
{
    // Must match Python vault salt exactly
    private const VAULT_SALT = 'webapp-backend-vault-2024';
    private const PBKDF2_ITERATIONS = 100000;

    private string $vaultDir;
    private string $masterKey;
    private ?string $encryptionKey = null;
    private array $cache = [];
    private int $cacheTtl = 300; // 5 minutes
    private array $cacheTimestamps = [];

    /**
     * Initialize the vault reader
     *
     * @param string $vaultDir Path to vault directory
     * @param string|null $masterKey Master key (reads from env/file if not provided)
     */
    public function __construct(string $vaultDir = '.vault', ?string $masterKey = null)
    {
        $this->vaultDir = rtrim($vaultDir, '/');
        $this->masterKey = $masterKey ?? $this->loadMasterKey();
        $this->initializeEncryption();
    }

    /**
     * Load master key from environment or .key file
     */
    private function loadMasterKey(): string
    {
        // Try environment variable first
        $key = $_ENV['VAULT_MASTER_KEY'] ?? getenv('VAULT_MASTER_KEY');

        if ($key) {
            return $key;
        }

        // Try .key file
        $keyFile = $this->vaultDir . '/.key';
        if (file_exists($keyFile)) {
            $key = trim(file_get_contents($keyFile));
            if ($key) {
                return $key;
            }
        }

        throw new \RuntimeException(
            'Master key not found. Set VAULT_MASTER_KEY environment variable or ensure .vault/.key exists'
        );
    }

    /**
     * Initialize encryption key derivation
     */
    private function initializeEncryption(): void
    {
        // Derive encryption key using PBKDF2 (must match Python implementation)
        $derivedKey = hash_pbkdf2(
            'sha256',
            $this->masterKey,
            self::VAULT_SALT,
            self::PBKDF2_ITERATIONS,
            32,
            true
        );

        // Fernet uses base64url encoding
        $this->encryptionKey = $this->base64UrlEncode($derivedKey);
    }

    /**
     * Get a secret value
     *
     * @param string $key Secret key
     * @param mixed $default Default value if key not found
     * @return mixed Decrypted secret value or default
     */
    public function get(string $key, $default = null)
    {
        // Check cache first
        if (isset($this->cache[$key])) {
            $timestamp = $this->cacheTimestamps[$key] ?? 0;
            if (time() - $timestamp < $this->cacheTtl) {
                return $this->cache[$key];
            }
        }

        // Load from encrypted storage
        $secrets = $this->loadSecrets();
        $value = $secrets[$key] ?? $default;

        // Update cache
        if ($value !== null) {
            $this->cache[$key] = $value;
            $this->cacheTimestamps[$key] = time();
        }

        return $value;
    }

    /**
     * List all secret keys
     *
     * @return array List of secret key names
     */
    public function listKeys(): array
    {
        $secrets = $this->loadSecrets();
        return array_keys($secrets);
    }

    /**
     * Check if vault is available and configured
     *
     * @return bool True if vault is properly configured
     */
    public function isAvailable(): bool
    {
        try {
            $secretsFile = $this->vaultDir . '/secrets.enc';
            return file_exists($secretsFile) && $this->encryptionKey !== null;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * Load and decrypt all secrets
     *
     * @return array Decrypted secrets
     */
    private function loadSecrets(): array
    {
        $secretsFile = $this->vaultDir . '/secrets.enc';

        if (!file_exists($secretsFile)) {
            return [];
        }

        try {
            $encryptedData = file_get_contents($secretsFile);

            if (empty($encryptedData)) {
                return [];
            }

            $decryptedData = $this->fernetDecrypt($encryptedData);
            return json_decode($decryptedData, true) ?? [];

        } catch (\Exception $e) {
            error_log("Vault: Failed to load secrets: " . $e->getMessage());
            return [];
        }
    }

    /**
     * Decrypt Fernet-encrypted data
     *
     * Fernet format:
     * - Version (1 byte): 0x80
     * - Timestamp (8 bytes): Big-endian uint64
     * - IV (16 bytes): AES initialization vector
     * - Ciphertext (variable): AES-128-CBC encrypted
     * - HMAC (32 bytes): SHA256 HMAC of version + timestamp + IV + ciphertext
     *
     * @param string $token Base64-encoded Fernet token
     * @return string Decrypted plaintext
     */
    private function fernetDecrypt(string $token): string
    {
        // Decode base64url token
        $data = $this->base64UrlDecode($token);

        if (strlen($data) < 57) { // 1 + 8 + 16 + 32 minimum
            throw new \RuntimeException('Invalid Fernet token: too short');
        }

        // Parse Fernet structure
        $version = ord($data[0]);
        if ($version !== 0x80) {
            throw new \RuntimeException('Invalid Fernet token: wrong version');
        }

        $timestamp = substr($data, 1, 8);
        $iv = substr($data, 9, 16);
        $ciphertext = substr($data, 25, -32);
        $hmac = substr($data, -32);

        // Derive signing and encryption keys from Fernet key
        $fernetKey = $this->base64UrlDecode($this->encryptionKey);
        $signingKey = substr($fernetKey, 0, 16);
        $encryptionKey = substr($fernetKey, 16, 16);

        // Verify HMAC
        $signedData = substr($data, 0, -32);
        $expectedHmac = hash_hmac('sha256', $signedData, $signingKey, true);

        if (!hash_equals($expectedHmac, $hmac)) {
            throw new \RuntimeException('Invalid Fernet token: HMAC verification failed');
        }

        // Decrypt ciphertext
        $plaintext = openssl_decrypt(
            $ciphertext,
            'AES-128-CBC',
            $encryptionKey,
            OPENSSL_RAW_DATA,
            $iv
        );

        if ($plaintext === false) {
            throw new \RuntimeException('Decryption failed');
        }

        return $plaintext;
    }

    /**
     * Base64 URL-safe encode
     */
    private function base64UrlEncode(string $data): string
    {
        return rtrim(strtr(base64_encode($data), '+/', '-_'), '=');
    }

    /**
     * Base64 URL-safe decode
     */
    private function base64UrlDecode(string $data): string
    {
        $padded = str_pad(strtr($data, '-_', '+/'), strlen($data) + (4 - strlen($data) % 4) % 4, '=');
        return base64_decode($padded);
    }

    /**
     * Get vault metadata
     *
     * @return array Vault metadata
     */
    public function getMetadata(): array
    {
        $metadataFile = $this->vaultDir . '/metadata.json';

        if (!file_exists($metadataFile)) {
            return [];
        }

        return json_decode(file_get_contents($metadataFile), true) ?? [];
    }
}

/**
 * Get or create vault singleton instance
 *
 * @param string $vaultDir Path to vault directory
 * @return SecretsVault|null Vault instance or null if not available
 */
function getVault(string $vaultDir = '.vault'): ?SecretsVault
{
    static $instance = null;

    if ($instance === null) {
        try {
            $instance = new SecretsVault($vaultDir);
        } catch (\Exception $e) {
            error_log("Vault not available: " . $e->getMessage());
            return null;
        }
    }

    return $instance;
}

/**
 * Get a secret value with fallback to environment
 *
 * @param string $key Secret key
 * @param mixed $default Default value
 * @param string $vaultDir Vault directory
 * @return mixed Secret value
 */
function getSecret(string $key, $default = null, string $vaultDir = '.vault')
{
    // Try vault first
    $vault = getVault($vaultDir);
    if ($vault && $vault->isAvailable()) {
        $value = $vault->get($key);
        if ($value !== null) {
            return $value;
        }
    }

    // Fallback to environment
    return $_ENV[$key] ?? getenv($key) ?: $default;
}
