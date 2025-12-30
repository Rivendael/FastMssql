# Security Policy

## Reporting Security Vulnerabilities

If you discover a security vulnerability in FastMSSQL, please **do not** open a public GitHub issue. Instead, email your report to [riverb514@gmail.com](mailto:riverb514@gmail.com) with:

- A clear description of the vulnerability
- Steps to reproduce (if applicable)
- Potential impact
- Any suggested fixes (if you have them)

Please allow reasonable time for a response and remediation before public disclosure. Security researchers are appreciated, and we'll acknowledge your contribution in release notes if you wish.

---

## Security Best Practices for Users

### 1. Connection Encryption

**Always use encryption in production environments.** FastMSSQL provides multiple encryption levels through `SslConfig`:

```python
from fastmssql import Connection, SslConfig, EncryptionLevel

# ✅ RECOMMENDED: Encrypt all traffic (production)
ssl_config = SslConfig(encryption_level=EncryptionLevel.Required)
async with Connection("Server=...", ssl_config=ssl_config) as conn:
    result = await conn.query("SELECT 1")
```

#### Encryption Levels

- **`EncryptionLevel.Required`** (recommended for production)
  - All traffic between client and server is encrypted with TLS
  - Most secure option
  - Requires either `trust_server_certificate=True` OR a valid CA certificate

- **`EncryptionLevel.LoginOnly`**
  - Only login credentials are encrypted
  - Data traffic is unencrypted
  - Suitable for internal networks only
  - **Not recommended for production**

- **`EncryptionLevel.Off`** (default, development only)
  - No encryption
  - **Never use in production**
  - Development and testing only

### 2. Certificate Validation

When using `EncryptionLevel.Required`, you must validate the server's certificate. Choose one of two approaches:

#### Option A: Trust the Server Certificate (Use Cautiously)

```python
from fastmssql import Connection, SslConfig, EncryptionLevel

ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    trust_server_certificate=True  # ⚠️ Bypasses certificate validation
)
```

**When to use:**
- Development/testing environments
- Self-signed certificates you control
- Internal corporate networks with trusted infrastructure

**⚠️ Security Risk:** This disables certificate validation, making you vulnerable to man-in-the-middle (MITM) attacks. Only use if you fully understand the risks.

#### Option B: Validate Against a CA Certificate (Recommended)

```python
from fastmssql import Connection, SslConfig, EncryptionLevel

ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="/path/to/ca-cert.pem"  # ✅ Validates server certificate
)
```

**Supported certificate formats:**
- `.pem` — PEM-encoded (most common)
- `.crt` — DER or PEM-encoded
- `.der` — DER binary format

**How to obtain a CA certificate:**
- From your database administrator
- From your cloud provider (AWS, Azure, GCP)
- From the certificate authority that issued the server certificate

**Example: Azure SQL Server Certificate**

```bash
# Download Azure SQL certificate
curl https://cacerts.digicert.com/DigiCertGlobalRootCA.crt.pem -o azure-ca.pem

# Use in your connection
ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="./azure-ca.pem"
)
```

### 3. Server Name Indication (SNI)

For servers hosting multiple TLS certificates on the same IP, enable SNI:

```python
ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="/path/to/ca-cert.pem",
    enable_sni=True,  # ✅ Send server name in TLS handshake
    server_name="myserver.example.com"  # Certificate should match this
)
```

**When to enable:**
- Cloud databases (Azure SQL, AWS RDS, etc.)
- Reverse proxies or load balancers
- Any environment where IP != hostname in the certificate

### 4. Credentials Management

**Never hardcode credentials in your source code.**

#### ✅ Recommended Approaches

**Environment Variables:**
```python
import os
from fastmssql import Connection

server = os.getenv("DB_SERVER")
username = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")

conn_str = f"Server={server};User Id={username};Password={password}"
async with Connection(conn_str) as conn:
    result = await conn.query("SELECT 1")
```

**.env File (Development Only):**
```bash
# .env
DB_SERVER=localhost,1433
DB_USERNAME=sa
DB_PASSWORD=YourStrong!Password
```

```python
import os
from dotenv import load_dotenv
from fastmssql import Connection

load_dotenv()  # Load .env file
conn_str = os.getenv("DB_CONNECTION_STRING")
```

**Cloud Secrets Management:**
- AWS Secrets Manager
- Azure Key Vault
- HashiCorp Vault
- Google Cloud Secret Manager

#### Password Requirements

For SQL Server:
- Minimum 8 characters
- Must contain uppercase, lowercase, numbers, and special characters
- Use strong, randomly generated passwords
- Rotate passwords regularly

### 5. Connection Pooling Security

FastMSSQL uses connection pooling (bb8-based) with smart defaults:

```python
from fastmssql import Connection, PoolConfig

pool_config = PoolConfig(
    max_size=10,      # Maximum connections
    min_idle=2,       # Minimum idle connections
    max_lifetime=1800 # Max connection lifetime in seconds (30 min)
)

async with Connection(conn_str, pool_config=pool_config) as conn:
    result = await conn.query("SELECT 1")
```

**Security Considerations:**
- Connections are properly closed and cleaned up
- Connection objects are not reused across security contexts
- Each connection uses the credentials provided at creation time

### 6. SQL Injection Prevention

Always use **parameterized queries**. Never concatenate user input into SQL strings.

#### ❌ UNSAFE
```python
user_id = request.args.get('user_id')
result = await conn.query(f"SELECT * FROM users WHERE id = {user_id}")
```

#### ✅ SAFE
```python
user_id = request.args.get('user_id')
result = await conn.query(
    "SELECT * FROM users WHERE id = @P1",
    [user_id]
)
```

FastMSSQL handles parameter escaping and type conversion automatically. All user input should go through parameters, never into the SQL string.

### 7. Input Validation

Even with parameterized queries, validate user input:

```python
from typing import Optional

def validate_page_number(page: Optional[str]) -> int:
    """Convert and validate page parameter."""
    try:
        page_num = int(page or 1)
        if page_num < 1 or page_num > 10000:
            raise ValueError("Page must be between 1 and 10000")
        return page_num
    except (ValueError, TypeError):
        raise ValueError("Invalid page number")

# Use validated input
try:
    page = validate_page_number(request.args.get('page'))
    offset = (page - 1) * 50
    result = await conn.query(
        "SELECT * FROM users ORDER BY id OFFSET @P1 ROWS FETCH NEXT 50 ROWS ONLY",
        [offset]
    )
except ValueError as e:
    # Handle validation error
    return {"error": str(e)}
```

### 8. Error Handling

Avoid exposing sensitive information in error messages:

#### ❌ UNSAFE
```python
try:
    result = await conn.query("SELECT * FROM users WHERE id = @P1", [user_id])
except Exception as e:
    return {"error": str(e)}  # Exposes database details
```

#### ✅ SAFE
```python
import logging
from fastmssql import FastMssqlError

logger = logging.getLogger(__name__)

try:
    result = await conn.query("SELECT * FROM users WHERE id = @P1", [user_id])
except FastMssqlError as e:
    logger.error(f"Database error: {e}")  # Log detailed error
    return {"error": "A database error occurred"}  # Generic response to user
```

**Guidelines:**
- Log detailed errors server-side for debugging
- Return generic error messages to clients
- Never expose connection strings, table names, or query details
- Never expose stack traces in production

### 9. Network Security

#### Use TLS/SSL for All Connections

```python
# ✅ HTTPS connections to application
# ✅ TLS connections to database
ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    ca_certificate_path="/path/to/ca-cert.pem"
)
```

#### Network Segmentation

- Restrict database access to application servers only
- Use firewall rules to limit connections to port 1433
- Run database in private subnets when possible
- Use VPN or SSH tunnels for remote administration

#### Connection Strings in URLs

If you must pass connection details in URLs (not recommended), use POST data with HTTPS, never URL parameters:

```python
# ❌ UNSAFE - credentials in query string
GET /api/query?username=sa&password=weak

# ✅ Use POST with HTTPS
POST /api/query
Content-Type: application/json
{
    "username": "sa",
    "password": "strong-password"
}
```

### 10. Dependency Management

FastMSSQL is built on secure, well-maintained Rust libraries:

```toml
# Key dependencies (from Cargo.toml)
tiberius = "0.12"  # SQL Server client library (security-focused)
tokio = { version = "1", features = ["full"] }
pyo3 = "0.21"  # Python bindings
bb8 = "0.8"    # Connection pooling
```

**Keep dependencies updated:**

```bash
# Check for security vulnerabilities
cargo audit

# Update dependencies
cargo update
```

---

## Known Security Considerations

### 1. TrustServerCertificate Flag

The `trust_server_certificate` flag bypasses certificate validation. This is convenient for development but creates a vulnerability in production:

- **Risk:** Man-in-the-middle attacks
- **When safe:** Controlled networks, self-signed certs you own
- **Alternative:** Use proper CA certificates

### 2. Self-Signed Certificates

Self-signed certificates are useful for testing but offer no protection against MITM attacks:

```bash
# Generate a self-signed certificate for testing
openssl req -x509 -newkey rsa:4096 -nodes -out cert.pem -keyout key.pem -days 365

# Use with trust flag (development only)
ssl_config = SslConfig(
    encryption_level=EncryptionLevel.Required,
    trust_server_certificate=True  # Only for development
)
```

### 3. Local Development Environment

For local testing, the default settings are acceptable:

```python
# Development only - no encryption
conn = Connection("Server=localhost;User Id=sa;Password=StrongPassword123!")
```

But once deployed to staging or production, always use encryption.

### 4. Connection String Encoding

Connection strings support special characters. Ensure special characters are properly handled:

```python
# Passwords with special characters should be stored securely
# Use environment variables or vault, not hardcoded
password = os.getenv("DB_PASSWORD")  # "P@ssw0rd!#$%"
```

---

## Security for Contributors

### Code Review

All contributions are reviewed for security issues:
- Input validation and output encoding
- Proper error handling (no sensitive info leakage)
- Secure defaults
- Dependency updates

### Reporting Security Issues in Dependencies

If you discover a vulnerability in a dependency:

1. Check if there's an updated version available
2. Report to the maintainers of the affected project
3. Email us (security contact above) if it affects FastMSSQL
4. Do not publicly disclose until patched

### Running Tests

Tests include security-focused scenarios:

```bash
# Run security-related tests
pytest tests/test_ssl_config.py
pytest tests/test_ssl_integration.py
pytest tests/test_error_handling.py
```

---

## Security Checklist for Deployment

Use this checklist before deploying to production:

- [ ] Enable encryption: `EncryptionLevel.Required`
- [ ] Validate certificates: Use `ca_certificate_path` (not `trust_server_certificate=True`)
- [ ] Enable SNI: `enable_sni=True` for cloud databases
- [ ] Rotate credentials: Use strong, unique passwords
- [ ] Secure storage: Credentials in environment variables or vault
- [ ] Network access: Database in private subnet, firewall rules configured
- [ ] Error handling: Detailed logging server-side, generic messages to clients
- [ ] Input validation: Validate all user inputs
- [ ] Parameterized queries: All user data through parameters
- [ ] Keep updated: Regular dependency updates and security patches
- [ ] Monitor: Log and monitor database connections and errors

---

## Additional Resources

- [Microsoft SQL Server TLS 1.2 Support](https://docs.microsoft.com/en-us/sql/database-engine/configure-windows/enable-encrypted-connections-to-the-database-engine)
- [OWASP Top 10](https://owasp.org/www-project-top-ten/)
- [CWE-89: SQL Injection](https://cwe.mitre.org/data/definitions/89.html)
- [CWE-295: Improper Certificate Validation](https://cwe.mitre.org/data/definitions/295.html)
- [Python Security](https://python.readthedocs.io/en/stable/library/security_warnings.html)

---

## Acknowledgments

Thank you to all security researchers who responsibly disclose vulnerabilities. Your contributions help keep FastMSSQL secure for everyone.

Last updated: December 2025
