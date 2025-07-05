# 🔒 Security Guide for Schema Graph Builder

This document outlines the security features and best practices for using the Schema Graph Builder package.

## 🛡️ Security Features

### 1. Credential Management

#### Environment Variables (Recommended)
The most secure way to provide database credentials is via environment variables:

```bash
export DB_USERNAME=your_username
export DB_PASSWORD=your_secure_password
export DB_HOST=your_database_host
```

#### Encrypted Configuration Files
For configuration files, you can encrypt passwords:

```bash
# Encrypt a password
python -m schema_graph_builder.cli encrypt-password "your_password"

# Or use the Python API
from schema_graph_builder.utils.security import CredentialManager
result = CredentialManager.encrypt_password("your_password")
```

Add the encrypted credentials to your config file:
```yaml
host: localhost
port: 5432
database: mydb
username: myuser
encrypted_password: "gAAAAABh..."
encryption_key: "your_encryption_key"
```

### 2. Configuration Validation

Validate configuration files before use:

```bash
# CLI validation
schema-graph-builder validate-config config/db.yaml postgres

# Python API validation
from schema_graph_builder.utils.config_validator import validate_database_config
result = validate_database_config("config/db.yaml", "postgres")
```

### 3. Audit Logging

Enable audit logging to track database access:

```python
from schema_graph_builder import SchemaGraphBuilder
from schema_graph_builder.utils.logging_config import setup_logging

# Setup with audit logging
setup_logging(level="INFO", filename="app.log")
builder = SchemaGraphBuilder(audit_log_file="audit.log")
```

### 4. Secure Connection Handling

All database connections include:
- Connection timeouts (30 seconds)
- Connection pooling with recycling
- Automatic connection cleanup
- Credential masking in logs

### 5. Input Validation

All inputs are validated for:
- SQL injection patterns
- Dangerous characters
- Type validation
- Range checking

## 🔐 Best Practices

### 1. Credential Security

**DO:**
- Use environment variables for credentials
- Encrypt passwords in configuration files
- Use strong passwords (8+ characters)
- Rotate credentials regularly
- Use dedicated database accounts with minimal privileges

**DON'T:**
- Store plain text passwords in configuration files
- Use default passwords (admin, root, etc.)
- Share credentials in code repositories
- Use administrative accounts for application access

### 2. Network Security

**DO:**
- Use SSL/TLS connections when possible
- Restrict database access to specific IP addresses
- Use VPNs for remote database access
- Monitor network traffic for anomalies

**DON'T:**
- Connect to databases over unencrypted connections
- Allow database access from any IP address
- Use default database ports in production

### 3. Configuration Security

**DO:**
- Validate all configuration files
- Use secure file permissions (600) for config files
- Store sensitive configs outside web-accessible directories
- Use configuration templates without sensitive data

**DON'T:**
- Commit configuration files with credentials to version control
- Use world-readable permissions on config files
- Store configuration files in public directories

### 4. Monitoring and Auditing

**DO:**
- Enable audit logging for production use
- Monitor for failed connection attempts
- Set up alerts for security events
- Regularly review audit logs

**DON'T:**
- Ignore security warnings
- Disable security features for convenience
- Skip log analysis

## 🚨 Security Configuration Examples

### Secure PostgreSQL Configuration

```yaml
# config/postgres_secure.yaml
host: db.company.com
port: 5432
database: production_db
username: schema_reader
# Password via environment variable DB_PASSWORD
# OR use encrypted password:
# encrypted_password: "gAAAAABh..."
# encryption_key: "your_key"

# Optional: SSL configuration
ssl_mode: require
ssl_cert: /path/to/client-cert.pem
ssl_key: /path/to/client-key.pem
ssl_ca: /path/to/ca-cert.pem
```

### Secure CLI Usage

```bash
# Set environment variables
export DB_USERNAME=schema_reader
export DB_PASSWORD=your_secure_password

# Run with validation and audit logging
schema-graph-builder analyze postgres \
  --config config/secure.yaml \
  --validate-config \
  --audit-log /var/log/schema-builder-audit.log \
  --log-level INFO
```

### Secure Python API Usage

```python
import os
from schema_graph_builder import SchemaGraphBuilder
from schema_graph_builder.utils.logging_config import setup_logging

# Setup secure logging
setup_logging(
    level="INFO", 
    filename="/var/log/schema-builder.log"
)

# Initialize with audit logging
builder = SchemaGraphBuilder(
    audit_log_file="/var/log/schema-builder-audit.log"
)

# Use with validation
result = builder.analyze_database(
    db_type="postgres",
    config_path="config/secure.yaml",
    validate_config=True  # Always validate in production
)
```

## 🔍 Security Checklist

### Development
- [ ] Use environment variables for credentials
- [ ] Enable configuration validation
- [ ] Set up proper logging
- [ ] Use strong passwords
- [ ] Test with security scanning tools

### Staging
- [ ] Use encrypted configuration files
- [ ] Enable audit logging
- [ ] Test with production-like security settings
- [ ] Validate SSL/TLS connections
- [ ] Review audit logs

### Production
- [ ] Use dedicated database accounts with minimal privileges
- [ ] Enable full audit logging
- [ ] Set up monitoring and alerting
- [ ] Use SSL/TLS for all connections
- [ ] Regularly rotate credentials
- [ ] Monitor for security events
- [ ] Keep security documentation updated

## 🚨 Incident Response

If you suspect a security incident:

1. **Immediately** rotate affected database credentials
2. Review audit logs for suspicious activity
3. Check for unauthorized schema access
4. Update security configurations
5. Contact your security team
6. Document the incident for future prevention

## 📞 Reporting Security Issues

If you discover a security vulnerability in Schema Graph Builder:

1. **DO NOT** create a public issue
2. Email security concerns to: [security@your-domain.com]
3. Include detailed information about the vulnerability
4. Provide steps to reproduce if possible
5. Allow time for assessment and patching

## 🔄 Security Updates

- Subscribe to security advisories
- Keep the package updated to the latest version
- Review security changelog for each update
- Test security updates in staging before production

---

**Remember: Security is a shared responsibility. This package provides tools and best practices, but proper implementation and operational security depend on your specific environment and requirements.** 