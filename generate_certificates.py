#!/usr/bin/env python3
"""
Generate self-signed SSL certificates for the distributed job queue system.
Run this script once to create cert.pem and key.pem in the project directory.
"""

import ssl
import socket
import certifi
from pathlib import Path

def generate_self_signed_cert():
    """Generate a self-signed certificate and key using Python's ssl module."""
    import subprocess
    import sys
    
    project_dir = Path(__file__).parent
    cert_file = project_dir / 'cert.pem'
    key_file = project_dir / 'key.pem'
    
    # Check if files already exist
    if cert_file.exists() and key_file.exists():
        print(f"✓ Certificates already exist:")
        print(f"  - {cert_file}")
        print(f"  - {key_file}")
        return
    
    # Use OpenSSL command to generate self-signed certificate
    # If OpenSSL is not available, provide fallback instructions
    try:
        cmd = [
            'openssl', 'req', '-x509', '-newkey', 'rsa:2048',
            '-keyout', str(key_file), '-out', str(cert_file),
            '-days', '365', '-nodes',
            '-subj', '/C=US/ST=State/L=City/O=Org/CN=localhost'
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print(f"✓ Certificates generated successfully:")
            print(f"  - {cert_file}")
            print(f"  - {key_file}")
        else:
            print(f"✗ OpenSSL error: {result.stderr}")
            print("\nFallback: Using Python cryptography library...")
            generate_with_cryptography()
    
    except FileNotFoundError:
        print("OpenSSL not found. Attempting to use Python cryptography library...")
        generate_with_cryptography()

def generate_with_cryptography():
    """Fallback: Generate certificate using cryptography library."""
    try:
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.hazmat.primitives import serialization
        import datetime
        
        project_dir = Path(__file__).parent
        cert_file = project_dir / 'cert.pem'
        key_file = project_dir / 'key.pem'
        
        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        
        # Generate certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"State"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"City"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Organization"),
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),
        ])
        
        cert = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            datetime.datetime.utcnow() + datetime.timedelta(days=365)
        ).sign(private_key, hashes.SHA256(), default_backend())
        
        # Write private key
        with open(key_file, 'wb') as f:
            f.write(private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ))
        
        # Write certificate
        with open(cert_file, 'wb') as f:
            f.write(cert.public_bytes(serialization.Encoding.PEM))
        
        print(f"✓ Certificates generated successfully (using cryptography):")
        print(f"  - {cert_file}")
        print(f"  - {key_file}")
        
    except ImportError:
        print("✗ Neither OpenSSL nor cryptography library available.")
        print("\nPlease install one of the following:")
        print("  1. OpenSSL: https://www.openssl.org/")
        print("  2. Python cryptography: pip install cryptography")

if __name__ == '__main__':
    print("=" * 60)
    print("   SSL/TLS Certificate Generator")
    print("=" * 60)
    generate_self_signed_cert()
    print("\nNote: These are self-signed certificates for testing/development.")
    print("For production, use certificates from a trusted Certificate Authority.")
