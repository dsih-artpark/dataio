"""
Email domain validation for registration.

During beta, we restrict registration to institutional email addresses.
Personal email addresses require admin approval.
"""

import re
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

# Exact domains that are always allowed (institutional)
INSTITUTIONAL_DOMAINS = [
    "artpark.in",
    "iisc.ac.in",
]

# Regex patterns for institutional email domains
INSTITUTIONAL_PATTERNS = [
    r".*\.ac\.in$",       # Indian academic (e.g., iisc.ac.in, iitb.ac.in)
    r".*\.edu$",          # US education (e.g., stanford.edu, mit.edu)
    r".*\.edu\.[a-z]{2}$", # International education (e.g., .edu.au, .edu.sg)
    r".*\.ac\.[a-z]{2}$",  # International academic (e.g., .ac.uk, .ac.jp)
    r".*\.res\.in$",      # Indian research institutions
    r".*\.gov\.in$",      # Indian government
    r".*\.ernet\.in$",    # Indian education and research network
    r".*\.iitb\.ac\.in$", # IIT Bombay subdomains
    r".*\.iisc\.ac\.in$", # IISc subdomains
]

# Blocked domains (common personal email providers)
BLOCKED_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "yahoo.in",
    "hotmail.com",
    "outlook.com",
    "live.com",
    "icloud.com",
    "protonmail.com",
    "proton.me",
    "aol.com",
    "mail.com",
    "zoho.com",
    "yandex.com",
    "gmx.com",
    "rediffmail.com",
]


def get_email_domain(email: str) -> str:
    """Extract domain from email address."""
    if "@" not in email:
        return ""
    return email.split("@")[1].lower()


def is_institutional_email(email: str) -> bool:
    """
    Check if an email is from an institutional domain.

    Args:
        email: The email address to check

    Returns:
        True if the email is from an institutional domain
    """
    domain = get_email_domain(email)
    if not domain:
        return False

    # Check exact domain matches
    if domain in INSTITUTIONAL_DOMAINS:
        return True

    # Check pattern matches
    for pattern in INSTITUTIONAL_PATTERNS:
        if re.match(pattern, domain):
            return True

    return False


def is_blocked_domain(email: str) -> bool:
    """
    Check if an email is from a blocked (personal) domain.

    Args:
        email: The email address to check

    Returns:
        True if the email is from a blocked domain
    """
    domain = get_email_domain(email)
    return domain in BLOCKED_DOMAINS


def validate_registration_email(email: str) -> Tuple[bool, str, str]:
    """
    Validate an email for registration.

    Args:
        email: The email address to validate

    Returns:
        Tuple of (is_valid, verification_status, message)
        - is_valid: Whether registration can proceed
        - verification_status: 'verified' for institutional, 'pending' for others
        - message: Human-readable message about the validation result
    """
    email = email.lower().strip()
    domain = get_email_domain(email)

    if not domain:
        return False, "", "Invalid email address"

    # Check if it's an institutional email
    if is_institutional_email(email):
        logger.info(f"Institutional email detected: {domain}")
        return True, "verified", "Institutional email - auto-verified"

    # Check if it's a blocked personal email domain
    if is_blocked_domain(email):
        logger.info(f"Personal email detected (pending verification): {domain}")
        return True, "pending", (
            "Personal email addresses require admin approval. "
            "You will be notified once your account is verified."
        )

    # Unknown domain - treat as pending verification
    logger.info(f"Unknown domain (pending verification): {domain}")
    return True, "pending", (
        "Your email domain requires admin approval. "
        "You will be notified once your account is verified."
    )


def get_domain_type(email: str) -> str:
    """
    Get the type of domain for an email.

    Args:
        email: The email address

    Returns:
        'institutional', 'personal', or 'unknown'
    """
    if is_institutional_email(email):
        return "institutional"
    if is_blocked_domain(email):
        return "personal"
    return "unknown"
