"""
Configuration settings for the Reveel data pipeline.
"""
import os
from typing import Dict, Any

# Database configuration
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', '5432')),
    'database': os.getenv('POSTGRES_DB', 'postgres'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', '')
}

# Rate sheet for cost calculations
RATE_SHEET = {
    'GROUND': 1.0,
    '2DAY': 5.0, 
    'EXPRESS': 10.0,
    'FREIGHT': 20.0
}

# Discount rates for scenario analysis
DISCOUNT_RATES = {
    'GROUND': 0.20,
    'FREIGHT': 0.30,
    '2DAY': 0.50
}

# Data file patterns
DATA_PATTERNS = {
    'clients': 'clients*.csv',
    'invoices': 'invoices*.csv',
    'client_pdfs': 'clients*.pdf',
    'invoice_pdfs': 'invoices*.pdf'
}

# Logging configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} | {message}',
    'rotation': '10 MB'
}