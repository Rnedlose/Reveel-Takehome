# Reveel Data Engineering Take-Home Project

A comprehensive data pipeline for ingesting, normalizing, and analyzing messy client and invoice data from multiple sources and schema variants.

## AI Prompt Disclosure

As allowed by the project guidelines, I collaborated with AI during development.
The following is the consolidated prompt that captures the guidance I provided to the AI to generate the project structure and code:

# Prompt Used
"Build a Python data pipeline that ingests messy client and invoice datasets (CSV and PDF with multiple schema variants), normalizes them into a consistent schema, and stores them in PostgreSQL with idempotent upserts. The pipeline should create a fact table linking clients and invoices, calculate shipment costs using a provided rate sheet, and support analysis queries: top 5 clients by cost, month-over-month growth (2024–2025), a discount scenario (20% off Ground, 30% off Freight, 50% off 2-Day), and a reclassification scenario (Express billed as Ground). Code should be modular (separate files for processing, database, pipeline, and analysis), use pandas, PyPDF2, SQLAlchemy, and loguru, and include a standalone reporting script and requirements.txt."

## Architecture Overview

This project implements a production-ready data pipeline that:
- **Ingests** data from multiple CSV files and PDF exports with different schema versions
- **Normalizes** inconsistent data formats, handles duplicates, and standardizes field values
    -  Clients: drop duplicates on client_id, keeping the “best” record.  Furthermore, I chose to use Status Ranking: Active > Inactive > Unknown. Additionally, I kept the newest record first, backfilling missing fields from other records.
    -  Invoices: drop duplicates on invoice_id, keeping the first record and dropping duplicates outright.
    -  Additionally, each row was hashed with SHA-256 by combining the fields used in the postgres tables, ensuring a unique identifier that only changes if the data changes.  This allows the pipeline to be idempotent, and reruns should only modify data if the hash is different.
- **Stores** cleaned data in PostgreSQL with proper data modeling
- **Analyzes** business metrics with SQL queries for actionable insights

### Key Components

```
src/
├── config.py          # Configuration settings and constants
├── database.py        # PostgreSQL connection and schema management
├── data_processing.py  # Data ingestion, cleaning, and normalization
├── pipeline.py        # Main orchestration logic
└── analysis.py        # Business intelligence queries and reporting
```

## Quick Start

### Prerequisites

- Python 3.13.7
- Docker
- PostgreSQL database running in docker
- Required Python packages (see requirements.txt)

### Setup
1. **Start PostgreSQL Database**:
   docker run -d --restart unless-stopped -p "127.0.0.1:5432:5432" --name=postgres17 -e POSTGRES_HOST_AUTH_METHOD=trust postgres:17
   ```bash
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the Complete Pipeline**:
   ```bash
   python -m src.pipeline --data-dir "data files"
   ```

4. **Generate Analysis Report**:
   ```bash
   python run_analysis.py
   ```

### Database Configuration

The pipeline uses these default connection settings:
- **Host**: localhost
- **Port**: 5432
- **Database**: postgres
- **User**: postgres
- **Password**: (empty)

Override via environment variables:
```bash
export POSTGRES_HOST=your_host
export POSTGRES_PORT=5432
export POSTGRES_DB=your_db
export POSTGRES_USER=your_user
export POSTGRES_PASSWORD=your_password
```

## Data Processing

### Schema Handling

The pipeline automatically detects and normalizes 3 different schema variants:

**Client Data Schemas**:
- **v1**: `client_id, client_name, status, created_at`
- **v2**: `id, name, tier, acct_open_date` 
- **v3**: `customer_key, display_name, active_flag, signup_ts, currency`

**Invoice Data Schemas**:
- **v1**: `invoice_id, client_id, invoice_date, amount, currency, shipment_type`
- **v2**: `inv_no, customer_key, inv_dt, subtotal/total, curr, ship_type`
- **v3**: `invoice_uid, client_ref, issued_on, amount_usd, shipment_category`

### Data Quality Handling

**Normalization Features**:
- Standardized date parsing with multiple format support
- Name cleaning and proper case formatting
- Status normalization (Active/Inactive/Unknown)
- Shipment type standardization with variant mapping
- Currency and amount parsing with error handling
- Duplicate detection and resolution with conflict priority

**Conflict Resolution Strategy**:
1. **Newest data wins**: Most recent records take precedence
2. **Status hierarchy**: ACTIVE > INACTIVE > UNKNOWN
3. **Field backfilling**: Missing values filled from alternate sources
4. **Hash-based change detection**: Only updates when data actually changes

## Database Schema

### Tables Created

**clients**
```sql
CREATE TABLE clients (
    client_id VARCHAR(10) PRIMARY KEY,
    client_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'UNKNOWN',
    tier VARCHAR(20) DEFAULT 'UNKNOWN',
    created_at DATE,
    currency VARCHAR(3) DEFAULT 'USD',
    row_hash VARCHAR(64) UNIQUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**invoices**
```sql
CREATE TABLE invoices (
    invoice_id VARCHAR(50) PRIMARY KEY,
    client_id VARCHAR(10),
    client_name VARCHAR(255),
    invoice_date DATE,
    amount DECIMAL(10,2),
    currency VARCHAR(3) DEFAULT 'USD',
    shipment_type VARCHAR(20),
    row_hash VARCHAR(64) UNIQUE,
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**invoice_facts** (Dimensional Model)
```sql
CREATE TABLE invoice_facts (
    fact_id SERIAL PRIMARY KEY,
    client_id VARCHAR(10),
    client_name VARCHAR(255) NOT NULL,
    client_status VARCHAR(20),
    client_tier VARCHAR(20),
    invoice_id VARCHAR(50) NOT NULL,
    invoice_date DATE NOT NULL,
    invoice_amount DECIMAL(10,2) NOT NULL,
    shipment_type VARCHAR(20) NOT NULL,
    rate_per_unit DECIMAL(10,2),
    calculated_cost DECIMAL(10,2),
    created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Business Analysis

The pipeline automatically generates answers to key business questions using **calculated costs** based on shipment type rates.

### Cost Calculation Methodology

The analysis uses calculated costs rather than raw invoice amounts:
- **calculated_cost = invoice_amount × rate_per_unit**
- Rate sheet: GROUND ($1), 2DAY ($5), EXPRESS ($10), FREIGHT ($20)
- Provides more accurate business insights based on operational cost structure
- Enables meaningful comparisons across different shipment types

### 1. Top 5 Clients by Calculated Costs
Identifies clients with highest calculated costs based on shipment type rates applied to invoice amounts.

### 2. Month-over-Month Cost Growth Analysis
Tracks calculated cost growth trends for 2024-2025 period with detailed month-over-month comparisons.

### 3. Discount Scenario Analysis
Models pricing scenarios with discounts:
- 20% off GROUND shipments
- 30% off FREIGHT shipments
- 50% off 2DAY shipments

**Results**: New top 5 spenders after discounts with total savings calculated.

### 4. EXPRESS → GROUND Reclassification
Analyzes cost savings if EXPRESS shipments were billed as GROUND using calculated costs:

- Calculates potential savings based on rate differences (EXPRESS $10 vs GROUND $1 per unit)
- Identifies clients with significant savings opportunities  
- Provides percentage and absolute dollar savings analysis

## Technical Decisions & Assumptions

### Design Decisions

**Database Choice**: PostgreSQL
- Excellent JSON/JSONB support for flexible schema evolution
- Advanced SQL features for complex analysis queries
- Scalable for production workloads

**Processing Strategy**: Batch Processing
- Handles large datasets efficiently
- Idempotent operations
- Full data lineage and change tracking
- Suitable for daily/hourly refresh cycles

**Schema Design**: Dimensional Modeling
- Fact table optimized for analytical queries
- Separate staging tables for raw data integrity
- Indexed for query performance
- Flexible for additional metrics

### Key Assumptions

**Data Quality Assumptions**:
- Client IDs following pattern `C\d{5}` are considered valid
- Missing client IDs can be matched by exact name matching
- Date formats are handled by pandas/dateutil
- Currency amounts are in USD unless specified otherwise

**Business Logic Assumptions**:
- ACTIVE status takes precedence over INACTIVE for duplicate clients
- Newer records are more authoritative than older ones
- Invoice amounts represent actual billed amounts
- Shipment type determines billing rate according to rate sheet

**Rate Sheet** (as specified):
- GROUND: $1 per unit
- 2DAY: $5 per unit
- EXPRESS: $10 per unit
- FREIGHT: $20 per unit

### Error Handling

**Graceful Degradation**:
- Invalid dates → NULL with warning logged
- Unparsable amounts → 0.0 with warning logged
- Unknown shipment types → "UNKNOWN" category
- Missing client matches → Invoice stored with NULL client_id
- PDF parsing errors → Empty DataFrame returned

## Pipeline Features

### Idempotency
- Safe to run multiple times without data duplication
- Hash-based change detection prevents unnecessary updates
- UPSERT operations handle existing records gracefully

### Monitoring & Observability
- Comprehensive logging with structured messages
- Row count tracking and data quality metrics
- Performance timing for each pipeline stage
- Error handling with detailed context

### Scalability Considerations
- Batch processing suitable for large datasets
- Database indexes on key query fields
- Configurable batch sizes and memory management
- Modular design allows horizontal scaling

## Testing & Validation

### Data Validation
```bash
# Test idempotency by running pipeline twice
python -m src.pipeline --data-dir "data files"
python -m src.pipeline --data-dir "data files"  # Should show no changes

# Validate row counts
psql -h localhost -U postgres -c "SELECT COUNT(*) FROM clients;"
psql -h localhost -U postgres -c "SELECT COUNT(*) FROM invoices;"
psql -h localhost -U postgres -c "SELECT COUNT(*) FROM invoice_facts;"
```

### Query Validation
```bash
# Run analysis queries
python run_analysis.py

# Validate specific business rules (matches analysis.py calculated costs)
psql -h localhost -U postgres -c "
SELECT 
  shipment_type,
  COUNT(*) AS shipment_count,
  SUM(CASE shipment_type
        WHEN 'GROUND'  THEN invoice_amount * 1
        WHEN '2DAY'    THEN invoice_amount * 5
        WHEN 'EXPRESS' THEN invoice_amount * 10
        WHEN 'FREIGHT' THEN invoice_amount * 20
        ELSE invoice_amount
      END) AS calculated_cost
FROM invoice_facts
GROUP BY shipment_type
ORDER BY calculated_cost DESC;"

## Project Results
Inside this repo is a pipeline.log.res file.  This is the final run through results of running this pipeline.
You can see all the logic and output from the terminal there.
In addition, when you run this pipeline, it will output to this logfile the results
of running the pipeline and the analysis.  The validation psql cmd will output only to the terminal,
though I have copied and pasted this into the pipeline.log.res file for validation purposes.
```

## Production Considerations

### What Would Be Different in Production

**Infrastructure**:
- Containerized deployment
- Managed PostgreSQL
- Message queues for processing coordination

**Data Engineering**:
- Production workflow orchestration
- Production data warehouse for versioned data storage

**Monitoring & Operations**:
- Application metrics
- Distributed tracing
- Error tracking
- Data lineage tracking
- Automated alerting for pipeline failures

**Security & Compliance**:
- Data encryption at rest and in transit
- Role-based access control (RBAC)
- Audit logging for compliance (SOX, GDPR)
- Secrets management
- Data masking for non-production environments

**Testing**:
- Unit tests with pytest (>90% coverage)
- Integration tests with test databases
- Data quality tests for each schema variant
- Performance benchmarking with load testing

Reporting
- Implement a reporting dashboard
- Hook the pipeline up to the dashboard

**Project Structure**:
```
Reveel-Takehome/
├── README.md                    # This documentation
├── requirements.txt             # Python dependencies  
├── run_analysis.py             # Analysis report generator
├── src/                        # Source code
│   ├── __init__.py
│   ├── config.py               # Configuration settings
│   ├── database.py             # Database utilities
│   ├── data_processing.py      # ETL logic
│   ├── pipeline.py             # Main orchestrator
│   └── analysis.py             # BI queries
├── data files/                 # Input CSV and PDF files
│   ├── clients_v1 (1).pdf
│   ├── clients_v1 (2).csv
│   ├── clients_v2 (2).csv
│   ├── clients_v3 (2).csv
│   ├── invoices_v1 (3).csv
│   ├── invoices_v2 (2).csv
│   └── invoices_v3 (2).csv
├── project requirements/       # Original project specification
│   ├── Data_Engineering_Takehome_Project.pdf
│   └── Data_Engineering_Takehome_Project.docx
├── tests/                      # Unit tests (future)
├── docs/                       # Additional documentation
└── sql/                        # SQL scripts (future)
```

