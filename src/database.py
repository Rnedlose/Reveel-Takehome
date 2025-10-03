"""
Database utilities for PostgreSQL connection and table management.
"""
import pandas as pd
from sqlalchemy import create_engine, text, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from loguru import logger
from typing import Optional, Dict, Any, List
from contextlib import contextmanager

from .config import DB_CONFIG


class DatabaseManager:
    """Manages PostgreSQL database connections and operations."""
    
    def __init__(self, config: Dict[str, Any] = None):
        """Initialize database manager with configuration."""
        self.config = config or DB_CONFIG
        self.connection_string = self._build_connection_string()
        self.engine = None
        self.session_factory = None
        
    def _build_connection_string(self) -> str:
        """Build PostgreSQL connection string from config."""
        return (f"postgresql://{self.config['user']}:"
                f"{self.config['password']}@{self.config['host']}:"
                f"{self.config['port']}/{self.config['database']}")
    
    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.engine = create_engine(self.connection_string)
            self.session_factory = sessionmaker(bind=self.engine)
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close database connection."""
        if self.engine:
            self.engine.dispose()
            logger.info("Database connection closed")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        if not self.engine:
            self.connect()
        
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()
    
    @contextmanager
    def get_session(self):
        """Context manager for database sessions."""
        if not self.session_factory:
            self.connect()
            
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise
        finally:
            session.close()
    
    def execute_sql(self, sql: str, params: Dict = None) -> Any:
        """Execute SQL statement."""
        with self.get_connection() as conn:
            result = conn.execute(text(sql), params or {})
            conn.commit()
            return result
    
    def create_tables(self) -> None:
        """Create all required tables."""
        logger.info("Creating database tables...")
        
        # Create schema SQL
        create_tables_sql = '''
        -- Clients table
        CREATE TABLE IF NOT EXISTS clients (
            client_id VARCHAR(10) PRIMARY KEY,
            client_name VARCHAR(255) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'UNKNOWN',
            tier VARCHAR(20) DEFAULT 'UNKNOWN',
            created_at DATE,
            currency VARCHAR(3) DEFAULT 'USD',
            row_hash VARCHAR(64) UNIQUE,
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Invoices table  
        CREATE TABLE IF NOT EXISTS invoices (
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

        -- Fact table combining clients and invoices
        CREATE TABLE IF NOT EXISTS invoice_facts (
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
            created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(client_id, invoice_id)
        );

        -- Indexes for better query performance
        CREATE INDEX IF NOT EXISTS idx_clients_status ON clients(status);
        CREATE INDEX IF NOT EXISTS idx_clients_tier ON clients(tier);
        CREATE INDEX IF NOT EXISTS idx_invoices_client_id ON invoices(client_id);
        CREATE INDEX IF NOT EXISTS idx_invoices_date ON invoices(invoice_date);
        CREATE INDEX IF NOT EXISTS idx_invoices_shipment_type ON invoices(shipment_type);
        CREATE INDEX IF NOT EXISTS idx_invoice_facts_client_id ON invoice_facts(client_id);
        CREATE INDEX IF NOT EXISTS idx_invoice_facts_date ON invoice_facts(invoice_date);
        CREATE INDEX IF NOT EXISTS idx_invoice_facts_shipment_type ON invoice_facts(shipment_type);
        
        -- Update timestamp triggers
        CREATE OR REPLACE FUNCTION update_updated_timestamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_timestamp = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ language 'plpgsql';

        DROP TRIGGER IF EXISTS update_clients_timestamp ON clients;
        CREATE TRIGGER update_clients_timestamp 
            BEFORE UPDATE ON clients 
            FOR EACH ROW EXECUTE FUNCTION update_updated_timestamp();

        DROP TRIGGER IF EXISTS update_invoices_timestamp ON invoices;
        CREATE TRIGGER update_invoices_timestamp 
            BEFORE UPDATE ON invoices 
            FOR EACH ROW EXECUTE FUNCTION update_updated_timestamp();
        '''
        
        self.execute_sql(create_tables_sql)
        logger.info("Database tables created successfully")
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table exists."""
        inspector = inspect(self.engine)
        return table_name in inspector.get_table_names()
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.execute_sql(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]
    
    def truncate_table(self, table_name: str) -> None:
        """Truncate a table."""
        self.execute_sql(f"TRUNCATE TABLE {table_name} CASCADE")
        logger.info(f"Table {table_name} truncated")
    
    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, 
                        conflict_columns: List[str] = None) -> None:
        """Upsert DataFrame to PostgreSQL table."""
        if df.empty:
            logger.warning(f"Empty DataFrame provided for table {table_name}")
            return
            
        logger.info(f"Upserting {len(df)} rows to {table_name}")
        
        # Prepare DataFrame for database insertion
        df_copy = df.copy()
        
        # Handle date columns - convert string dates to proper date format
        date_columns = ['created_at', 'invoice_date']
        for col in date_columns:
            if col in df_copy.columns:
                # Convert to datetime then to date
                df_copy[col] = pd.to_datetime(df_copy[col], errors='coerce').dt.date
        
        # Use pandas to_sql with SQLAlchemy for better performance
        with self.get_connection() as conn:
            # Insert data using pandas to_sql 
            df_copy.to_sql(
                name=f"{table_name}_temp", 
                con=conn, 
                if_exists='replace',
                index=False,
                method='multi'
            )
            
            # Build upsert query with proper type casting
            columns = df_copy.columns.tolist()
            conflict_cols = conflict_columns or [columns[0]]  # Default to first column as key
            
            # Create select statement with proper casting for date columns
            select_cols = []
            for col in columns:
                if col in date_columns:
                    select_cols.append(f"temp.{col}::date")
                else:
                    select_cols.append(f"temp.{col}")
            
            # Create update statement with proper casting (using EXCLUDED for PostgreSQL)
            update_parts = []
            for col in columns:
                if col not in conflict_cols:
                    update_parts.append(f"{col} = EXCLUDED.{col}")
            
            columns_str = ', '.join(columns)
            select_str = ', '.join(select_cols)
            conflict_str = ', '.join(conflict_cols)
            update_str = ', '.join(update_parts)
            
            upsert_sql = f'''
            INSERT INTO {table_name} ({columns_str})
            SELECT {select_str} FROM {table_name}_temp temp
            ON CONFLICT ({conflict_str}) 
            DO UPDATE SET {update_str}
            '''
            
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE {table_name}_temp"))
            conn.commit()
        
        logger.info(f"Successfully upserted data to {table_name}")
