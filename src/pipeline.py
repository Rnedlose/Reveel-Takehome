"""
Main data pipeline for processing Reveel take-home project data.
Orchestrates client and invoice data processing, creates fact tables, and runs analysis queries.
"""
import os
import glob
from typing import List, Dict, Any
import pandas as pd
from loguru import logger

from .config import DB_CONFIG, RATE_SHEET, DATA_PATTERNS
from .database import DatabaseManager
from .data_processing import ClientProcessor, InvoiceProcessor


class RevealPipeline:
    """Main pipeline for processing client and invoice data."""
    
    def __init__(self, data_dir: str = None, db_config: Dict = None):
        """Initialize pipeline with data directory and database config."""
        self.data_dir = data_dir or os.getcwd()
        self.db_manager = DatabaseManager(db_config or DB_CONFIG)
        self.client_processor = ClientProcessor()
        self.invoice_processor = InvoiceProcessor()
        
        # Setup logging
        logger.add("pipeline.log", rotation="10 MB", level="INFO")
        logger.info(f"Pipeline initialized with data directory: {self.data_dir}")
    
    def setup_database(self) -> None:
        """Initialize database connection and create tables."""
        logger.info("Setting up database...")
        self.db_manager.connect()
        self.db_manager.create_tables()
        logger.info("Database setup complete")
    
    def find_data_files(self) -> Dict[str, List[str]]:
        """Find all data files matching expected patterns."""
        logger.info(f"Searching for data files in: {self.data_dir}")
        
        files = {}
        for data_type, pattern in DATA_PATTERNS.items():
            full_pattern = os.path.join(self.data_dir, pattern)
            found_files = glob.glob(full_pattern)
            files[data_type] = found_files
            logger.info(f"Found {len(found_files)} {data_type} files: {found_files}")
        
        return files
    
    def process_clients(self, client_files: List[str]) -> pd.DataFrame:
        """Process all client files and return normalized data."""
        logger.info("Processing client data...")
        
        if not client_files:
            logger.warning("No client files found")
            return pd.DataFrame()
        
        # Process files with the client processor
        client_data = self.client_processor.process_files(client_files)
        
        if not client_data.empty:
            logger.info(f"Processed {len(client_data)} client records")
            # Store in database
            self.db_manager.upsert_dataframe(
                client_data, 
                'clients', 
                conflict_columns=['client_id']
            )
            logger.info("Client data stored in database")
        
        return client_data
    
    def process_invoices(self, invoice_files: List[str]) -> pd.DataFrame:
        """Process all invoice files and return normalized data."""
        logger.info("Processing invoice data...")
        
        if not invoice_files:
            logger.warning("No invoice files found")
            return pd.DataFrame()
        
        # Process files with the invoice processor
        invoice_data = self.invoice_processor.process_files(invoice_files)
        
        if not invoice_data.empty:
            logger.info(f"Processed {len(invoice_data)} invoice records")
            # Store in database
            self.db_manager.upsert_dataframe(
                invoice_data, 
                'invoices', 
                conflict_columns=['invoice_id']
            )
            logger.info("Invoice data stored in database")
        
        return invoice_data
    
    def create_fact_table(self) -> None:
        """Create fact table by joining clients and invoices with additional calculations."""
        logger.info("Creating invoice facts table...")
        
        # SQL to create fact table with proper joins and calculations
        fact_sql = '''
        INSERT INTO invoice_facts (
            client_id, client_name, client_status, client_tier,
            invoice_id, invoice_date, invoice_amount, shipment_type,
            rate_per_unit, calculated_cost
        )
        SELECT DISTINCT
            COALESCE(c.client_id, i.client_id) as client_id,
            COALESCE(c.client_name, i.client_name) as client_name,
            c.status as client_status,
            c.tier as client_tier,
            i.invoice_id,
            i.invoice_date::date,
            i.amount as invoice_amount,
            i.shipment_type,
            rates.rate_per_unit,
            i.amount * rates.rate_per_unit as calculated_cost
        FROM invoices i
        LEFT JOIN clients c 
            ON c.client_id = i.client_id 
            OR UPPER(c.client_name) = UPPER(i.client_name)
        LEFT JOIN (
            VALUES
                ('GROUND', 1.0),
                ('2DAY', 5.0),
                ('EXPRESS', 10.0),
                ('FREIGHT', 20.0)
        ) AS rates(shipment_type, rate_per_unit)
            ON i.shipment_type = rates.shipment_type
        WHERE i.invoice_id IS NOT NULL
        ON CONFLICT (client_id, invoice_id) DO UPDATE SET
            client_name     = EXCLUDED.client_name,
            client_status   = EXCLUDED.client_status,
            client_tier     = EXCLUDED.client_tier,
            invoice_date    = EXCLUDED.invoice_date,
            invoice_amount  = EXCLUDED.invoice_amount,
            shipment_type   = EXCLUDED.shipment_type,
            rate_per_unit   = EXCLUDED.rate_per_unit,
            calculated_cost = EXCLUDED.calculated_cost;
        '''
        
        # Clear existing fact table data first for idempotency
        self.db_manager.execute_sql("DELETE FROM invoice_facts")
        
        # Execute fact table creation
        result = self.db_manager.execute_sql(fact_sql)
        
        # Get count of fact records
        count_result = self.db_manager.execute_sql("SELECT COUNT(*) FROM invoice_facts")
        fact_count = count_result.fetchone()[0]
        
        logger.info(f"Created {fact_count} fact table records")
    
    def run_analysis_queries(self) -> Dict[str, Any]:
        """Run all required analysis queries and return results."""
        logger.info("Running analysis queries...")
        
        results = {}
        
        # Query 1: Top 5 clients by total calculated costs
        logger.info("Query 1: Top 5 clients by total costs")
        q1_sql = '''
        SELECT 
            client_id,
            client_name,
            client_status,
            SUM(calculated_cost) as total_invoice_cost,
            COUNT(invoice_id) as invoice_count
        FROM invoice_facts 
        WHERE client_id IS NOT NULL
        GROUP BY client_id, client_name, client_status
        ORDER BY total_invoice_cost DESC
        LIMIT 5;
        '''
        q1_result = self.db_manager.execute_sql(q1_sql)
        results['top_5_clients'] = q1_result.fetchall()
        
        # Query 2: Month-over-month cost growth per client (2024-2025)
        logger.info("Query 2: Month-over-month cost growth per client")
        q2_sql = '''
        WITH monthly_totals AS (
            SELECT 
                client_id,
                client_name,
                DATE_TRUNC('month', invoice_date) as invoice_month,
                SUM(calculated_cost) as monthly_amount
            FROM invoice_facts 
            WHERE invoice_date >= '2024-01-01' 
                AND invoice_date < '2026-01-01'
                AND client_id IS NOT NULL
            GROUP BY client_id, client_name, DATE_TRUNC('month', invoice_date)
        ),
        with_previous AS (
            SELECT 
                *,
                LAG(monthly_amount) OVER (PARTITION BY client_id ORDER BY invoice_month) as prev_month_amount
            FROM monthly_totals
        )
        SELECT 
            client_id,
            client_name,
            invoice_month,
            monthly_amount,
            prev_month_amount,
            CASE 
                WHEN prev_month_amount IS NULL OR prev_month_amount = 0 THEN NULL
                ELSE ((monthly_amount - prev_month_amount) / prev_month_amount * 100)
            END as growth_percentage
        FROM with_previous
        WHERE prev_month_amount IS NOT NULL
        ORDER BY client_id, invoice_month;
        '''
        q2_result = self.db_manager.execute_sql(q2_sql)
        results['month_over_month_growth'] = q2_result.fetchall()
        
        # Query 3: Discount scenario analysis (using calculated_cost)
        logger.info("Query 3: Discount scenario analysis")
        q3_sql = '''
        WITH discounted_costs AS (
            SELECT 
                client_id,
                client_name,
                shipment_type,
                SUM(calculated_cost) as original_amount,
                SUM(CASE shipment_type
                    WHEN 'GROUND'  THEN calculated_cost * 0.8
                    WHEN 'FREIGHT' THEN calculated_cost * 0.7
                    WHEN '2DAY'    THEN calculated_cost * 0.5
                    ELSE calculated_cost
                END) as discounted_amount
            FROM invoice_facts
            WHERE client_id IS NOT NULL
            GROUP BY client_id, client_name, shipment_type
        )
        SELECT 
            client_id,
            client_name,
            SUM(original_amount) as total_original,
            SUM(discounted_amount) as total_discounted,
            SUM(original_amount) - SUM(discounted_amount) as total_savings
        FROM discounted_costs
        GROUP BY client_id, client_name
        ORDER BY total_discounted DESC
        LIMIT 5;
        '''
        q3_result = self.db_manager.execute_sql(q3_sql)
        results['discount_scenario_top_5'] = q3_result.fetchall()
        
        # Query 4: Reclassification scenario (EXPRESS -> GROUND using calculated_cost)
        logger.info("Query 4: EXPRESS to GROUND reclassification savings")
        q4_sql = '''
        WITH express_analysis AS (
            SELECT 
                client_id,
                client_name,
                COUNT(CASE WHEN shipment_type = 'EXPRESS' THEN 1 END) as express_shipments,
                SUM(CASE WHEN shipment_type = 'EXPRESS' THEN calculated_cost ELSE 0 END) as express_cost,
                SUM(CASE WHEN shipment_type = 'EXPRESS' THEN calculated_cost * 0.1 ELSE 0 END) as ground_equivalent_cost,
                SUM(calculated_cost) as total_cost
            FROM invoice_facts
            WHERE client_id IS NOT NULL
            GROUP BY client_id, client_name
            HAVING COUNT(CASE WHEN shipment_type = 'EXPRESS' THEN 1 END) > 0
        )
        SELECT 
            client_id,
            client_name,
            express_shipments,
            express_cost,
            ground_equivalent_cost,
            express_cost - ground_equivalent_cost as total_savings,
            ((express_cost - ground_equivalent_cost) / total_cost * 100) as savings_percentage,
            CASE 
                WHEN ((express_cost - ground_equivalent_cost) / total_cost * 100) > 50 THEN 'YES'
                ELSE 'NO'
            END as over_50_percent_savings,
            CASE 
                WHEN (express_cost - ground_equivalent_cost) > 500000 THEN 'YES'
                ELSE 'NO'
            END as over_500k_savings
        FROM express_analysis
        ORDER BY total_savings DESC;
        '''
        q4_result = self.db_manager.execute_sql(q4_sql)
        results['express_to_ground_analysis'] = q4_result.fetchall()
        
        logger.info("All analysis queries completed")
        return results

    
    def run_full_pipeline(self) -> Dict[str, Any]:
        """Run the complete data pipeline."""
        logger.info("Starting full pipeline execution...")
        
        try:
            # 1. Setup database
            self.setup_database()
            
            # 2. Find data files
            data_files = self.find_data_files()
            
            # 3. Process clients
            client_data = self.process_clients(data_files.get('clients', []))
            
            # 4. Process invoices  
            invoice_data = self.process_invoices(data_files.get('invoices', []))
            
            # 5. Create fact table
            if not client_data.empty or not invoice_data.empty:
                self.create_fact_table()
            
            # 6. Run analysis queries
            analysis_results = self.run_analysis_queries()
            
            logger.info("Pipeline execution completed successfully!")
            
            return {
                'client_count': len(client_data),
                'invoice_count': len(invoice_data),
                'analysis_results': analysis_results
            }
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            # Clean up database connection
            self.db_manager.disconnect()


def main():
    """Main entry point for the pipeline."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Reveel Data Pipeline')
    parser.add_argument('--data-dir', default='.', help='Directory containing data files')
    parser.add_argument('--log-level', default='INFO', help='Logging level')
    
    args = parser.parse_args()
    
    # Configure logging
    logger.remove()
    logger.add(lambda msg: print(msg, end=""), level=args.log_level)
    
    # Run pipeline
    pipeline = RevealPipeline(data_dir=args.data_dir)
    results = pipeline.run_full_pipeline()
    
    print("\\n=== PIPELINE RESULTS ===")
    print(f"Processed {results['client_count']} clients")
    print(f"Processed {results['invoice_count']} invoices")
    print("\\nAnalysis queries completed - check database for detailed results")


if __name__ == '__main__':
    main()