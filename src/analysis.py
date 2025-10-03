"""
Analysis queries module for the Reveel data pipeline.
Contains all business intelligence queries and report generation.
"""
import pandas as pd
from typing import Dict, Any, List
from loguru import logger

from .database import DatabaseManager


class AnalysisEngine:
    """Engine for running business analysis queries."""
    
    def __init__(self, db_manager: DatabaseManager):
        """Initialize with database manager."""
        self.db_manager = db_manager
        
    def run_all_analyses(self) -> Dict[str, Any]:
        """Run all analysis queries and return formatted results."""
        results = {}
        
        logger.info("Running comprehensive business analysis...")
        
        # Query 1: Top 5 clients by total costs
        results['top_5_clients'] = self.get_top_clients_by_revenue()
        
        # Query 2: Month-over-month growth analysis
        results['mom_growth'] = self.get_month_over_month_growth()
        
        # Query 3: Discount scenario analysis
        results['discount_analysis'] = self.get_discount_scenario_analysis()
        
        # Query 4: Express to Ground reclassification analysis
        results['reclassification_analysis'] = self.get_express_reclassification_analysis()
        
        # Additional insights
        results['summary_stats'] = self.get_summary_statistics()
        
        return results
    
    def get_top_clients_by_revenue(self) -> Dict[str, Any]:
        """Query 1: Top 5 clients by total calculated costs."""
        logger.info("Running Query 1: Top 5 clients by calculated costs")
        
        query = '''
        SELECT 
            client_id,
            client_name,
            client_status,
            SUM(calculated_cost) as total_invoice_cost,
            COUNT(invoice_id) as invoice_count,
            AVG(calculated_cost) as avg_invoice_cost
        FROM invoice_facts 
        WHERE client_id IS NOT NULL
        GROUP BY client_id, client_name, client_status
        ORDER BY total_invoice_cost DESC
        LIMIT 5;
        '''
        
        result = self.db_manager.execute_sql(query)
        data = result.fetchall()
        
        return {
            'query': 'Top 5 clients by total calculated costs',
            'data': data,
            'columns': ['client_id', 'client_name', 'client_status', 'total_cost', 'invoice_count', 'avg_invoice_cost'],
            'insights': [
                f"Top client: {data[0][1]} with ${data[0][3]:,.2f} in costs",
                f"Total costs from top 5: ${sum(row[3] for row in data):,.2f}",
                f"Average invoices per top client: {sum(row[4] for row in data) / len(data):.1f}"
            ]
        }
    
    def get_month_over_month_growth(self) -> Dict[str, Any]:
        """Query 2: Month-over-month cost growth per client for 2024-2025."""
        logger.info("Running Query 2: Month-over-month growth analysis")
        
        query = '''
        WITH monthly_totals AS (
            SELECT 
                client_id,
                client_name,
                DATE_TRUNC('month', invoice_date) as invoice_month,
                SUM(calculated_cost) as monthly_amount,
                COUNT(*) as monthly_invoices
            FROM invoice_facts 
            WHERE invoice_date >= '2024-01-01' 
                AND invoice_date < '2026-01-01'
                AND client_id IS NOT NULL
            GROUP BY client_id, client_name, DATE_TRUNC('month', invoice_date)
        ),
        with_previous AS (
            SELECT 
                *,
                LAG(monthly_amount) OVER (PARTITION BY client_id ORDER BY invoice_month) as prev_month_amount,
                LAG(monthly_invoices) OVER (PARTITION BY client_id ORDER BY invoice_month) as prev_month_invoices
            FROM monthly_totals
        )
        SELECT 
            client_id,
            client_name,
            invoice_month,
            monthly_amount,
            prev_month_amount,
            monthly_invoices,
            prev_month_invoices,
            CASE 
                WHEN prev_month_amount IS NULL OR prev_month_amount = 0 THEN NULL
                ELSE ((monthly_amount - prev_month_amount) / prev_month_amount * 100)
            END as growth_percentage
        FROM with_previous
        WHERE prev_month_amount IS NOT NULL
        ORDER BY client_id, invoice_month
        LIMIT 20;
        '''
        
        result = self.db_manager.execute_sql(query)
        data = result.fetchall()
        
        positive_growth = len([r for r in data if r[7] and r[7] > 0])
        negative_growth = len([r for r in data if r[7] and r[7] < 0])
        
        return {
            'query': 'Month-over-month cost growth per client (2024-2025)',
            'data': data,
            'columns': ['client_id', 'client_name', 'month', 'monthly_cost', 'prev_month_cost', 
                       'monthly_invoices', 'prev_month_invoices', 'growth_percentage'],
            'insights': [
                f"Periods with positive growth: {positive_growth}",
                f"Periods with negative growth: {negative_growth}",
                f"Growth rate range: {min([r[7] for r in data if r[7]]):.1f}% to {max([r[7] for r in data if r[7]]):.1f}%"
            ]
        }
    
    def get_discount_scenario_analysis(self) -> Dict[str, Any]:
        """Query 3: Discount scenario analysis (20% off GROUND, 30% off FREIGHT, 50% off 2DAY)."""
        logger.info("Running Query 3: Discount scenario analysis")
        
        query = '''
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
                END) as discounted_amount,
                COUNT(*) as shipment_count
            FROM invoice_facts
            WHERE client_id IS NOT NULL
            GROUP BY client_id, client_name, shipment_type
        ),
        client_totals AS (
            SELECT 
                client_id,
                client_name,
                SUM(original_amount) as total_original,
                SUM(discounted_amount) as total_discounted,
                SUM(original_amount) - SUM(discounted_amount) as total_savings,
                SUM(shipment_count) as total_shipments
            FROM discounted_costs
            GROUP BY client_id, client_name
        )
        SELECT 
            client_id,
            client_name,
            total_original,
            total_discounted,
            total_savings,
            (total_savings / total_original * 100) as savings_percentage,
            total_shipments
        FROM client_totals
        ORDER BY total_discounted DESC
        LIMIT 10;
        '''
        
        result = self.db_manager.execute_sql(query)
        data = result.fetchall()
        
        total_savings = sum(row[4] for row in data)
        total_original = sum(row[2] for row in data)
        
        return {
            'query': 'Discount scenario - new top 5 spenders after discounts',
            'data': data[:5],
            'columns': ['client_id', 'client_name', 'original_cost', 'discounted_cost', 
                       'total_savings', 'savings_percentage', 'total_shipments'],
            'insights': [
                f"Total savings for top 10 clients: ${total_savings:,.2f}",
                f"Average savings percentage: {(total_savings / total_original * 100):.1f}%",
                f"New #1 spender after discounts: {data[0][1]} (${data[0][3]:,.2f})"
            ]
        }
    
    def get_express_reclassification_analysis(self) -> Dict[str, Any]:
        """Query 4: EXPRESS to GROUND reclassification savings analysis."""
        logger.info("Running Query 4: EXPRESS to GROUND reclassification analysis")
        
        query = '''
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
            END as over_500k_savings,
            total_cost
        FROM express_analysis
        ORDER BY total_savings DESC;
        '''
        
        result = self.db_manager.execute_sql(query)
        data = result.fetchall()
        
        over_50_percent = [r for r in data if r[7] == 'YES']
        over_500k = [r for r in data if r[8] == 'YES']
        total_potential_savings = sum(row[5] for row in data)
        
        return {
            'query': 'EXPRESS to GROUND reclassification savings opportunity',
            'data': data[:10],
            'columns': ['client_id', 'client_name', 'express_shipments', 'express_cost', 
                       'ground_equivalent_cost', 'total_savings', 'savings_percentage', 
                       'over_50_percent_savings', 'over_500k_savings', 'total_cost'],
            'insights': [
                f"Total potential savings across all clients: ${total_potential_savings:,.2f}",
                f"Clients with >50% savings: {len(over_50_percent)} clients",
                f"Clients with >$500k savings: {len(over_500k)} clients",
                f"Biggest savings opportunity: {data[0][1]} (${data[0][5]:,.2f})"
            ],
            'answers': {
                'clients_over_50_percent_savings': [r[1] for r in over_50_percent],
                'clients_over_500k_savings': [r[1] for r in over_500k],
                'total_cost_savings_opportunity': total_potential_savings
            }
        }
    
    def get_summary_statistics(self) -> Dict[str, Any]:
        """Get overall pipeline and data summary statistics."""
        logger.info("Generating summary statistics")
        
        stats_query = '''
        SELECT 
            COUNT(DISTINCT client_id) as unique_clients,
            COUNT(DISTINCT invoice_id) as unique_invoices,
            SUM(calculated_cost) as total_costs,
            AVG(calculated_cost) as avg_invoice_cost,
            MIN(invoice_date) as earliest_invoice,
            MAX(invoice_date) as latest_invoice,
            COUNT(DISTINCT shipment_type) as unique_shipment_types
        FROM invoice_facts;
        '''
        
        result = self.db_manager.execute_sql(stats_query)
        stats = result.fetchone()
        
        shipment_query = '''
        SELECT 
            shipment_type,
            COUNT(*) as shipment_count,
            SUM(calculated_cost) as shipment_costs,
            AVG(calculated_cost) as avg_shipment_cost
        FROM invoice_facts
        GROUP BY shipment_type
        ORDER BY shipment_costs DESC;
        '''
        
        result = self.db_manager.execute_sql(shipment_query)
        shipment_data = result.fetchall()
        
        return {
            'overall_stats': {
                'unique_clients': stats[0],
                'unique_invoices': stats[1], 
                'total_costs': stats[2],
                'average_invoice_cost': stats[3],
                'date_range': f"{stats[4]} to {stats[5]}",
                'unique_shipment_types': stats[6]
            },
            'shipment_breakdown': shipment_data,
            'insights': [
                f"Data covers {stats[0]} unique clients and {stats[1]} invoices",
                f"Total calculated costs processed: ${stats[2]:,.2f}",
                f"Average invoice cost: ${stats[3]:,.2f}",
                f"Most valuable shipment type: {shipment_data[0][0]} (${shipment_data[0][2]:,.2f})"
            ]
        }
    
    def generate_analysis_report(self, results: Dict[str, Any]) -> str:
        """Generate a formatted analysis report as a string."""
        report_lines = []
        report_lines.append("\n" + "="*80)
        report_lines.append("REVEEL DATA ENGINEERING TAKE-HOME PROJECT - ANALYSIS REPORT")
        report_lines.append("="*80)
        
        # Summary Statistics
        report_lines.append("\nSUMMARY STATISTICS")
        report_lines.append("-" * 50)
        for insight in results['summary_stats']['insights']:
            report_lines.append(f"• {insight}")
        
        # Query 1 Results
        report_lines.append("\nQUERY 1: TOP 5 CLIENTS BY COSTS")
        report_lines.append("-" * 50)
        for i, row in enumerate(results['top_5_clients']['data'], 1):
            report_lines.append(f"{i}. {row[1]} ({row[0]}) - ${row[3]:,.2f} ({row[4]} invoices)")
        
        # Query 2 Results
        report_lines.append("\nQUERY 2: MONTH-OVER-MONTH GROWTH ANALYSIS")
        report_lines.append("-" * 50)
        mom_data = results['mom_growth']['data']
        if mom_data:
            # Show insights first
            for insight in results['mom_growth']['insights']:
                report_lines.append(f"• {insight}")
            report_lines.append("\nSample Growth Periods (first 10):")
            for row in mom_data[:10]:
                client_name = row[1]
                month = str(row[2])[:7]  # YYYY-MM format
                monthly_cost = row[3]
                prev_cost = row[4]
                growth_pct = row[7]
                if growth_pct is not None:
                    report_lines.append(f"  {client_name} - {month}: ${monthly_cost:,.2f} ({growth_pct:+.1f}% vs prev month)")
        else:
            report_lines.append("No month-over-month growth data available")
        
        # Query 3 Results
        report_lines.append("\nQUERY 3: DISCOUNT SCENARIO - NEW TOP 5 SPENDERS")
        report_lines.append("-" * 50)
        for i, row in enumerate(results['discount_analysis']['data'], 1):
            savings_pct = row[5]
            report_lines.append(f"{i}. {row[1]} - ${row[3]:,.2f} after discounts (saved ${row[4]:,.2f}, {savings_pct:.1f}%)")
        
        # Query 4 Results
        report_lines.append("\nQUERY 4: EXPRESS→GROUND RECLASSIFICATION SAVINGS")
        report_lines.append("-" * 50)
        answers = results['reclassification_analysis']['answers']
        report_lines.append(f"Total potential savings: ${answers['total_cost_savings_opportunity']:,.2f}")
        report_lines.append(f"Clients with >50% savings: {len(answers['clients_over_50_percent_savings'])}")
        report_lines.append(f"Clients with >$500k savings: {len(answers['clients_over_500k_savings'])}")
        
        if answers['clients_over_500k_savings']:
            report_lines.append("\nClients with >$500k savings opportunity:")
            for client in answers['clients_over_500k_savings'][:10]:
                report_lines.append(f"  • {client}")
        
        # Shipment Type Analysis
        report_lines.append("\nSHIPMENT TYPE BREAKDOWN")
        report_lines.append("-" * 50)
        for shipment_type, count, costs, avg_cost in results['summary_stats']['shipment_breakdown']:
            report_lines.append(f"{shipment_type}: {count:,} shipments, ${costs:,.2f} total (avg: ${avg_cost:,.2f})")
        
        report_lines.append("\n" + "="*80)
        return "\n".join(report_lines)
    
    def print_analysis_report(self, results: Dict[str, Any]) -> None:
        """Print a formatted analysis report to console and log to file."""
        report_content = self.generate_analysis_report(results)
        
        # Print to console
        print(report_content)
        
        # Log to file (split into individual lines to respect logger formatting)
        logger.info("=== ANALYSIS REPORT START ===")
        for line in report_content.split('\n'):
            if line.strip():  # Skip empty lines
                logger.info(line)
        logger.info("=== ANALYSIS REPORT END ===")
