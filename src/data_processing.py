"""
Data processing utilities for normalizing and cleaning client and invoice data.
"""
import os
import re
import hashlib
import glob
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from PyPDF2 import PdfReader
import dateutil.parser as dparser
from loguru import logger

from .config import RATE_SHEET


def _row_hash(row: Dict[str, Any]) -> str:
    """Generate hash for row deduplication."""
    s = "|".join(f"{k}={row.get(k)}" for k in sorted(row.keys()))
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _clean_name(x: Union[str, float, None]) -> Optional[str]:
    """Clean and standardize names."""
    if pd.isna(x):
        return None
    s = " ".join(str(x).strip().split())
    return " ".join(w if (w.isupper() and len(w) <= 3) else w.title() for w in s.split())


def _norm_status(x: Union[str, float, None]) -> str:
    """Normalize status values."""
    if pd.isna(x):
        return "UNKNOWN"
    s = str(x).strip().lower()
    if s in {"active", "act", "y", "yes", "true", "1"}:
        return "ACTIVE"
    if s in {"inactive", "inact", "n", "no", "false", "0"}:
        return "INACTIVE"
    if s.startswith("active"):
        return "ACTIVE"
    if s.startswith("inact"):
        return "INACTIVE"
    return "UNKNOWN"


def _norm_shipment_type(x: Union[str, float, None]) -> str:
    """Normalize shipment type values."""
    if pd.isna(x):
        return "UNKNOWN"
    s = str(x).strip().upper()
    
    # Handle variations
    variations = {
        "2DAY": ["2 DAY", "TWO DAY", "2-DAY"],
        "GROUND": ["GND", "STANDARD", "REGULAR"],
        "EXPRESS": ["EXP", "NEXT DAY", "OVERNIGHT"],
        "FREIGHT": ["FRT", "CARGO", "HEAVY"]
    }
    
    for standard, variants in variations.items():
        if s == standard or s in variants:
            return standard
    
    return s if s in RATE_SHEET else "UNKNOWN"


def _parse_date(x: Union[str, float, None]) -> pd.Timestamp:
    """Parse various date formats to standardized datetime."""
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT
    try:
        # Try pandas first (handles most formats)
        return pd.to_datetime(x, errors="coerce", utc=True)
    except Exception:
        try:
            # Fallback to dateutil for complex formats
            return pd.to_datetime(dparser.parse(str(x), fuzzy=True), utc=True)
        except Exception:
            logger.warning(f"Could not parse date: {x}")
            return pd.NaT


def _parse_amount(x: Union[str, float, int, None]) -> float:
    """Parse monetary amounts, handling various formats."""
    if pd.isna(x):
        return 0.0
    
    if isinstance(x, (int, float)):
        return float(x)
    
    # Clean string amounts
    s = str(x).strip()
    # Remove currency symbols and commas
    s = re.sub(r'[^\d.-]', '', s)
    
    try:
        return float(s)
    except (ValueError, TypeError):
        logger.warning(f"Could not parse amount: {x}")
        return 0.0


class ClientProcessor:
    """Processes client data from various file formats and schemas."""
    
    def __init__(self):
        self.required_columns = ["client_id", "client_name", "status", "tier", "created_at", "currency"]
    
    def read_pdf(self, path: str) -> pd.DataFrame:
        """Extract client data from PDF files."""
        logger.info(f"Processing PDF file: {path}")
        
        try:
            reader = PdfReader(path)
            text = "\\n".join((p.extract_text() or "") for p in reader.pages)
            lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
            
            records = []
            i = 0
            while i < len(lines):
                # Look for client_id pattern (C followed by 5 digits)
                if re.match(r"^C\d{5}$", lines[i]):
                    if i + 3 < len(lines):
                        client_id = lines[i]
                        client_name = lines[i + 1]
                        status = lines[i + 2] 
                        created_at = lines[i + 3]
                        
                        records.append({
                            'client_id': client_id,
                            'client_name': client_name,
                            'status': status,
                            'created_at': created_at,
                            'tier': None,
                            'currency': 'USD'
                        })
                        i += 4
                    else:
                        i += 1
                else:
                    i += 1
            
            df = pd.DataFrame(records)
            logger.info(f"Extracted {len(df)} client records from PDF")
            return df
            
        except Exception as e:
            logger.error(f"Error processing PDF {path}: {e}")
            return pd.DataFrame(columns=self.required_columns)
    
    def read_csv(self, path: str) -> pd.DataFrame:
        """Read client data from CSV files, handling different schemas."""
        logger.info(f"Processing CSV file: {path}")
        
        try:
            df = pd.read_csv(path)
            logger.info(f"Read {len(df)} rows from {path}")
            
            # Normalize column names for easier matching
            cols_lower = {c.lower(): c for c in df.columns}
            
            # Detect schema version and map columns
            if "id" in cols_lower and "tier" in cols_lower:
                # Schema v2
                logger.debug("Detected schema v2")
                column_mapping = {
                    cols_lower.get("id", "id"): "client_id",
                    cols_lower.get("name", "name"): "client_name", 
                    cols_lower.get("tier", "tier"): "tier",
                    cols_lower.get("acct_open_date", "acct_open_date"): "created_at"
                }
            elif "customer_key" in cols_lower and "display_name" in cols_lower:
                # Schema v3
                logger.debug("Detected schema v3")
                column_mapping = {
                    cols_lower.get("customer_key", "customer_key"): "client_id",
                    cols_lower.get("display_name", "display_name"): "client_name",
                    cols_lower.get("active_flag", "active_flag"): "status",
                    cols_lower.get("signup_ts", "signup_ts"): "created_at",
                    cols_lower.get("currency", "currency"): "currency"
                }
            else:
                # Schema v1 (default)
                logger.debug("Detected schema v1")
                column_mapping = {
                    cols_lower.get("client_id", "client_id"): "client_id",
                    cols_lower.get("client_name", "client_name"): "client_name",
                    cols_lower.get("status", "status"): "status", 
                    cols_lower.get("created_at", "created_at"): "created_at"
                }
            
            # Apply column mapping
            for old_col, new_col in column_mapping.items():
                if old_col in df.columns:
                    df = df.rename(columns={old_col: new_col})
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing CSV {path}: {e}")
            return pd.DataFrame(columns=self.required_columns)
    
    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize client dataframe to standard schema."""
        if df.empty:
            return df
        
        logger.info(f"Normalizing {len(df)} client records")
        
        # Ensure all required columns exist
        for col in self.required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Apply normalization functions
        df['client_name'] = df['client_name'].apply(_clean_name)
        df['status'] = df['status'].apply(_norm_status)
        df['tier'] = df['tier'].fillna("UNKNOWN").astype(str).str.upper()
        df['currency'] = df['currency'].fillna("USD").astype(str).str.upper()
        df['created_at_dt'] = df['created_at'].apply(_parse_date)
        
        # Uppercase string columns
        string_cols = ['client_id', 'client_name', 'status', 'tier', 'currency']
        for col in string_cols:
            df[col] = df[col].astype(str).str.upper()
        
        # Handle deduplication within file
        status_rank = {"ACTIVE": 2, "INACTIVE": 1, "UNKNOWN": 0}
        df['status_rank'] = df['status'].map(status_rank).fillna(0)
        
        # Sort by client_id, then by status preference, then by date (newest first)
        df = df.sort_values(['client_id', 'status_rank', 'created_at_dt'], 
                           ascending=[True, False, False])
        
        # Remove duplicates, keeping the "best" record for each client
        df = df.drop_duplicates('client_id', keep='first')
        
        # Finalize created_at as string
        df['created_at'] = df['created_at_dt'].dt.strftime('%Y-%m-%d')
        df['created_at'] = df['created_at'].replace('NaT', None)
        
        # Add row hash for change detection
        df['row_hash'] = df.apply(lambda row: _row_hash(row[self.required_columns].to_dict()), axis=1)
        
        # Select final columns
        final_df = df[self.required_columns + ['row_hash']].copy()
        
        logger.info(f"Normalized to {len(final_df)} unique client records")
        return final_df
    
    def process_files(self, file_patterns: List[str]) -> pd.DataFrame:
        """Process multiple client files and return merged DataFrame."""
        all_dfs = []
        
        for pattern in file_patterns:
            files = glob.glob(pattern)
            logger.info(f"Found {len(files)} files matching pattern: {pattern}")
            
            for file_path in files:
                if file_path.lower().endswith('.pdf'):
                    df = self.read_pdf(file_path)
                elif file_path.lower().endswith('.csv'):
                    df = self.read_csv(file_path)
                else:
                    logger.warning(f"Unsupported file format: {file_path}")
                    continue
                
                if not df.empty:
                    df = self.normalize_dataframe(df)
                    all_dfs.append(df)
        
        if not all_dfs:
            logger.warning("No client data found")
            return pd.DataFrame(columns=self.required_columns + ['row_hash'])
        
        # Merge all dataframes
        logger.info("Merging client data from all files")
        return self.merge_dataframes(all_dfs)
    
    def merge_dataframes(self, dfs: List[pd.DataFrame]) -> pd.DataFrame:
        """Merge multiple client DataFrames with conflict resolution."""
        if not dfs:
            return pd.DataFrame(columns=self.required_columns + ['row_hash'])
        
        all_df = pd.concat(dfs, ignore_index=True)
        
        # Use client_id as primary key, fall back to client_name if invalid
        valid_id_mask = all_df['client_id'].astype(str).str.match(r'^C\d{5}$', na=False)
        all_df['merge_key'] = all_df['client_id'].where(valid_id_mask, all_df['client_name'])
        
        # Parse dates for comparison
        all_df['created_at_dt'] = pd.to_datetime(all_df['created_at'], errors='coerce')
        status_rank = {"ACTIVE": 2, "INACTIVE": 1, "UNKNOWN": 0}
        all_df['status_rank'] = all_df['status'].map(status_rank).fillna(0)
        
        merged_rows = []
        for key, group in all_df.groupby('merge_key', sort=False):
            # Sort by date (newest first) and status rank (best first)
            group = group.sort_values(['created_at_dt', 'status_rank'], 
                                    ascending=[False, False])
            
            # Start with the "best" record
            base = group.iloc[0].to_dict()
            
            # Backfill missing fields from other records
            for _, row in group.iterrows():
                for col in self.required_columns:
                    if (not base.get(col) or 
                        base[col] in {'NONE', 'NAN', 'UNKNOWN', 'nan'}):
                        if pd.notna(row[col]) and str(row[col]).strip():
                            base[col] = row[col]
            
            # Ensure uppercase consistency
            for col in ['client_id', 'client_name', 'status', 'tier', 'currency']:
                if col in base and pd.notna(base[col]):
                    base[col] = str(base[col]).upper()
            
            # Finalize created_at
            if pd.notna(base.get('created_at_dt')):
                base['created_at'] = base['created_at_dt'].strftime('%Y-%m-%d')
            
            # Generate row hash
            base['row_hash'] = _row_hash({k: base.get(k) for k in self.required_columns})
            
            merged_rows.append(base)
        
        final_df = pd.DataFrame(merged_rows)
        final_columns = self.required_columns + ['row_hash']
        result = final_df[final_columns].copy()
        
        logger.info(f"Merged to {len(result)} unique client records")
        return result


class InvoiceProcessor:
    """Processes invoice data from various file formats and schemas."""
    
    def __init__(self):
        self.required_columns = ["invoice_id", "client_id", "client_name", "invoice_date", 
                               "amount", "currency", "shipment_type"]
    
    def read_csv(self, path: str) -> pd.DataFrame:
        """Read invoice data from CSV files, handling different schemas."""
        logger.info(f"Processing invoice CSV file: {path}")
        
        try:
            df = pd.read_csv(path)
            logger.info(f"Read {len(df)} invoice rows from {path}")
            
            # Normalize column names
            cols_lower = {c.lower().replace(' ', '_'): c for c in df.columns}
            
            # Detect schema version and map columns
            if "inv_no" in cols_lower and "customer_key" in cols_lower:
                # Schema v2
                logger.debug("Detected invoice schema v2")
                column_mapping = {
                    cols_lower.get("inv_no"): "invoice_id",
                    cols_lower.get("customer_key"): "client_id", 
                    cols_lower.get("inv_dt"): "invoice_date",
                    cols_lower.get("total", cols_lower.get("subtotal")): "amount",
                    cols_lower.get("curr"): "currency",
                    cols_lower.get("ship_type"): "shipment_type"
                }
            elif "invoice_uid" in cols_lower and "client_ref" in cols_lower:
                # Schema v3
                logger.debug("Detected invoice schema v3")
                column_mapping = {
                    cols_lower.get("invoice_uid"): "invoice_id",
                    cols_lower.get("client_ref"): "client_name",
                    cols_lower.get("issued_on"): "invoice_date", 
                    cols_lower.get("amount_usd"): "amount",
                    cols_lower.get("shipment_category"): "shipment_type"
                }
                # Schema v3 uses client names instead of IDs
            else:
                # Schema v1 (default)
                logger.debug("Detected invoice schema v1")  
                column_mapping = {
                    cols_lower.get("invoice_id"): "invoice_id",
                    cols_lower.get("client_id"): "client_id",
                    cols_lower.get("invoice_date"): "invoice_date",
                    cols_lower.get("amount"): "amount",
                    cols_lower.get("currency"): "currency", 
                    cols_lower.get("shipment_type"): "shipment_type"
                }
            
            # Apply column mapping
            for old_col, new_col in column_mapping.items():
                if old_col and old_col in df.columns and new_col:
                    df = df.rename(columns={old_col: new_col})
            
            return df
            
        except Exception as e:
            logger.error(f"Error processing invoice CSV {path}: {e}")
            return pd.DataFrame(columns=self.required_columns)
    
    def normalize_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize invoice dataframe to standard schema."""
        if df.empty:
            return df
            
        logger.info(f"Normalizing {len(df)} invoice records")
        
        # Ensure all required columns exist
        for col in self.required_columns:
            if col not in df.columns:
                df[col] = None
        
        # Apply normalization functions
        df['invoice_date'] = df['invoice_date'].apply(_parse_date)
        df['amount'] = df['amount'].apply(_parse_amount)
        df['shipment_type'] = df['shipment_type'].apply(_norm_shipment_type)
        df['currency'] = df['currency'].fillna("USD").astype(str).str.upper()
        
        # Clean client names
        df['client_name'] = df['client_name'].apply(_clean_name)
        
        # Uppercase string columns
        string_cols = ['invoice_id', 'client_id', 'client_name', 'currency', 'shipment_type']
        for col in string_cols:
            df[col] = df[col].astype(str).str.upper().replace('NAN', None)
        
        # Convert dates back to string format
        df['invoice_date'] = df['invoice_date'].dt.strftime('%Y-%m-%d')
        df['invoice_date'] = df['invoice_date'].replace('NaT', None)
        
        # Add row hash for change detection
        df['row_hash'] = df.apply(lambda row: _row_hash(row[self.required_columns].to_dict()), axis=1)
        
        # Remove duplicates based on invoice_id
        df = df.drop_duplicates('invoice_id', keep='first')
        
        # Select final columns
        final_df = df[self.required_columns + ['row_hash']].copy()
        
        logger.info(f"Normalized to {len(final_df)} unique invoice records")
        return final_df
    
    def process_files(self, file_patterns: List[str]) -> pd.DataFrame:
        """Process multiple invoice files and return merged DataFrame."""
        all_dfs = []
        
        for pattern in file_patterns:
            files = glob.glob(pattern)
            logger.info(f"Found {len(files)} invoice files matching pattern: {pattern}")
            
            for file_path in files:
                if file_path.lower().endswith('.csv'):
                    df = self.read_csv(file_path)
                    if not df.empty:
                        df = self.normalize_dataframe(df)
                        all_dfs.append(df)
                else:
                    logger.warning(f"Unsupported invoice file format: {file_path}")
        
        if not all_dfs:
            logger.warning("No invoice data found")
            return pd.DataFrame(columns=self.required_columns + ['row_hash'])
        
        # Concatenate all invoice dataframes
        logger.info("Merging invoice data from all files")
        result = pd.concat(all_dfs, ignore_index=True)
        result = result.drop_duplicates('invoice_id', keep='first')
        
        logger.info(f"Final merged invoice data: {len(result)} records")
        return result