2025-10-02 18:30:12.299 | INFO     | __main__:__init__:28 - Pipeline initialized with data directory: data files
2025-10-02 18:30:12.300 | INFO     | __main__:run_full_pipeline:293 - Starting full pipeline execution...
2025-10-02 18:30:12.300 | INFO     | __main__:setup_database:32 - Setting up database...
2025-10-02 18:30:12.322 | INFO     | src.database:connect:35 - Database connection established
2025-10-02 18:30:12.322 | INFO     | src.database:create_tables:83 - Creating database tables...
2025-10-02 18:30:12.337 | INFO     | src.database:create_tables:162 - Database tables created successfully
2025-10-02 18:30:12.337 | INFO     | __main__:setup_database:35 - Database setup complete
2025-10-02 18:30:12.337 | INFO     | __main__:find_data_files:39 - Searching for data files in: data files
2025-10-02 18:30:12.337 | INFO     | __main__:find_data_files:46 - Found 3 clients files: ['data files/clients_v1 (2).csv', 'data files/clients_v2 (2).csv', 'data files/clients_v3 (2).csv']
2025-10-02 18:30:12.337 | INFO     | __main__:find_data_files:46 - Found 3 invoices files: ['data files/invoices_v1 (3).csv', 'data files/invoices_v2 (2).csv', 'data files/invoices_v3 (2).csv']
2025-10-02 18:30:12.337 | INFO     | __main__:find_data_files:46 - Found 1 client_pdfs files: ['data files/clients_v1 (1).pdf']
2025-10-02 18:30:12.338 | INFO     | __main__:find_data_files:46 - Found 0 invoice_pdfs files: []
2025-10-02 18:30:12.338 | INFO     | __main__:process_clients:52 - Processing client data...
2025-10-02 18:30:12.338 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v1 (2).csv
2025-10-02 18:30:12.338 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v1 (2).csv
2025-10-02 18:30:12.341 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v1 (2).csv
2025-10-02 18:30:12.341 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:12.363 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:12.363 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v2 (2).csv
2025-10-02 18:30:12.363 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v2 (2).csv
2025-10-02 18:30:12.364 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v2 (2).csv
2025-10-02 18:30:12.364 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:12.380 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:12.380 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v3 (2).csv
2025-10-02 18:30:12.380 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v3 (2).csv
2025-10-02 18:30:12.381 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v3 (2).csv
2025-10-02 18:30:12.381 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:12.399 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:12.399 | INFO     | src.data_processing:process_files:278 - Merging client data from all files
2025-10-02 18:30:12.429 | INFO     | src.data_processing:merge_dataframes:332 - Merged to 60 unique client records
2025-10-02 18:30:12.429 | INFO     | __main__:process_clients:62 - Processed 60 client records
2025-10-02 18:30:12.429 | INFO     | src.database:upsert_dataframe:186 - Upserting 60 rows to clients
2025-10-02 18:30:12.444 | INFO     | src.database:upsert_dataframe:243 - Successfully upserted data to clients
2025-10-02 18:30:12.444 | INFO     | __main__:process_clients:69 - Client data stored in database
2025-10-02 18:30:12.444 | INFO     | __main__:process_invoices:75 - Processing invoice data...
2025-10-02 18:30:12.444 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v1 (3).csv
2025-10-02 18:30:12.444 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v1 (3).csv
2025-10-02 18:30:12.451 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v1 (3).csv
2025-10-02 18:30:12.453 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:14.268 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:14.268 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v2 (2).csv
2025-10-02 18:30:14.268 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v2 (2).csv
2025-10-02 18:30:14.274 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v2 (2).csv
2025-10-02 18:30:14.275 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:15.887 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:15.887 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v3 (2).csv
2025-10-02 18:30:15.887 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v3 (2).csv
2025-10-02 18:30:15.892 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v3 (2).csv
2025-10-02 18:30:15.893 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:17.658 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:17.659 | INFO     | src.data_processing:process_files:464 - Merging invoice data from all files
2025-10-02 18:30:17.661 | INFO     | src.data_processing:process_files:468 - Final merged invoice data: 12000 records
2025-10-02 18:30:17.663 | INFO     | __main__:process_invoices:85 - Processed 12000 invoice records
2025-10-02 18:30:17.663 | INFO     | src.database:upsert_dataframe:186 - Upserting 12000 rows to invoices
2025-10-02 18:30:18.405 | INFO     | src.database:upsert_dataframe:243 - Successfully upserted data to invoices
2025-10-02 18:30:18.406 | INFO     | __main__:process_invoices:92 - Invoice data stored in database
2025-10-02 18:30:18.406 | INFO     | __main__:create_fact_table:98 - Creating invoice facts table...
2025-10-02 18:30:18.624 | INFO     | __main__:create_fact_table:152 - Created 12000 fact table records
2025-10-02 18:30:18.625 | INFO     | __main__:run_analysis_queries:156 - Running analysis queries...
2025-10-02 18:30:18.625 | INFO     | __main__:run_analysis_queries:161 - Query 1: Top 5 clients by total costs
2025-10-02 18:30:18.627 | INFO     | __main__:run_analysis_queries:179 - Query 2: Month-over-month cost growth per client
2025-10-02 18:30:18.639 | INFO     | __main__:run_analysis_queries:217 - Query 3: Discount scenario analysis
2025-10-02 18:30:18.643 | INFO     | __main__:run_analysis_queries:250 - Query 4: EXPRESS to GROUND reclassification savings
2025-10-02 18:30:18.646 | INFO     | __main__:run_analysis_queries:287 - All analysis queries completed
2025-10-02 18:30:18.646 | INFO     | __main__:run_full_pipeline:315 - Pipeline execution completed successfully!
2025-10-02 18:30:18.646 | INFO     | src.database:disconnect:44 - Database connection closed
2025-10-02 18:30:23.564 | INFO     | __main__:__init__:28 - Pipeline initialized with data directory: data files
2025-10-02 18:30:23.564 | INFO     | __main__:run_full_pipeline:293 - Starting full pipeline execution...
2025-10-02 18:30:23.564 | INFO     | __main__:setup_database:32 - Setting up database...
2025-10-02 18:30:23.586 | INFO     | src.database:connect:35 - Database connection established
2025-10-02 18:30:23.586 | INFO     | src.database:create_tables:83 - Creating database tables...
2025-10-02 18:30:23.601 | INFO     | src.database:create_tables:162 - Database tables created successfully
2025-10-02 18:30:23.601 | INFO     | __main__:setup_database:35 - Database setup complete
2025-10-02 18:30:23.601 | INFO     | __main__:find_data_files:39 - Searching for data files in: data files
2025-10-02 18:30:23.602 | INFO     | __main__:find_data_files:46 - Found 3 clients files: ['data files/clients_v1 (2).csv', 'data files/clients_v2 (2).csv', 'data files/clients_v3 (2).csv']
2025-10-02 18:30:23.602 | INFO     | __main__:find_data_files:46 - Found 3 invoices files: ['data files/invoices_v1 (3).csv', 'data files/invoices_v2 (2).csv', 'data files/invoices_v3 (2).csv']
2025-10-02 18:30:23.602 | INFO     | __main__:find_data_files:46 - Found 1 client_pdfs files: ['data files/clients_v1 (1).pdf']
2025-10-02 18:30:23.602 | INFO     | __main__:find_data_files:46 - Found 0 invoice_pdfs files: []
2025-10-02 18:30:23.602 | INFO     | __main__:process_clients:52 - Processing client data...
2025-10-02 18:30:23.602 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v1 (2).csv
2025-10-02 18:30:23.602 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v1 (2).csv
2025-10-02 18:30:23.604 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v1 (2).csv
2025-10-02 18:30:23.605 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:23.625 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:23.625 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v2 (2).csv
2025-10-02 18:30:23.625 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v2 (2).csv
2025-10-02 18:30:23.626 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v2 (2).csv
2025-10-02 18:30:23.626 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:23.643 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:23.643 | INFO     | src.data_processing:process_files:258 - Found 1 files matching pattern: data files/clients_v3 (2).csv
2025-10-02 18:30:23.643 | INFO     | src.data_processing:read_csv:154 - Processing CSV file: data files/clients_v3 (2).csv
2025-10-02 18:30:23.643 | INFO     | src.data_processing:read_csv:158 - Read 61 rows from data files/clients_v3 (2).csv
2025-10-02 18:30:23.644 | INFO     | src.data_processing:normalize_dataframe:209 - Normalizing 61 client records
2025-10-02 18:30:23.662 | INFO     | src.data_processing:normalize_dataframe:249 - Normalized to 60 unique client records
2025-10-02 18:30:23.662 | INFO     | src.data_processing:process_files:278 - Merging client data from all files
2025-10-02 18:30:23.691 | INFO     | src.data_processing:merge_dataframes:332 - Merged to 60 unique client records
2025-10-02 18:30:23.691 | INFO     | __main__:process_clients:62 - Processed 60 client records
2025-10-02 18:30:23.691 | INFO     | src.database:upsert_dataframe:186 - Upserting 60 rows to clients
2025-10-02 18:30:23.708 | INFO     | src.database:upsert_dataframe:243 - Successfully upserted data to clients
2025-10-02 18:30:23.708 | INFO     | __main__:process_clients:69 - Client data stored in database
2025-10-02 18:30:23.708 | INFO     | __main__:process_invoices:75 - Processing invoice data...
2025-10-02 18:30:23.708 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v1 (3).csv
2025-10-02 18:30:23.708 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v1 (3).csv
2025-10-02 18:30:23.716 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v1 (3).csv
2025-10-02 18:30:23.717 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:25.544 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:25.544 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v2 (2).csv
2025-10-02 18:30:25.544 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v2 (2).csv
2025-10-02 18:30:25.550 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v2 (2).csv
2025-10-02 18:30:25.551 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:27.183 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:27.183 | INFO     | src.data_processing:process_files:448 - Found 1 invoice files matching pattern: data files/invoices_v3 (2).csv
2025-10-02 18:30:27.183 | INFO     | src.data_processing:read_csv:345 - Processing invoice CSV file: data files/invoices_v3 (2).csv
2025-10-02 18:30:27.189 | INFO     | src.data_processing:read_csv:349 - Read 12060 invoice rows from data files/invoices_v3 (2).csv
2025-10-02 18:30:27.190 | INFO     | src.data_processing:normalize_dataframe:405 - Normalizing 12060 invoice records
2025-10-02 18:30:28.948 | INFO     | src.data_processing:normalize_dataframe:439 - Normalized to 12000 unique invoice records
2025-10-02 18:30:28.948 | INFO     | src.data_processing:process_files:464 - Merging invoice data from all files
2025-10-02 18:30:28.951 | INFO     | src.data_processing:process_files:468 - Final merged invoice data: 12000 records
2025-10-02 18:30:28.952 | INFO     | __main__:process_invoices:85 - Processed 12000 invoice records
2025-10-02 18:30:28.952 | INFO     | src.database:upsert_dataframe:186 - Upserting 12000 rows to invoices
2025-10-02 18:30:29.726 | INFO     | src.database:upsert_dataframe:243 - Successfully upserted data to invoices
2025-10-02 18:30:29.727 | INFO     | __main__:process_invoices:92 - Invoice data stored in database
2025-10-02 18:30:29.727 | INFO     | __main__:create_fact_table:98 - Creating invoice facts table...
2025-10-02 18:30:29.958 | INFO     | __main__:create_fact_table:152 - Created 12000 fact table records
2025-10-02 18:30:29.958 | INFO     | __main__:run_analysis_queries:156 - Running analysis queries...
2025-10-02 18:30:29.958 | INFO     | __main__:run_analysis_queries:161 - Query 1: Top 5 clients by total costs
2025-10-02 18:30:29.961 | INFO     | __main__:run_analysis_queries:179 - Query 2: Month-over-month cost growth per client
2025-10-02 18:30:29.972 | INFO     | __main__:run_analysis_queries:217 - Query 3: Discount scenario analysis
2025-10-02 18:30:29.976 | INFO     | __main__:run_analysis_queries:250 - Query 4: EXPRESS to GROUND reclassification savings
2025-10-02 18:30:29.979 | INFO     | __main__:run_analysis_queries:287 - All analysis queries completed
2025-10-02 18:30:29.980 | INFO     | __main__:run_full_pipeline:315 - Pipeline execution completed successfully!
2025-10-02 18:30:29.980 | INFO     | src.database:disconnect:44 - Database connection closed
2025-10-02 18:31:12.041 | INFO     | src.database:connect:35 - Database connection established
2025-10-02 18:31:12.041 | INFO     | src.analysis:run_all_analyses:23 - Running comprehensive business analysis...
2025-10-02 18:31:12.041 | INFO     | src.analysis:get_top_clients_by_revenue:44 - Running Query 1: Top 5 clients by calculated costs
2025-10-02 18:31:12.054 | INFO     | src.analysis:get_month_over_month_growth:77 - Running Query 2: Month-over-month growth analysis
2025-10-02 18:31:12.065 | INFO     | src.analysis:get_discount_scenario_analysis:138 - Running Query 3: Discount scenario analysis
2025-10-02 18:31:12.073 | INFO     | src.analysis:get_express_reclassification_analysis:202 - Running Query 4: EXPRESS to GROUND reclassification analysis
2025-10-02 18:31:12.080 | INFO     | src.analysis:get_summary_statistics:267 - Generating summary statistics
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:392 - === ANALYSIS REPORT START ===
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - ================================================================================
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - REVEEL DATA ENGINEERING TAKE-HOME PROJECT - ANALYSIS REPORT
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - ================================================================================
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - SUMMARY STATISTICS
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Data covers 60 unique clients and 12000 invoices
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Total calculated costs processed: $1,160,627,072.61
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Average invoice cost: $96,718.92
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Most valuable shipment type: FREIGHT ($714,788,073.40)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - QUERY 1: TOP 5 CLIENTS BY COSTS
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - 1. STARK PARTNERS (C25055) - $26,920,738.40 (210 invoices)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - 2. WAYNE GROUP (C14175) - $26,033,563.39 (211 invoices)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - 3. UMBRELLA INDUSTRIES (C03366) - $24,438,391.01 (212 invoices)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - 4. WONKA LLC (C60889) - $24,313,081.28 (204 invoices)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - 5. RED LOGISTICS (C94736) - $23,986,771.70 (244 invoices)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - QUERY 2: MONTH-OVER-MONTH GROWTH ANALYSIS
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Periods with positive growth: 11
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Periods with negative growth: 9
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - • Growth rate range: -83.6% to 1595.9%
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 - Sample Growth Periods (first 10):
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-02: $1,160,280.10 (+1595.9% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-03: $1,058,454.09 (-8.8% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-04: $173,063.23 (-83.6% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-05: $846,001.13 (+388.8% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-06: $658,851.61 (-22.1% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-07: $1,241,111.40 (+88.4% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-08: $234,272.06 (-81.1% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-09: $272,576.97 (+16.4% vs prev month)
2025-10-02 18:31:12.110 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-10: $489,969.41 (+79.8% vs prev month)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   ZENITH SUPPLY - 2024-11: $204,679.83 (-58.2% vs prev month)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - QUERY 3: DISCOUNT SCENARIO - NEW TOP 5 SPENDERS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 1. STARK PARTNERS - $21,002,962.12 after discounts (saved $5,917,776.28, 22.0%)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 2. WAYNE GROUP - $20,012,098.51 after discounts (saved $6,021,464.88, 23.1%)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 3. UMBRELLA INDUSTRIES - $18,996,296.21 after discounts (saved $5,442,094.80, 22.3%)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 4. WONKA LLC - $18,920,442.44 after discounts (saved $5,392,638.84, 22.2%)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 5. RED LOGISTICS - $18,790,361.92 after discounts (saved $5,196,409.78, 21.7%)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - QUERY 4: EXPRESS→GROUND RECLASSIFICATION SAVINGS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - Total potential savings: $335,182,700.52
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - Clients with >50% savings: 1
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - Clients with >$500k savings: 60
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - Clients with >$500k savings opportunity:
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • MASSIVE CO
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • WAYNE HOLDINGS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • WAYNE FREIGHT
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • STARK PARTNERS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • RED LOGISTICS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • BLUE CO
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • ZENITH HOLDINGS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • TYRELL INDUSTRIES
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • VERTEX SUPPLY
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 -   • NIMBUS HOLDINGS
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - SHIPMENT TYPE BREAKDOWN
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - --------------------------------------------------
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - FREIGHT: 1,216 shipments, $714,788,073.40 total (avg: $587,819.14)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - EXPRESS: 2,986 shipments, $372,425,222.80 total (avg: $124,723.79)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - 2DAY: 1,860 shipments, $55,645,496.60 total (avg: $29,916.93)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - GROUND: 5,938 shipments, $17,768,279.81 total (avg: $2,992.30)
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:395 - ================================================================================
2025-10-02 18:31:12.111 | INFO     | src.analysis:print_analysis_report:396 - === ANALYSIS REPORT END ===
2025-10-02 18:31:12.111 | INFO     | src.database:disconnect:44 - Database connection closed

…/Projects/Reveel-Takehome ❯ psql -h localhost -U postgres -c "
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
 shipment_type | shipment_count | calculated_cost
---------------+----------------+-----------------
 FREIGHT       |           1216 |    714788073.40
 EXPRESS       |           2986 |    372425222.80
 2DAY          |           1860 |     55645496.60
 GROUND        |           5938 |     17768279.81
(4 rows)
