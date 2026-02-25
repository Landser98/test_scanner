-- ============================================================================
-- Unified Bank Statement Database Schema
-- PostgreSQL
-- ============================================================================

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- 1. CLIENTS & ACCOUNTS
-- ============================================================================

CREATE TABLE clients (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    iin_bin VARCHAR(20) NOT NULL UNIQUE,
    full_name VARCHAR(500) NOT NULL,
    client_type VARCHAR(50) NOT NULL, -- 'IP', 'LLC', 'Individual', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE accounts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    client_id UUID NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
    account_number VARCHAR(50) NOT NULL UNIQUE,
    bank VARCHAR(100) NOT NULL,
    currency VARCHAR(10) DEFAULT 'KZT',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- 2. STATEMENTS (Main entity)
-- ============================================================================

CREATE TABLE statements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    bank VARCHAR(100) NOT NULL,
    pdf_name VARCHAR(500),
    period_from DATE,
    period_to DATE,
    statement_generation_date DATE,
    first_operation_date DATE,
    last_operation_date DATE,
    calc_start_date DATE,
    calc_end_date DATE,
    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_statements_account ON statements(account_id);
CREATE INDEX idx_statements_period ON statements(period_from, period_to);
CREATE INDEX idx_statements_uploaded_at ON statements(uploaded_at);

-- ============================================================================
-- 2.1 PROJECTS
-- User-level grouping entity (up to 9 statements per project)
-- ============================================================================

CREATE TABLE projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'draft', -- draft/processing/completed/completed_with_warnings/failed
    created_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE project_statements (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    statement_id UUID REFERENCES statements(id) ON DELETE CASCADE,
    upload_order SMALLINT NOT NULL,
    source_filename VARCHAR(500),
    processing_status VARCHAR(50) NOT NULL, -- success/skipped/error
    processing_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX uq_project_statements_statement ON project_statements(statement_id);
CREATE INDEX idx_project_statements_project ON project_statements(project_id);
CREATE INDEX idx_project_statements_upload_order ON project_statements(project_id, upload_order);

-- ============================================================================
-- 3. STATEMENT HEADER
-- Unified column names for all banks
-- ============================================================================

CREATE TABLE statement_headers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL UNIQUE REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Account info
    account_number VARCHAR(50),
    currency VARCHAR(10),
    account_holder_name VARCHAR(500),
    iin_bin VARCHAR(20),
    
    -- Period info
    period_from DATE,
    period_to DATE,
    
    -- Balance info
    opening_balance DECIMAL(18, 4),
    opening_balance_date DATE,
    opening_balance_equiv_kzt DECIMAL(18, 4),
    
    closing_balance DECIMAL(18, 4),
    closing_balance_date DATE,
    closing_balance_equiv_kzt DECIMAL(18, 4),
    
    -- Turnover info
    debit_turnover DECIMAL(18, 4),
    credit_turnover DECIMAL(18, 4),
    
    -- Raw data (backup)
    raw_header_text TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_statement_headers_statement ON statement_headers(statement_id);

-- ============================================================================
-- 4. TRANSACTIONS (tx)
-- Unified column names for all banks
-- ============================================================================

CREATE TABLE transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Operation date
    operation_date DATE NOT NULL,
    operation_datetime TIMESTAMP,
    value_date DATE,
    
    -- Transaction amounts
    debit_amount DECIMAL(18, 4) DEFAULT 0,
    credit_amount DECIMAL(18, 4) DEFAULT 0,
    
    -- Foreign exchange (if applicable)
    exchange_rate DECIMAL(12, 6) DEFAULT 1.0,
    amount_kzt_equivalent DECIMAL(18, 4),
    
    -- Payment details
    payment_code_knp VARCHAR(10),
    payment_purpose TEXT,
    
    -- Counterparty info
    counterparty_name VARCHAR(500),
    counterparty_iin_bin VARCHAR(20),
    counterparty_account VARCHAR(50),
    counterparty_bank_bic VARCHAR(20),
    
    -- Transaction document
    document_number VARCHAR(50),
    
    -- Bank-specific fields (optional)
    raw_operation_text TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_statement ON transactions(statement_id);
CREATE INDEX idx_transactions_operation_date ON transactions(operation_date);
CREATE INDEX idx_transactions_credit_amount ON transactions(credit_amount) WHERE credit_amount > 0;

-- ============================================================================
-- 5. TRANSACTIONS WITH IP INCOME FLAGS (tx_ip)
-- Transaction enriched with IP income calculation flags
-- ============================================================================

CREATE TABLE transactions_ip_flags (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    transaction_id UUID NOT NULL UNIQUE REFERENCES transactions(id) ON DELETE CASCADE,
    statement_id UUID NOT NULL REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Normalized payment code
    knp_normalized VARCHAR(10),
    
    -- IP Income classification
    is_non_business_by_knp BOOLEAN DEFAULT FALSE,
    is_non_business_by_keywords BOOLEAN DEFAULT FALSE,
    is_non_business BOOLEAN DEFAULT FALSE,
    is_business_income BOOLEAN DEFAULT FALSE,
    
    -- Amount used for IP income calculation
    ip_credit_amount DECIMAL(18, 4) DEFAULT 0,
    
    -- Additional flags
    exclusion_reason VARCHAR(200),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_ip_flags_statement ON transactions_ip_flags(statement_id);
CREATE INDEX idx_transactions_ip_flags_is_business ON transactions_ip_flags(is_business_income);

-- ============================================================================
-- 6. MONTHLY INCOME SUMMARY (ip_income_monthly)
-- Aggregated IP income by month
-- ============================================================================

CREATE TABLE ip_income_monthly (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL REFERENCES statements(id) ON DELETE CASCADE,
    
    month DATE NOT NULL, -- First day of the month
    business_income DECIMAL(18, 4) NOT NULL,
    transaction_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ip_income_monthly_statement ON ip_income_monthly(statement_id);
CREATE INDEX idx_ip_income_monthly_month ON ip_income_monthly(month);

-- ============================================================================
-- 7. INCOME SUMMARY (income_summary)
-- Aggregated IP income statistics for the entire statement
-- ============================================================================

CREATE TABLE income_summaries (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL UNIQUE REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Income calculations
    total_income DECIMAL(18, 4),
    total_income_adjusted DECIMAL(18, 4),
    
    -- Statistics
    total_sum DECIMAL(18, 4),
    max_transaction DECIMAL(18, 4),
    min_transaction DECIMAL(18, 4),
    mean_transaction DECIMAL(18, 4),
    median_transaction DECIMAL(18, 4),
    
    transactions_used INTEGER DEFAULT 0,
    transactions_excluded INTEGER DEFAULT 0,
    
    -- Calculation formula details
    formula VARCHAR(200),
    calculation_notes TEXT,
    
    -- Metadata
    calculation_date TIMESTAMP,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_income_summaries_statement ON income_summaries(statement_id);

-- ============================================================================
-- 8. STATEMENT FOOTER
-- Totals and summary information from the end of statement
-- ============================================================================

CREATE TABLE statement_footers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL UNIQUE REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Total amounts
    total_debit DECIMAL(18, 4),
    total_credit DECIMAL(18, 4),
    
    -- Final balance
    final_balance DECIMAL(18, 4),
    final_balance_date DATE,
    
    -- Raw data (backup)
    raw_footer_text TEXT,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_statement_footers_statement ON statement_footers(statement_id);

-- ============================================================================
-- 9. STATEMENT METADATA
-- Processing and validation metadata for each statement
-- ============================================================================

CREATE TABLE statement_metadata (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    statement_id UUID NOT NULL UNIQUE REFERENCES statements(id) ON DELETE CASCADE,
    
    -- Validation info
    validation_flags VARCHAR(1000), -- semicolon-separated flags
    validation_score DECIMAL(5, 2),
    
    -- Processing info
    processor VARCHAR(50), -- which module processed this
    processing_date TIMESTAMP,
    
    -- Balance validation
    opening_balance DECIMAL(18, 4),
    closing_balance DECIMAL(18, 4),
    rollforward_sum_tx DECIMAL(18, 4),
    balance_matches BOOLEAN,
    
    -- Additional metadata
    debug_info JSONB, -- store complex debug data as JSON
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_statement_metadata_statement ON statement_metadata(statement_id);

-- ============================================================================
-- 10. RELATED PARTIES / COUNTERPARTIES
-- For network analysis and counterparty tracking
-- ============================================================================

CREATE TABLE counterparties (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    
    -- Counterparty identification
    name VARCHAR(500) NOT NULL,
    iin_bin VARCHAR(20),
    account_number VARCHAR(50),
    bank_bic VARCHAR(20),
    
    -- Transaction statistics
    transaction_count INTEGER DEFAULT 0,
    total_sent DECIMAL(18, 4) DEFAULT 0,
    total_received DECIMAL(18, 4) DEFAULT 0,
    
    -- Classification
    counterparty_type VARCHAR(50), -- 'supplier', 'customer', 'employee', etc.
    is_related_party BOOLEAN DEFAULT FALSE,
    
    -- Time tracking
    first_transaction_date DATE,
    last_transaction_date DATE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_counterparties_account ON counterparties(account_id);
CREATE INDEX idx_counterparties_name ON counterparties(name);
CREATE INDEX idx_counterparties_iin_bin ON counterparties(iin_bin);

-- ============================================================================
-- 11. RELATED PARTIES NETWORK
-- Links between accounts showing money flows
-- ============================================================================

CREATE TABLE related_party_transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    counterparty_id UUID NOT NULL REFERENCES counterparties(id) ON DELETE CASCADE,
    transaction_id UUID NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
    
    direction VARCHAR(20), -- 'incoming' or 'outgoing'
    amount DECIMAL(18, 4),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_related_party_transactions_counterparty ON related_party_transactions(counterparty_id);
CREATE INDEX idx_related_party_transactions_transaction ON related_party_transactions(transaction_id);

-- ============================================================================
-- VIEWS for easier querying
-- ============================================================================

-- View: Full statement with all details
CREATE VIEW v_full_statements AS
SELECT 
    s.id as statement_id,
    c.iin_bin,
    c.full_name,
    a.account_number,
    s.bank,
    s.pdf_name,
    s.period_from,
    s.period_to,
    sh.opening_balance,
    sh.closing_balance,
    sh.debit_turnover,
    sh.credit_turnover,
    COUNT(DISTINCT t.id) as transaction_count
FROM statements s
JOIN accounts a ON s.account_id = a.id
JOIN clients c ON a.client_id = c.id
LEFT JOIN statement_headers sh ON s.id = sh.statement_id
LEFT JOIN transactions t ON s.id = t.statement_id
GROUP BY s.id, c.iin_bin, c.full_name, a.account_number, s.bank, s.pdf_name, s.period_from, s.period_to, sh.opening_balance, sh.closing_balance, sh.debit_turnover, sh.credit_turnover;

-- View: IP Income summary by account
CREATE VIEW v_ip_income_by_account AS
SELECT 
    a.id as account_id,
    a.account_number,
    c.full_name,
    s.bank,
    ism.total_income,
    ism.total_income_adjusted,
    ism.mean_transaction,
    ism.transactions_used,
    s.period_from,
    s.period_to
FROM income_summaries ism
JOIN statements s ON ism.statement_id = s.id
JOIN accounts a ON s.account_id = a.id
JOIN clients c ON a.client_id = c.id
ORDER BY s.period_to DESC, a.account_number;

-- View: Transaction details with IP flags
CREATE VIEW v_transactions_with_flags AS
SELECT 
    t.id,
    t.statement_id,
    s.bank,
    a.account_number,
    c.full_name,
    t.operation_date,
    t.credit_amount,
    t.debit_amount,
    t.payment_purpose,
    t.counterparty_name,
    tif.is_business_income,
    tif.is_non_business,
    tif.ip_credit_amount
FROM transactions t
JOIN statements s ON t.statement_id = s.id
JOIN accounts a ON s.account_id = a.id
JOIN clients c ON a.client_id = c.id
LEFT JOIN transactions_ip_flags tif ON t.id = tif.transaction_id
ORDER BY t.operation_date DESC;

-- ============================================================================
-- TRIGGERS for auto-updated timestamps
-- ============================================================================

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_clients_update BEFORE UPDATE ON clients FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_accounts_update BEFORE UPDATE ON accounts FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_statements_update BEFORE UPDATE ON statements FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_projects_update BEFORE UPDATE ON projects FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_statement_headers_update BEFORE UPDATE ON statement_headers FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_transactions_update BEFORE UPDATE ON transactions FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_transactions_ip_flags_update BEFORE UPDATE ON transactions_ip_flags FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_income_summaries_update BEFORE UPDATE ON income_summaries FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_statement_footers_update BEFORE UPDATE ON statement_footers FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_statement_metadata_update BEFORE UPDATE ON statement_metadata FOR EACH ROW EXECUTE FUNCTION update_timestamp();
CREATE TRIGGER trigger_counterparties_update BEFORE UPDATE ON counterparties FOR EACH ROW EXECUTE FUNCTION update_timestamp();
