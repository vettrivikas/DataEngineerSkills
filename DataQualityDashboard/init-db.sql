-- Initial database setup for Data Quality Dashboard
-- This script creates sample data for testing and demonstration

-- Create test schema
CREATE SCHEMA IF NOT EXISTS test_schema;

-- Create customers table with regulatory data elements
CREATE TABLE IF NOT EXISTS test_schema.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    credit_score INTEGER,
    transaction_amount DECIMAL(10,2),
    regulatory_flag BOOLEAN,
    account_status VARCHAR(20),
    created_date DATE DEFAULT CURRENT_DATE,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    currency_code VARCHAR(3),
    account_balance DECIMAL(12,2),
    age INTEGER,
    interest_rate DECIMAL(5,2)
);

-- Insert sample data for testing
INSERT INTO test_schema.customers (
    name, credit_score, transaction_amount, regulatory_flag, 
    account_status, created_date, currency_code, account_balance, age, interest_rate
) VALUES 
('John Smith', 750, 1500.00, true, 'active', '2024-06-01', 'CAD', 25000.00, 35, 3.5),
('Jane Doe', 680, 2300.50, false, 'pending', '2024-06-15', 'USD', 18500.75, 42, 4.1),
('Bob Johnson', 820, 950.75, true, 'active', '2024-06-20', 'CAD', 45000.00, 28, 2.9),
('Alice Brown', 590, 4200.00, true, 'review', '2024-05-30', 'EUR', 12000.25, 38, 5.2),
('Charlie Davis', 720, 800.25, false, 'active', '2024-06-25', 'CAD', 32000.00, 31, 3.8),
('Emma Wilson', 760, 1800.00, true, 'active', '2024-06-10', 'CAD', 28000.50, 45, 3.2),
('David Lee', NULL, 2500.00, false, 'pending', '2024-06-05', 'USD', NULL, 29, 4.5),
('Sarah Chen', 695, NULL, true, 'active', '2024-06-18', 'CAD', 22000.00, NULL, 3.7),
('Michael Brown', 810, 1200.00, true, 'active', '2024-06-22', 'GBP', 38000.00, 33, 2.8),
('Lisa Garcia', 650, 3200.00, false, 'review', '2024-05-28', 'CAD', 15000.00, 40, 4.8)
ON CONFLICT DO NOTHING;

-- Create transactions table for timeliness testing
CREATE TABLE IF NOT EXISTS test_schema.transactions (
    transaction_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES test_schema.customers(customer_id),
    transaction_date DATE,
    amount DECIMAL(10,2),
    transaction_type VARCHAR(50),
    currency_code VARCHAR(3),
    status VARCHAR(20)
);

-- Insert recent and old transactions for timeliness metrics
INSERT INTO test_schema.transactions (
    customer_id, transaction_date, amount, transaction_type, currency_code, status
) VALUES 
(1, CURRENT_DATE - INTERVAL '5 days', 500.00, 'deposit', 'CAD', 'completed'),
(2, CURRENT_DATE - INTERVAL '10 days', 1200.00, 'transfer', 'USD', 'completed'),
(3, CURRENT_DATE - INTERVAL '2 days', 300.00, 'withdrawal', 'CAD', 'completed'),
(4, CURRENT_DATE - INTERVAL '45 days', 800.00, 'deposit', 'EUR', 'completed'),
(5, CURRENT_DATE - INTERVAL '1 day', 150.00, 'payment', 'CAD', 'pending'),
(1, CURRENT_DATE - INTERVAL '60 days', 2000.00, 'transfer', 'CAD', 'completed'),
(2, CURRENT_DATE - INTERVAL '3 days', 750.00, 'deposit', 'USD', 'completed'),
(3, CURRENT_DATE - INTERVAL '15 days', 1500.00, 'transfer', 'CAD', 'completed')
ON CONFLICT DO NOTHING;

-- Create a table with data quality issues for testing
CREATE TABLE IF NOT EXISTS test_schema.data_issues (
    id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    duplicate_customer_id INTEGER, -- For uniqueness testing
    invalid_credit_score INTEGER,  -- Values outside 300-850 range
    invalid_currency VARCHAR(10),  -- Invalid currency codes
    missing_required_field VARCHAR(100),
    created_date DATE
);

-- Insert problematic data for testing data quality metrics
INSERT INTO test_schema.data_issues (
    customer_id, duplicate_customer_id, invalid_credit_score, 
    invalid_currency, missing_required_field, created_date
) VALUES 
(1, 1, 250, 'INVALID', NULL, '2024-06-01'),  -- Below credit score range, invalid currency, null field
(2, 1, 950, 'XYZ', 'Valid Field', '2024-06-02'),  -- Above credit score range, invalid currency, duplicate customer_id
(3, 3, 800, 'CAD', 'Another Field', '2024-06-03'),  -- Valid credit score, duplicate customer_id
(4, 2, NULL, 'USD', 'Field Value', '2024-06-04'),  -- Null credit score
(5, 4, 1200, 'ABC', NULL, '2024-06-05')  -- Way above credit score range, invalid currency, null field
ON CONFLICT DO NOTHING;

-- Grant permissions (if needed)
-- GRANT SELECT ON ALL TABLES IN SCHEMA test_schema TO your_user;