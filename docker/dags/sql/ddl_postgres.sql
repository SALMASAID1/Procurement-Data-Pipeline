-- Schema for Master Data (PostgreSQL)
-- Master data tables for Products and Suppliers

-- Create schema
CREATE SCHEMA IF NOT EXISTS master_data;
 
-- PRODUCTS TABLE
CREATE TABLE IF NOT EXISTS master_data.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255) NOT NULL,
    product_code VARCHAR(50) UNIQUE NOT NULL,
    category VARCHAR(100),
    unit_price DECIMAL(10,2) NOT NULL,
    safety_stock_level INT DEFAULT 0,
    min_order_quantity INT DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- SUPPLIERS TABLE
CREATE TABLE IF NOT EXISTS master_data.suppliers (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(255) NOT NULL,
    supplier_code VARCHAR(50) UNIQUE NOT NULL,
    contact_email VARCHAR(255),
    contact_phone VARCHAR(50),
    lead_time_days INT DEFAULT 7,
    reliability_score DECIMAL(3,2) DEFAULT 0.95,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PRODUCT-SUPPLIER MAPPING
CREATE TABLE IF NOT EXISTS master_data.product_suppliers (
    product_id INT REFERENCES master_data.products(product_id),
    supplier_id INT REFERENCES master_data.suppliers(supplier_id),
    unit_cost DECIMAL(10,2),
    priority INT DEFAULT 1,
    is_preferred BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (product_id, supplier_id)
);

-- INDEXES FOR PERFORMANCE
CREATE INDEX IF NOT EXISTS idx_products_category ON master_data.products(category);
CREATE INDEX IF NOT EXISTS idx_products_active ON master_data.products(is_active);
CREATE INDEX IF NOT EXISTS idx_suppliers_active ON master_data.suppliers(is_active);
CREATE INDEX IF NOT EXISTS idx_ps_priority ON master_data.product_suppliers(priority);

-- SAMPLE DATA
INSERT INTO master_data.products 
    (product_id, product_name, product_code, category, unit_price, safety_stock_level)
VALUES 
    (1, 'Widget A', 'WGT-001', 'Electronics', 29.99, 100),
    (2, 'Widget B', 'WGT-002', 'Electronics', 49.99, 50),
    (3, 'Component X', 'CMP-001', 'Hardware', 15.00, 200),
    (4, 'Component Y', 'CMP-002', 'Hardware', 22.50, 150),
    (5, 'Assembly Kit', 'ASM-001', 'Kits', 89.99, 30)
ON CONFLICT (product_code) DO NOTHING;

INSERT INTO master_data.suppliers 
    (supplier_id, supplier_name, supplier_code, contact_email, lead_time_days, reliability_score)
VALUES
    (1, 'TechSupply Co', 'SUP-001', 'orders@techsupply.com', 5, 0.98),
    (2, 'Global Parts Inc', 'SUP-002', 'sales@globalparts.com', 7, 0.95),
    (3, 'FastShip Ltd', 'SUP-003', 'procurement@fastship.com', 3, 0.92)
ON CONFLICT (supplier_code) DO NOTHING;

INSERT INTO master_data.product_suppliers 
    (product_id, supplier_id, unit_cost, priority, is_preferred)
VALUES
    (1, 1, 20.00, 1, TRUE),
    (1, 2, 22.00, 2, FALSE),
    (2, 1, 35.00, 1, TRUE),
    (2, 3, 38.00, 2, FALSE),
    (3, 2, 10.00, 1, TRUE),
    (3, 3, 11.00, 2, FALSE),
    (4, 2, 15.00, 1, TRUE),
    (4, 1, 16.00, 2, FALSE),
    (5, 3, 60.00, 1, TRUE),
    (5, 1, 65.00, 2, FALSE)
ON CONFLICT (product_id, supplier_id) DO NOTHING;

-- Reset sequences to avoid conflicts
SELECT setval('master_data.products_product_id_seq', (SELECT MAX(product_id) FROM master_data.products));
SELECT setval('master_data.suppliers_supplier_id_seq', (SELECT MAX(supplier_id) FROM master_data.suppliers));
