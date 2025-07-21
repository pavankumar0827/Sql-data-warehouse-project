/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY ci.cst_id) customer_key,
		ci.cst_id customer_id,
		ci.cst_key customer_number,
		ci.cst_firstname firstname,
		ci.cst_lastname lastname,
		cl.cntry country,
		ci.cst_martial_status martial_status,
		CASE
			WHEN cst_gndr = 'n\a' AND gen IS NOT NULL AND gen != 'n/a' THEN gen
			ELSE cst_gndr
		END gender,
		ca.bdate birthdate,
		ci.cst_create_date create_date
	FROM silver.crm_cust_info ci
	LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_id = ca.cid
	LEFT JOIN silver.erp_loc_a101 cl ON ci.cst_id = cl.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt,prd_key) product_key,
	pi.prd_id product_id,
	pi.prd_key product_number,
	pi.prd_nm product_name,
	pi.cat_id category_id,
	pc.cat category,
	pc.subcat sub_category,
	pc.maintenanace,
	pi.prd_cost cost,
	pi.prd_line product_line,
	pi.prd_start_dt start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 pc ON pi.cat_id = pc.id
where pi.prd_end_dt IS NULL -- Filter out all historical data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
	sd.sls_order_num order_number,
	product_key,
	customer_key,
	sd.sls_order_dt order_date,
	sd.sls_ship_dt ship_date,
	sd.sls_due_dt due_date,
	sd.sls_sales sales,
	sd.sls_quantity quantity,
	sd.sls_price price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers c ON sd.sls_cust_id = c.customer_id
LEFT JOIN gold.dim_products p ON sd.sls_prd_key = p.product_number
