-- Drop the table if it exists in the 'crmdb' database
SELECT 'Drop the crm_cust_info table if it exists in the crmdb database' as Info;

DROP TABLE IF EXISTS crmdb.crm_cust_info;

-- Create the table in the 'crmdb' database
CREATE TABLE crmdb.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);


-- Drop the table if it exists in the 'crmdb' database
SELECT 'Drop the crm_prd_info table if it exists in the crmdb database';

DROP TABLE IF EXISTS crmdb.crm_prd_info;

-- Create the table in the 'crmdb' database
CREATE TABLE crmdb.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);


-- Drop the table if it exists in the 'crmdb' database
SELECT 'Drop the crm_sales_details table if it exists in the crmdb database';

DROP TABLE IF EXISTS crmdb.crm_sales_details;

-- Create the table in the 'crmdb' database
CREATE TABLE crmdb.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);
