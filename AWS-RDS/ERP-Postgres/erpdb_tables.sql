-- Drop the table if it exists, and create the first table under erpsch schema
DROP TABLE IF EXISTS erpsch.erp_loc_a101;

CREATE TABLE erpsch.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

-- Drop the table if it exists, and create the second table under erpsch schema
DROP TABLE IF EXISTS erpsch.erp_cust_az12;

CREATE TABLE erpsch.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

-- Drop the table if it exists, and create the third table under erpsch schema
DROP TABLE IF EXISTS erpsch.erp_px_cat_g1v2;

CREATE TABLE erpsch.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);
