SELECT '>> Inserting Data Into: crmdb.crm_cust_info' AS msg;
    LOAD DATA LOCAL INFILE '/home/ec2-user/AWS_RDS/datasets/source_crm/cust_info.csv'
    INTO TABLE crmdb.crm_cust_info
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES;

    SELECT '>> Inserting Data Into: crmdb.crm_prd_info' AS msg;
    LOAD DATA LOCAL INFILE '/home/ec2-user/AWS_RDS/datasets/source_crm/prd_info.csv'
    INTO TABLE crmdb.crm_prd_info
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES;

    SELECT '>> Inserting Data Into: crmdb.crm_sales_details' AS msg;
    LOAD DATA LOCAL INFILE '/home/ec2-user/AWS_RDS/datasets/source_crm/sales_details.csv'
    INTO TABLE crmdb.crm_sales_details
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n' 
    IGNORE 1 LINES;