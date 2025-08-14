\copy erpsch.erp_loc_a101 (cid, cntry) FROM '/home/ec2-user/AWS_RDS/datasets/source_erp/LOC_A101.csv' DELIMITER ',' CSV HEADER;


\copy erpsch.erp_cust_az12 (cid, bdate, gen) FROM '/home/ec2-user/AWS_RDS/datasets/source_erp/CUST_AZ12.csv' DELIMITER ',' CSV HEADER;


\copy erpsch.erp_px_cat_g1v2 (id, cat, subcat, maintenance) FROM '/home/ec2-user/AWS_RDS/datasets/source_erp/PX_CAT_G1V2.csv' DELIMITER ',' CSV HEADER;
