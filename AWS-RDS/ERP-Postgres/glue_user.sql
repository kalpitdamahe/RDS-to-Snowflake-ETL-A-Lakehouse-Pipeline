CREATE ROLE glue_readaccess;
GRANT CONNECT ON DATABASE erpdb TO glue_readaccess;
GRANT USAGE ON SCHEMA erpsch TO glue_readaccess;
GRANT SELECT ON ALL TABLES IN SCHEMA erpsch TO glue_readaccess;

CREATE USER aws_glue WITH PASSWORD 'password';
GRANT glue_readaccess TO aws_glue;

SET password_encryption = 'md5';
ALTER USER aws_glue WITH PASSWORD 'password123';