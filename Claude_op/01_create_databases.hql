-- 01_create_databases.hql
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE DATABASE IF NOT EXISTS curated COMMENT 'Cleansed core banking data' LOCATION '/data/curated';
CREATE DATABASE IF NOT EXISTS risk    COMMENT 'Risk and AML outputs'       LOCATION '/data/risk';
CREATE DATABASE IF NOT EXISTS ref     COMMENT 'Reference data'             LOCATION '/data/ref_db';
