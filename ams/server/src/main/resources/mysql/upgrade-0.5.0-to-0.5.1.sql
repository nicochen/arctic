ALTER TABLE table_optimizing_process ADD COLUMN branch varchar(256) DEFAULT NULL COMMENT 'Branch name';
UPDATE `table_optimizing_process` SET `status`  = 'CLOSED' WHERE `status` ='RUNNING';
UPDATE `table_runtime` SET `optimizing_status`  = 'IDLE';
