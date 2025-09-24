SET PERSIST innodb_flush_log_at_trx_commit = 1;
SET PERSIST sync_binlog = 1;
-- 視需求把 innodb_doublewrite 設回 1
-- 其他參數依你的長期標準收斂
