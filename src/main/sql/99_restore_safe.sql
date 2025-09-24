-- ===== 導入完成：恢復安全與通用設定 =====
-- 需有 SYSTEM_VARIABLES_ADMIN / SUPER 權限

-- 1) 恢復交易/複製的嚴格持久化
SET PERSIST innodb_flush_log_at_trx_commit = 1;  -- 每次 COMMIT fsync redo，最安全
SET PERSIST sync_binlog = 1;                     -- 每次寫 binlog 都 fsync，避免主從丟事件

-- 2) binlog 設定（保守起見改回 FULL；若你長期用 MINIMAL 可跳過本行）
SET PERSIST binlog_row_image = 'FULL';

-- 3) 關閉 LOCAL INFILE（若線上不需要，建議關閉以降低風險；需要可保留為 1）
SET PERSIST local_infile = 0;

-- 4) 重新開啟統計自動重算（導入期為降低抖動可能關掉過）
SET PERSIST innodb_stats_auto_recalc = 1;

-- 5) I/O 能力恢復到較保守值（依你的平台調整；以下為常見預設/建議值）
SET PERSIST innodb_io_capacity     = 200;
SET PERSIST innodb_io_capacity_max = 2000;

-- 6) doublewrite（如果導入期曾關過，建議恢復；未改過可忽略）
-- SET PERSIST innodb_doublewrite = 1;

-- 7) Redo 容量（通常保持較大沒壞處；若你想收回到預設~1GB，可依需求調整）
-- SET PERSIST innodb_redo_log_capacity = 1073741824;  -- 1 * 1024^3
