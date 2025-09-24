-- ===== 高速批量灌入：全域/持久化設定 =====
-- 需有 SYSTEM_VARIABLES_ADMIN / SUPER 權限

-- 1) 降低持久化成本（⚠️ 有風險，只建議在“批量導入階段”啟用）
-- 交易提交時不每次 fsync redo；掉電可能丟最近幾百毫秒的資料
SET PERSIST innodb_flush_log_at_trx_commit = 2;

-- binlog 不每次 fsync；掉電可能丟最近幾百毫秒的 binlog 事件
SET PERSIST sync_binlog = 0;

-- 2) Binlog 精簡（ROW 模式下減少資料量）
SET PERSIST binlog_format = 'ROW';
SET PERSIST binlog_row_image = 'MINIMAL';

-- 3) Redo（重做日誌） 容量拉大（MySQL 8.0 可動態；若版本不支援會報錯，忽略即可）
--   請依機器調整，以下示例 8G；大量導入時可設 8~16G
SET PERSIST innodb_redo_log_capacity = 8589934592;  -- 8 * 1024^3

-- 4) （選擇）關閉 doublewrite（8.0.20+ 多數情況可動態；若報錯代表需重啟或版本不支援）
--   關閉會降低頁面損壞保護，請只在導入期間短暫使用
-- SET PERSIST innodb_doublewrite = 0;

-- 5) 開啟本地 infile（必須，否則 JDBC 的 LOCAL INFILE 無法用）
SET PERSIST local_infile = 1;

-- 6) 字元集 / 比對規則（全域預設，依你的標準而定）
SET PERSIST character_set_server = 'utf8mb4';
SET PERSIST collation_server     = 'utf8mb4_0900_ai_ci';

-- 7) IO 能力（依 NVMe/磁碟調整；數值僅範例）
SET PERSIST innodb_io_capacity      = 8000;
SET PERSIST innodb_io_capacity_max  = 16000;

-- 8) （可選）臨時關閉自動統計以減少元數據干擾
SET PERSIST innodb_stats_auto_recalc = 0;
