CREATE TABLE IF NOT EXISTS `testdb`.`user_trade` (
  id       BIGINT NOT NULL AUTO_INCREMENT,
  event_id BIGINT NOT NULL,
  user_id  BIGINT NOT NULL,
  symbol   VARCHAR(20) NOT NULL,
  qty      DECIMAL(20,8) NOT NULL,
  px       DECIMAL(20,8) NOT NULL,
  ctime    TIMESTAMP NOT NULL,
  -- AUTO_INCREMENT 要在 PK 最左欄，分割表 PK 必須含 partition key (user_id)
  PRIMARY KEY (id, user_id)
)
ENGINE=InnoDB
DEFAULT CHARSET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci
PARTITION BY LINEAR HASH(user_id)
PARTITIONS 32;

-- 匯入完成後可加的索引

-- 形態 A：有 qty 條件時最合適
CREATE INDEX idx_user_qty_ctime_desc
ON testdb.user_trade (user_id, qty, ctime DESC);

-- 形態 B：沒有 qty 條件、只取最新 N 筆時最合適
CREATE INDEX idx_user_ctime_desc
ON testdb.user_trade (user_id, ctime DESC);

-- OL環境避免鎖表，先建立為 INVISIBLE，建好後再切 VISIBLE，避免在建立過程中被誤用

-- 形態 A：有 qty 條件時最合適
ALTER TABLE testdb.user_trade
  ADD INDEX idx_user_qty_ctime_desc   (user_id, qty, ctime DESC) INVISIBLE,
  ALGORITHM=INPLACE, LOCK=NONE;

-- 形態 B：沒有 qty 條件、只取最新 N 筆時最合適
ALTER TABLE testdb.user_trade
  ADD INDEX idx_user_ctime_desc       (user_id, ctime DESC)      INVISIBLE,
  ALGORITHM=INPLACE, LOCK=NONE;

-- 建完後更新統計資訊（可選，但建議）
ANALYZE TABLE testdb.user_trade;

-- 分批啟用索引（觀察查詢計劃與延遲）
ALTER TABLE testdb.user_trade ALTER INDEX idx_user_qty_ctime_desc VISIBLE;
-- 視情況再開啟第二支
ALTER TABLE testdb.user_trade ALTER INDEX idx_user_ctime_desc     VISIBLE;
