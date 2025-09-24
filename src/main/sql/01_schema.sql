-- 建立資料表
CREATE TABLE IF NOT EXISTS `testdb`.`user_trade` (
  id BIGINT NOT NULL AUTO_INCREMENT,
  event_id BIGINT NOT NULL,              -- 來自撮合流水（全域遞增或(源,seq)映射一維）
  user_id BIGINT NOT NULL,
  symbol VARCHAR(20) NOT NULL,
  qty DECIMAL(20,8) NOT NULL,
  px  DECIMAL(20,8) NOT NULL,
  ctime TIMESTAMP NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uk_event (event_id),
  KEY idx_user_ctime (user_id, ctime)
) ENGINE=InnoDB
  DEFAULT CHARSET = utf8mb4
  COLLATE = utf8mb4_0900_ai_ci;