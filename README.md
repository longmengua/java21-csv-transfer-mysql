# java21-csv-transfer-mysql

## 📖 專案簡介
本專案示範如何使用 **Java 21 + MySQL Connector/J**，透過 **LOAD DATA LOCAL INFILE 串流** 的方式，將大量 CSV 格式資料高效導入 MySQL。  
設計上利用 **多執行緒並行**，Producer 動態產生資料，直接推送給 MySQL Server，不需要中間檔案落地。

## ✨ 功能特色
- 🔄 **多執行緒並行匯入**：每個 worker 一條 JDBC 連線 + 一個 `LOAD DATA LOCAL INFILE`。
- 📝 **動態資料產生**：透過 Producer Thread 即時生成 CSV，避免磁碟 IO。
- 📊 **進度監控**：每秒輸出 rows/s、MB/s、ETA 預估完成時間。
- ⚡ **高效能導入**：單機可達數千萬行/分鐘的吞吐量（取決於硬體與 MySQL 調整）。

## 📂 專案結構
- `src/main/java/com/example/demo/infrastructure/FirehoseLocalInfileParallel.java`
- `README.md`

## 🛠 環境需求
- JDK **21**+
- MySQL **8.0+**
- Maven **3.9+**
- 充足 CPU / RAM（建議 8C/16G 以上）
- MySQL 需開啟：
  ```ini
  [mysqld]
  local_infile=1
