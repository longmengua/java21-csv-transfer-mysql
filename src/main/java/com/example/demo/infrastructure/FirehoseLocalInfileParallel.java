package com.example.demo.infrastructure;

import com.mysql.cj.jdbc.JdbcStatement;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * FirehoseLocalInfileParallel
 *
 * 說明（含實測結果）：
 * - 目的：以多執行緒並行，透過 MySQL Connector/J 的 LOCAL INFILE 串流，把大量 CSV 資料直接灌入 MySQL。
 * - 方法：Producer 執行緒把 CSV 內容寫入 PipedOutputStream，JDBC 透過 setLocalInfileInputStream()
 *         將同一個資料串流交給 LOAD DATA LOCAL INFILE 讀取；每個 worker 一條連線一個 LOAD DATA。
 * - 進度：在 Producer 端做行數/位元組累計，每秒印一次（含 rows/s、平均 MB/s、ETA）。
 * - 建議表結構：分割表（例如 PARTITION BY LINEAR HASH(user_id) PARTITIONS 32），UNIQUE/PK 需包含 user_id。
 *
 * ✅ 實測（你提供的執行結果）：
 *   Worker 07 DONE: rows=12500000, elapsed=259643 ms
 *   Worker 03 DONE: rows=12500000, elapsed=259651 ms
 *   Worker 01 DONE: rows=12500000, elapsed=260037 ms
 *   Worker 06 DONE: rows=12500000, elapsed=260572 ms
 *   Worker 00 DONE: rows=12500000, elapsed=260660 ms
 *   Worker 05 DONE: rows=12500000, elapsed=260843 ms
 *   Worker 02 DONE: rows=12500000, elapsed=261156 ms
 *   Worker 04 DONE: rows=12500000, elapsed=261219 ms
 *   ALL DONE. totalRows=100000000, elapsed=261411 ms (22.95 M rows/min)
 *
 *   總耗時：約 261,411 ms（≈ 4 分 21 秒）
 *   平均吞吐：~ 22.95 百萬列/分鐘（約 382,500 列/秒）
 */
public class FirehoseLocalInfileParallel {

    // ====== 連線 & 目標表 ======
    static final String DB_HOST = "127.0.0.1"; // 使用 127.0.0.1 可避免解析到 IPv6 ::1
    static final int    DB_PORT = 3306;
    static final String DB_NAME = "testdb";
    static final String TABLE   = "user_trade";

    // JDBC 連線字串：啟用 LOCAL INFILE、設定連線/Socket 逾時、關閉 SSL（本機/測試）
    static final String URL =
            "jdbc:mysql://" + DB_HOST + ":" + DB_PORT + "/" + DB_NAME
                    + "?useUnicode=true&characterEncoding=UTF-8"
                    + "&allowLoadLocalInfile=true"
                    + "&connectTimeout=5000"
                    + "&socketTimeout=120000"
                    + "&useSSL=false&allowPublicKeyRetrieval=true";
    static final String USER = "root";
    static final String PASS = "root";

    // ====== 批量導入參數 ======
    static final long TOTAL_ROWS = 100_000_000L; // 總筆數：1e8
    static final int  PAR        = 8;            // 並行度（依硬體可調 16/32）
    static final long ROWS_PER_WORKER = TOTAL_ROWS / PAR; // 每個 worker 的目標筆數

    // ====== 生成參數 ======
    static final DateTimeFormatter TS_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static final int  PIPE_BUF        = 1 << 20; // PipedInputStream 緩衝（1MB）
    static final int  SB_FLUSH_BYTES  = 1 << 20; // StringBuilder 累積到 1MB 就 flush
    static final boolean FIXED_TIMESTAMP_PER_WORKER = true; // 每個 worker 固定一個時間字串（省 CPU）

    // ====== 進度列印參數 ======
    static final long PROGRESS_PRINT_INTERVAL_MS = 1000;     // 每 1 秒印一次
    static final long MIN_FLUSH_ROWS_FOR_PRINT   = 100_000;  // 至少累積這麼多行才印，避免過度頻繁

    public static void main(String[] args) throws Exception {
        long t0 = System.currentTimeMillis();

        // 固定大小的執行緒池 + CompletionService 收集每個 worker 的回傳行數
        ExecutorService pool = Executors.newFixedThreadPool(PAR);
        CompletionService<Long> cs = new ExecutorCompletionService<>(pool);

        // 啟動 PAR 個 worker，每個 worker 執行一次 LOAD DATA（綁定一個串流）
        for (int i = 0; i < PAR; i++) {
            final int idx = i;
            cs.submit(() -> runOneWorker(idx));
        }

        // 等待所有 worker 完成並累加實際寫入行數
        long done = 0;
        for (int i = 0; i < PAR; i++) done += cs.take().get();
        pool.shutdown();

        long t1 = System.currentTimeMillis();
        System.out.printf(
                "ALL DONE. totalRows=%d, elapsed=%d ms (%.2f M rows/min)%n",
                done, (t1 - t0),
                (done / 1_000_000.0) * (60_000.0 / Math.max(1, (t1 - t0)))
        );
    }

    /**
     * 具備指數回退的開連線：避免剛啟動的 MySQL 尚未 Ready 造成連不上。
     */
    static Connection openWithRetry() throws SQLException, InterruptedException {
        int max = 6; long backoff = 1000L; int attempt = 0;
        while (true) {
            try {
                return DriverManager.getConnection(URL, USER, PASS);
            } catch (SQLException e) {
                if (++attempt >= max) throw e;
                System.err.printf("connect failed (%s). retry %d/%d in %d ms%n",
                        e.getMessage(), attempt, max, backoff);
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 30_000L);
            }
        }
    }

    /**
     * 簡單的進度模型：Producer 每次 flush 後累計行數/位元組，定期印出進度。
     */
    static final class Progress {
        final long totalRowsTarget;            // 目標總筆數（該 worker）
        final AtomicLong rows = new AtomicLong();
        final AtomicLong bytes = new AtomicLong();
        volatile long lastPrintTs = System.currentTimeMillis();
        volatile long lastRowsPrinted = 0;
        volatile long startTs = System.currentTimeMillis();

        Progress(long totalRowsTarget) { this.totalRowsTarget = totalRowsTarget; }

        void add(long rowDelta, long byteDelta) {
            rows.addAndGet(rowDelta);
            bytes.addAndGet(byteDelta);
        }

        void maybePrint(int workerId) {
            long now = System.currentTimeMillis();
            long since = now - lastPrintTs;
            long curRows = rows.get();
            long rowsDelta = curRows - lastRowsPrinted;

            // 每秒且至少累積 MIN_FLUSH_ROWS_FOR_PRINT 行才列印，避免過度刷屏
            if (since >= PROGRESS_PRINT_INTERVAL_MS && rowsDelta >= MIN_FLUSH_ROWS_FOR_PRINT) {
                double rps = rowsDelta / (since / 1000.0); // 本次區間行速
                double mbps = (bytes.get() / (1024.0 * 1024.0))
                        / ((now - startTs) / 1000.0); // 平均 MB/s
                long remain = Math.max(0, totalRowsTarget - curRows);
                double etaSec = (rps > 0) ? (remain / rps) : Double.NaN;

                System.out.printf(
                        "Worker %02d PROGRESS: %,d / %,d rows (%.2f%%), speed: %.1f rows/s, avg %.2f MB/s, ETA: %s%n",
                        workerId, curRows, totalRowsTarget,
                        (curRows * 100.0 / totalRowsTarget),
                        rps, mbps,
                        Double.isNaN(etaSec) ? "n/a" : prettyDuration(etaSec)
                );
                lastRowsPrinted = curRows;
                lastPrintTs = now;
            }
        }

        static String prettyDuration(double sec) {
            long s = (long) Math.ceil(sec);
            long h = s / 3600; s %= 3600;
            long m = s / 60;   s %= 60;
            if (h > 0) return String.format("%dh%dm%ds", h, m, s);
            if (m > 0) return String.format("%dm%ds", m, s);
            return String.format("%ds", s);
        }
    }

    /**
     * 執行一個 worker：
     * - 建立 Pipe：Producer 寫入 CSV → JDBC 從 InputStream 讀取
     * - 綁定 InputStream 給 LOAD DATA LOCAL INFILE
     * - 執行 ps.execute()（期間會阻塞到 Server 讀完整個串流）
     */
    static long runOneWorker(int workerId) {
        // 注意：INFILE 的檔名純粹是識別用途（JDBC 實際讀的是我們提供的 InputStream）
        String loadSql = """
            LOAD DATA LOCAL INFILE 'stream_%02d.csv'
            INTO TABLE %s
            FIELDS TERMINATED BY ','
            LINES TERMINATED BY '\\n'
            (user_id, symbol, qty, px, ctime)
            """.formatted(workerId, TABLE);

        Progress prog = new Progress(ROWS_PER_WORKER);

        try (Connection conn = openWithRetry();
             PreparedStatement ps = conn.prepareStatement(loadSql)) {

            // 每條連線的加速參數：關閉唯一/外鍵檢查、延長網路逾時、必要時關閉 binlog
            try (Statement st = conn.createStatement()) {
                st.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
                st.execute("SET SESSION unique_checks=0");
                st.execute("SET SESSION foreign_key_checks=0");
                st.execute("SET SESSION net_write_timeout=600");
                st.execute("SET SESSION net_read_timeout=600");
                try { st.execute("SET SESSION sql_log_bin=0"); } catch (SQLException ignore) {}
            }
            conn.setAutoCommit(false);

            // Pipe：Producer 寫 → MySQL 驅動讀
            PipedOutputStream out = new PipedOutputStream();
            PipedInputStream in = new PipedInputStream(out, PIPE_BUF);

            // Producer 執行緒：串流生成 CSV（不落地檔案）
            Thread producer = new Thread(() -> produceCsvToStream(out, ROWS_PER_WORKER, workerId, prog));
            producer.setName("csv-producer-" + workerId);
            producer.start();

            // 將 InputStream 綁定到 Statement（JDBC 會把它當成 LOCAL INFILE 的來源）
            JdbcStatement mysqlStmt = ps.unwrap(JdbcStatement.class);
            mysqlStmt.setLocalInfileInputStream(in);

            long t0 = System.currentTimeMillis();
            ps.execute();   // 阻塞直到 Server 吃完整個 InputStream
            conn.commit();
            long t1 = System.currentTimeMillis();

            producer.join(); // 等 Producer 正常結束
            in.close();

            long rows = ROWS_PER_WORKER;
            System.out.printf("Worker %02d DONE: rows=%d, elapsed=%d ms%n",
                    workerId, rows, (t1 - t0));
            return rows;

        } catch (Exception e) {
            throw new RuntimeException("worker " + workerId + " failed", e);
        }
    }

    /**
     * 生成 CSV 到輸出串流（每次 flush 更新進度）：
     * - 欄位：user_id,symbol,qty,px,ctime\n
     * - FIXED_TIMESTAMP_PER_WORKER=true 時，該 worker 使用同一個時間字串（省 LocalDateTime.now() 開銷）
     */
    static void produceCsvToStream(OutputStream out, long rows, int seed, Progress prog) {
        try (OutputStreamWriter w = new OutputStreamWriter(out, StandardCharsets.UTF_8);
             BufferedWriter bw = new BufferedWriter(w, PIPE_BUF)) {

            Random r = new Random(seed * 1009L + 7L);
            String ts = FIXED_TIMESTAMP_PER_WORKER
                    ? LocalDateTime.now().plusSeconds(seed).format(TS_FMT) // 每個 worker 偏移幾秒，減少唯一鍵碰撞
                    : null;

            StringBuilder sb = new StringBuilder(SB_FLUSH_BYTES);
            long pendingRows = 0;   // 尚未累計到進度的行數
            long pendingBytes = 0;  // 尚未累計到進度的位元組數

            for (long i = 0; i < rows; i++) {
                long userId = 100_000L + r.nextInt(1_000_000); // 讓 user_id 分布均勻
                String symbol = switch ((int) (i % 3)) {
                    case 0 -> "BTCUSDT";
                    case 1 -> "ETHUSDT";
                    default -> "SOLUSDT";
                };
                String qty  = "1."   + (1000 + (i % 9000));
                String px   = "100." + (1000 + (i % 9000));
                String tss  = (ts != null) ? ts : LocalDateTime.now().format(TS_FMT);

                int before = sb.length();
                sb.append(userId).append(',')
                        .append(symbol).append(',')
                        .append(qty).append(',')
                        .append(px).append(',')
                        .append(tss).append('\n');

                pendingRows++;
                pendingBytes += (sb.length() - before);

                // 累積到 1MB 就寫入 Pipe，並更新進度
                if (sb.length() >= SB_FLUSH_BYTES) {
                    bw.write(sb.toString());
                    bw.flush();                 // 盡快把資料推進 Pipe，避免主執行緒久等
                    prog.add(pendingRows, pendingBytes);
                    prog.maybePrint(seed);      // 視需要印出進度
                    pendingRows = 0;
                    pendingBytes = 0;
                    sb.setLength(0);
                }
            }

            // 尾批次 flush
            if (sb.length() > 0) {
                bw.write(sb.toString());
                bw.flush();
                prog.add(pendingRows, pendingBytes);
                prog.maybePrint(seed);
            }
        } catch (IOException ioe) {
            throw new UncheckedIOException(ioe);
        }
    }
}
