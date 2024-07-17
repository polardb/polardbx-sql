package com.alibaba.polardbx.transfer.utils;

import com.alibaba.polardbx.transfer.Runner;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author wuzhe
 */
public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    // '${user_name}:${password}@tcp(${ip}:${port})/${db_name}'
    private static final String PATTERN = "(.*):(.*)@tcp\\((.*):(\\d+)\\)/(.*)";

    public static Connection getConnection(String dsn) throws SQLException {
        Pattern regex = Pattern.compile(PATTERN);
        Matcher matcher = regex.matcher(dsn);
        if (matcher.find()) {
            String username = matcher.group(1);
            String password = matcher.group(2);
            String host = matcher.group(3);
            String port = matcher.group(4);
            String dbname = matcher.group(5);
            String url = "jdbc:mysql://" + host + ":" + port + "/" + dbname;
            return DriverManager.getConnection(url, username, password);
        }
        throw new RuntimeException("Invalid dsn format, " +
            "should be like '${user_name}:${password}@tcp(${ip}:${port})/${db_name}'");
    }

    public static void prepare() throws Exception {
        Toml config = TomlConfig.getConfig();

        String testType = config.getString("test_type", "transfer-test");

        if ("all-types-test".equalsIgnoreCase(testType)) {
            prepareAllTypesTest(config);
        } else {
            prepareTransferTest(config);
        }
    }

    private static void prepareTransferTest(Toml config) throws SQLException {
        String dsn = config.getString("dsn");
        int rowCount = Math.toIntExact(config.getLong("row_count"));
        long initBalance = config.getLong("initial_balance");
        assert dsn != null;
        assert rowCount > 0;
        assert initBalance > 0;

        String suffix = config.getString("create_table_suffix", "");
        try (Connection conn = getConnection(dsn);
            Statement stmt = conn.createStatement()) {
            // create table
            String createSql = "CREATE TABLE IF NOT EXISTS accounts ("
                + "id INT PRIMARY KEY, "
                + "balance INT NOT NULL, "
                + "version INT NOT NULL DEFAULT '0', "
                + "gmt_modified TIMESTAMP DEFAULT NOW() ON UPDATE NOW()"
                + ")"
                + suffix;

            stmt.execute(createSql);

            // insert all data in one trx to make flashback query work
            stmt.execute("begin");

            try {
                int i = 0;
                while (i < rowCount) {
                    // insert data
                    StringBuilder insertSql = new StringBuilder("INSERT INTO accounts (id, balance) VALUES ");
                    // batch insert with batch size 1000
                    int j = 0;
                    while (j < 1000) {
                        if (i >= rowCount) {
                            break;
                        }
                        insertSql.append(String.format("(%s, %s),", i, initBalance));
                        i++;
                        j++;
                    }
                    // insert at least 1 value
                    if (j > 0) {
                        // delete the last ','
                        insertSql.deleteCharAt(insertSql.length() - 1);
                        stmt.execute(insertSql.toString());
                    }
                }
                stmt.execute("commit");
            } catch (Throwable t) {
                logger.error("prepare data failed", t);
                stmt.execute("rollback");
            }
        }
    }

    private static void prepareAllTypesTest(Toml config) throws Exception {
        String dsn = config.getString("dsn");
        int rowCount = Math.toIntExact(config.getLong("row_count"));
        long threads = config.getLong("threads", 1L);
        boolean bigColumn = config.getBoolean("big_column", false);
        final List<String> allColumns = new ArrayList<>(AllTypesTestUtils.getColumns());
        if (bigColumn) {
            allColumns.addAll(AllTypesTestUtils.getBigColumns());
        }

        String suffix = config.getString("create_table_suffix", "");
        try (Connection conn = getConnection(dsn);
            Statement stmt = conn.createStatement()) {
            // Create table.
            String createSql = AllTypesTestUtils.FULL_TYPE_TABLE_TEMPLATE + suffix;
            stmt.execute(createSql);
            String sql = AllTypesTestUtils.buildAllInsertSql();
            stmt.execute("set sql_mode=''");
            stmt.execute(sql);
        }

        final long rowsEachThread = rowCount / threads;
        final long rowsLastThread = rowsEachThread + (rowCount % threads);
        final int batch = 1;
        final long threadId = Thread.currentThread().getId();
        Callable<Boolean> batchInsert = () -> {
            long rows = Thread.currentThread().getId() == threadId ? rowsLastThread : rowsEachThread;
            String sql = null;
            try (Connection conn = getConnection(dsn);
                Statement stmt = conn.createStatement()) {
                stmt.execute("set sql_mode=''");
                while (rows > 0) {
                    int currentBatch = (int) Math.min(batch, rows);
                    sql = AllTypesTestUtils.buildInsertSql(currentBatch, allColumns);
                    stmt.execute(sql);
                    rows -= currentBatch;
                }
            } catch (Throwable t) {
                logger.error("Prepare data failed, sql: " + sql, t);
                return false;
            }
            return true;
        };

        ExecutorService threadPool = Executors.newFixedThreadPool((int) (threads - 1));
        List<Future<Boolean>> futures = new ArrayList<>();
        for (int i = 0; i < threads - 1; i++) {
            futures.add(threadPool.submit(batchInsert));
        }
        threadPool.shutdown();

        // Last batch is executed in the current thread.
        boolean success = batchInsert.call();
        for (Future<Boolean> future : futures) {
            success = success && future.get();
        }

        if (!success) {
            throw new RuntimeException("Prepare data failed.");
        }
    }

    public static List<String> getClassName(String packagePath, ClassLoader classLoader) {
        List<String> classNames = new ArrayList<>();
        String packageDirPath = packagePath.replace(".", "/");
        java.net.URL url = classLoader.getResource(packageDirPath);
        if (url != null) {
            String filePath = url.getFile();
            File dir = new File(filePath);
            File[] files = dir.listFiles();
            if (files != null) {
                for (File file : files) {
                    String fileName = file.getName();
                    if (file.isFile() && fileName.endsWith(".class")) {
                        String className = fileName.substring(0, fileName.lastIndexOf(".class"));
                        classNames.add(packagePath + "." + className);
                    }
                }
            }
        }
        return classNames;
    }

    public static List<String> getClassNameFromJar(String packagePath) {
        List<String> classNames = new ArrayList<>();
        try {
            String path = Runner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
            JarFile jarFile = new JarFile(path);
            Enumeration<JarEntry> entries = jarFile.entries();
            while (entries.hasMoreElements()) {
                JarEntry entry = entries.nextElement();
                String className = entry.getName().replace('/', '.');
                if (className.startsWith(packagePath) && className.endsWith(".class")) {
                    classNames.add(className.substring(0, className.lastIndexOf(".class")));
                }
            }
        } catch (Throwable t) {
            logger.warn(t.getMessage());
        }
        return classNames;
    }

    public static void initSessionHint(Statement stmt, List<String> sessionHint) throws SQLException {
        Map<String, Integer> groupCnt = new HashMap<>();
        ResultSet rs = stmt.executeQuery("show topology from accounts");
        while (rs.next()) {
            String partitionName = rs.getString("PARTITION_NAME");
            if (null != partitionName) {
                sessionHint.add(partitionName);
            } else {
                String groupName = rs.getString("GROUP_NAME");
                Integer cnt = groupCnt.get(groupName);
                if (null == cnt) {
                    groupCnt.put(groupName, 0);
                } else {
                    groupCnt.put(groupName, cnt + 1);
                }
            }
        }
        if (!groupCnt.isEmpty()) {
            // drds mode
            for (Map.Entry<String, Integer> entry : groupCnt.entrySet()) {
                String groupName = entry.getKey();
                int cnt = entry.getValue();
                for (int i = 0; i < cnt; i++) {
                    sessionHint.add(groupName + ":" + i);
                }
            }
        }
    }

    public static List<Account> getAccountsWithSessionHint(String hint, Statement stmt, List<String> sessionHint)
        throws SQLException {
        SecureRandom random = new SecureRandom();
        List<Account> accounts = new ArrayList<>();
        for (String s : sessionHint) {
            stmt.execute("SET PARTITION_HINT = '" + s + "'");
            // read all accounts in this partition
            ResultSet rs;
            if (random.nextBoolean()) {
                // check secondary index
                rs = stmt.executeQuery(hint + "select id, balance, version from accounts order by balance");
            } else {
                rs = stmt.executeQuery(hint + "select id, balance, version from accounts");
            }
            while (rs.next()) {
                accounts.add(new Account(rs.getInt(1), rs.getLong(2), rs.getLong(3)));
            }
        }
        return accounts;
    }

    public static List<Account> getAccounts(String hint, Statement stmt)
        throws SQLException {
        SecureRandom random = new SecureRandom();
        List<Account> accounts = new ArrayList<>();
        // read all accounts
        ResultSet rs;
        if (random.nextBoolean()) {
            // check secondary index
            rs = stmt.executeQuery(hint + "select id, balance, version from accounts order by balance");
        } else {
            rs = stmt.executeQuery(hint + "select id, balance, version from accounts");
        }
        while (rs.next()) {
            accounts.add(new Account(rs.getInt(1), rs.getLong(2), rs.getLong(3)));
        }
        return accounts;
    }

    public static List<Account> getAccountsWithFlashbackQuery(String hint, Statement stmt, int min, int max)
        throws SQLException {
        SecureRandom random = new SecureRandom();
        List<Account> accounts = new ArrayList<>();
        // flashback to x seconds
        long currentTime = new Date().getTime();
        int x = random.nextInt(max - min) + min;
        long flashbackTime = currentTime - (x * 1000L);
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(flashbackTime));
        String flashbackTimestamp = String.format("as of timestamp '%s'", time);
        ResultSet rs;
        if (random.nextBoolean()) {
            // check secondary index
            rs = stmt.executeQuery(
                hint + "select id, balance, version from accounts " + flashbackTimestamp + " order by balance");
        } else {
            rs = stmt.executeQuery(hint + "select id, balance, version from accounts " + flashbackTimestamp);
        }
        while (rs.next()) {
            accounts.add(new Account(rs.getInt(1), rs.getLong(2), rs.getLong(3)));
        }
        return accounts;
    }

    public static void findIncorrectColumns(Toml config) throws Exception {
        String dsn = config.getString("dsn");
        try (Connection conn = getConnection(dsn);
            Statement stmt = conn.createStatement()) {
            Collection<String> columns = AllTypesTestUtils.getColumns();
            String sql = "select check_sum_v2(%s) from full_type";
            String hint =
                "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_MPP=true ENABLE_MASTER_MPP=true ENABLE_COLUMNAR_OPTIMIZER=true OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true */";
            for (String column : columns) {
                String finalSql = String.format(sql, column);
                // Row store.
                ResultSet rs = stmt.executeQuery(finalSql);
                rs.next();
                long rowChecksum = rs.getLong(1);

                // Columnar store.
                rs = stmt.executeQuery(hint + finalSql);
                rs.next();
                long columnarChecksum = rs.getLong(1);

                if (rowChecksum != columnarChecksum) {
                    System.out.println(column);
                }
            }
        }
    }
}
