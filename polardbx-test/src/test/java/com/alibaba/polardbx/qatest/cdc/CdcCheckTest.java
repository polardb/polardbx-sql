/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.cdc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.cdc.MetaBuilder;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.HttpClientHelper;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.constant.ConfigConstant.URL_PATTERN_WITH_DB;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.configProp;
import static com.alibaba.polardbx.qatest.util.PropertiesUtil.getConnectionProperties;

/**
 * created by ziyang.lb
 **/
@Slf4j
public class CdcCheckTest extends BaseTestCase {

    protected DruidDataSource srcDs;
    protected DruidDataSource dstDs;

    @Before
    public void before() {
        String polardbxUser = configProp.getProperty(ConfigConstant.POLARDBX_USER);
        String polardbxPassword = configProp.getProperty(ConfigConstant.POLARDBX_PASSWORD);
        String polardbxPort = configProp.getProperty(ConfigConstant.POLARDBX_PORT);
        String polardbxAddress = configProp.getProperty(ConfigConstant.POLARDBX_ADDRESS);
        srcDs = JdbcUtil.getDruidDataSource(String.format(URL_PATTERN_WITH_DB + getConnectionProperties(),
                polardbxAddress, polardbxPort, PropertiesUtil.polardbXDBName1(false)), polardbxUser,
            polardbxPassword);

        String cdcSyncDbAddr = configProp.getProperty("cdcSyncDbAddr");
        String ip = cdcSyncDbAddr.split(":")[0];
        String port = cdcSyncDbAddr.split(":")[1];
        dstDs = JdbcUtil.getDruidDataSource(String.format(URL_PATTERN_WITH_DB + getConnectionProperties(),
                ip, port, "mysql"), configProp.getProperty("cdcSyncDbUser"),
            configProp.getProperty("cdcSyncDbPasswd"));
    }

    public void doCheck(int type) throws SQLException {
        log.info("start check data with type " + type);
        List<String> filteredTables = new ArrayList<>();
        List<Pair<String, String>> tablesPair = new ArrayList<>();
        List<String> databases = getDatabases(type);
        for (String database : databases) {
            if (filterDb(database)) {
                log.info("database is filtered for check [{}]", database);
                continue;
            }

            List<String> tables = getTableList(database, type);
            for (String table : tables) {
                Pair<String, String> tablePair = new Pair<>(database, table);
                if (filterTable(tablePair)) {
                    log.info("table is filtered for check, {}.{}", tablePair.getKey(), tablePair.getValue());
                    filteredTables.add(StringUtils.lowerCase(tablePair.getKey() + "." + tablePair.getValue()));
                    continue;
                }
                tablesPair.add(tablePair);
            }
        }

        List<Future<?>> futures = new ArrayList<>();
        ExecutorService executorService = Executors.newFixedThreadPool(20);
        ExecutorCompletionService<?> completionService = new ExecutorCompletionService<>(executorService);
        for (Pair<String, String> tablePair : tablesPair) {
            futures.add(completionService.submit(() -> {
                checkOneTable(tablePair);
                return null;
            }));
        }

        int index = 0;
        Exception exception = null;
        while (index < futures.size()) {
            try {
                Future<?> future = completionService.take();
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                exception = e;
                break;
            }

            index++;
        }

        // 任何一个线程返回，出现了异常，就退出整个调度
        if (index < futures.size()) {
            for (int errorIndex = 0; errorIndex < futures.size(); errorIndex++) {
                Future<?> future = futures.get(errorIndex);
                if (future.isDone()) {
                    try {
                        future.get();
                    } catch (InterruptedException | ExecutionException e) {
                        exception = e;
                    }
                } else {
                    future.cancel(true); // 对未完成的进行取消
                }
            }
        } else {
            for (int i = 0; i < futures.size(); i++) {// 收集一下正确处理完成的结果
                Future<?> future = futures.get(i);
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    exception = e;
                }
            }
        }

        if (exception != null) {
            throw new TddlNestableRuntimeException("Record-writing-failure occurred in adapt.", exception);
        }

        log.info("filtered tables count : " + filteredTables.size());
        log.info("filtered tables list : " + filteredTables);
        log.info("finish check data for type " + type);
    }

    public boolean filterDb(String database) {
        String checkAllowList = configProp.getProperty(ConfigConstant.CDC_CHECK_DB_ALLOWLIST, "");
        if (StringUtils.isNotBlank(checkAllowList)) {
            String[] dbs = StringUtils.split(checkAllowList, ";");
            for (String db : dbs) {
                if (StringUtils.equals(db, database)) {
                    return false;
                }
            }
            return true;
        } else {
            String checkBlackList = configProp.getProperty(ConfigConstant.CDC_CHECK_DB_BLACKLIST, "");
            if (StringUtils.isNotBlank(checkBlackList)) {
                String[] dbs = StringUtils.split(StringUtils.lowerCase(checkBlackList), ";");
                for (String db : dbs) {
                    if (StringUtils.equalsIgnoreCase(db, database)) {
                        return true;
                    }
                }
            }
            return false;
        }
    }

    public boolean filterTable(Pair<String, String> tablePair) {
        String database = tablePair.getKey();
        String table = tablePair.getValue();
        String fullTable = StringUtils.lowerCase(database + "." + table);
        String checkAllowList = configProp.getProperty(ConfigConstant.CDC_CHECK_TABLE_ALLOWLIST);
        if (StringUtils.isNotBlank(checkAllowList)) {
            String[] patterns = StringUtils.split(checkAllowList, ";");
            for (String patternStr : patterns) {
                Pattern pattern = Pattern.compile(patternStr);
                Matcher m = pattern.matcher(fullTable);
                if (m.find()) {
                    return false;
                }
            }
            log.info("Table is not in allow list. table name: {}, allow list: {}", fullTable, checkAllowList);
            return true;
        } else {
            String checkBlackList = configProp.getProperty(ConfigConstant.CDC_CHECK_TABLE_BLACKLIST, "");
            if (StringUtils.isNotBlank(checkBlackList)) {
                String[] patterns = StringUtils.split(StringUtils.lowerCase(checkBlackList), ";");
                for (String patternStr : patterns) {
                    Pattern pattern = Pattern.compile(patternStr);
                    Matcher m2 = pattern.matcher(fullTable);
                    if (m2.find()) {
                        log.info("table is filtered, table name is {}, with pattern {}", fullTable, patternStr);
                        return true;
                    }
                }
            }
            return false;
        }
    }
    public static final String SAFE_TABLE_PREFIX = "safe_point_table_";
    /**
     * 保障源库binlog，目标库收到了
     */
    public void safePoint() throws SQLException, InterruptedException {
        String dbName = PropertiesUtil.polardbXDBName1(false);
        int safeSuffix = 1;

        String currentSafePoint = getCurrentSafePointTable(dbName);
        if (StringUtils.isNotBlank(currentSafePoint)){
            safeSuffix = Integer.parseInt(currentSafePoint.substring(SAFE_TABLE_PREFIX.lastIndexOf("_")+1));
            String dropDDL = String.format("drop table `%s`.`%s`", dbName, currentSafePoint);
            executeDDL(dropDDL);
        }
        String nextTable = SAFE_TABLE_PREFIX + (safeSuffix+1);
        String createNextTable = String.format("create table `%s`.`%s`(id int primary key)", dbName, nextTable);
        executeDDL(createNextTable);

        long start = System.currentTimeMillis();
        int waitTimeMinute = Integer.valueOf(configProp.getProperty("cdcWaitTokenTimeOutMinute", "5"));
        while (true) {
            try {
                waitForSafeTable(dbName, nextTable);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() - start > 1000 * 60 * waitTimeMinute) {
                    throw new RuntimeException("check cdc safe point timeout", e);
                }
                Thread.sleep(1000);
            }
        }
    }

    private String getCurrentSafePointTable(String dbName) throws SQLException {
        List<String> srcTableList = getTableList(dbName, 0);
        for (String st : srcTableList){
            if (st.startsWith(SAFE_TABLE_PREFIX)){
                return st;
            }
        }
        return null;
    }

    private void waitForSafeTable(String dbName ,String tableName) throws SQLException {
        Connection connection = dstDs.getConnection();
        try{
            Statement st = connection.createStatement();
            st.execute("show create table `"+dbName+"`.`" + tableName+"`");
        }finally {
            connection.close();
        }
    }

    private void executeDDL(String ddl) throws SQLException {
        Connection connection = srcDs.getConnection();
        try{
            Statement st = connection.createStatement();
            st.executeUpdate(ddl);
        }finally {
            connection.close();
        }
    }

    public void checkOneTable(Pair<String, String> tablePair) throws Exception {
        try {
            List<String> columns1 = getColumnsByDesc(tablePair.getKey(), tablePair.getValue(), srcDs);
            List<String> columns2 = getColumnsByDesc(tablePair.getKey(), tablePair.getValue(), dstDs);
            Assert.assertEquals(
                columns1.stream().map(String::toLowerCase).collect(Collectors.toList()),
                columns2.stream().map(String::toLowerCase).collect(Collectors.toList()));
            log.info("columns is consistent between source and target for table {}.{}", tablePair.getKey(),
                tablePair.getValue());
        } catch (Throwable t) {
            log.info("Compute columns consistent error for [{}].[{}]!", tablePair.getKey(), tablePair.getValue(), t);
            throw t;
        }

        List<String> columns = getColumns(tablePair.getKey(), tablePair.getValue(), srcDs);
        String sourceSQL = generateChecksumSQL(tablePair.getKey(), tablePair.getValue(), columns);
        String dstSQL = generateChecksumSQL(tablePair.getKey(), tablePair.getValue(), columns);
        Connection sourceConn = null;
        ResultSet sourceRs = null;

        Connection targetConn = null;
        ResultSet targetRs = null;
        try {
            sourceConn = srcDs.getConnection();
            sourceRs = DataSourceUtil.query(sourceConn, sourceSQL, 0);
            String sourceCheckSum = null;
            while (sourceRs.next()) {
                sourceCheckSum = sourceRs.getString(1);
            }

            targetConn = dstDs.getConnection();
            targetRs = DataSourceUtil.query(targetConn, dstSQL, 0);
            String targetChecksum = null;
            while (targetRs.next()) {
                targetChecksum = targetRs.getString(1);
            }

            if (!StringUtils.equals(sourceCheckSum, targetChecksum)) {
                throw new RuntimeException(
                    String.format("checksum is diff for table %s.%s, source checksum is %s, target checksum is %s,"
                            + "source check sql is %s , target check sql is %s.",
                        tablePair.getKey(), tablePair.getValue(), sourceCheckSum, targetChecksum, sourceSQL, dstSQL));
            } else {
                log.info(String.format("checksum is consistent between source and target for table %s.%s",
                    tablePair.getKey(), tablePair.getValue()));
            }
        } catch (Exception e) {
            log.error("Compute checksum error. SQL: {}", sourceSQL, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(sourceRs, null, sourceConn);
            DataSourceUtil.closeQuery(targetRs, null, targetConn);
        }
    }

    public void waitCdcToken(String token) throws InterruptedException {
        long start = System.currentTimeMillis();
        int waitTimeMinute = Integer.valueOf(configProp.getProperty("cdcWaitTokenTimeOutMinute", "5"));
        while (true) {
            try {
                checkCdcToken(token);
                break;
            } catch (Exception e) {
                if (System.currentTimeMillis() - start > 1000 * 60 * waitTimeMinute) {
                    throw new RuntimeException("check cdc token timeout");
                }
                Thread.sleep(1000);
            }
        }
    }

    public void sendCdcToken(String token) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = srcDs.getConnection();
            stmt = conn.createStatement();
            stmt.execute("drop table if exists random_sql." + token);
            stmt.execute("create table random_sql." + token + "(id bigint)");
        } catch (SQLException e) {
            log.error("Exception in sending cdc token. SQL: {}", sql, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
    }

    public void checkCdcToken(String token) throws SQLException {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = dstDs.getConnection();
            stmt = conn.createStatement();
            stmt.execute("show create table random_sql." + token);
        } catch (SQLException e) {
            log.error("Exception in checking cdc token. SQL: {}", sql, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
    }

    public List<String> getDatabases(int type) throws SQLException {
        List<String> databases = new ArrayList<>();

        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        String sql = null;
        try {
            if (type == 0) {
                conn = srcDs.getConnection();
            } else {
                conn = dstDs.getConnection();
            }
            sql = "show databases";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String db = rs.getString(1);
                if (!StringUtils.equalsIgnoreCase(db, "information_schema") &&
                    !StringUtils.equalsIgnoreCase(db, "mysql") &&
                    !StringUtils.equalsIgnoreCase(db, "sys") &&
                    !StringUtils.equalsIgnoreCase(db, "performance_schema")) {
                    databases.add(StringUtils.lowerCase(db));
                }
            }
        } catch (SQLException e) {
            log.error("Exception in finding databases. SQL: {}", sql, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return databases;
    }

    public List<String> getTableList(String database, int type) throws SQLException {
        List<String> tables = new ArrayList<>();
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        String sql = null;
        try {
            if (type == 0) {
                conn = srcDs.getConnection();
            } else {
                conn = dstDs.getConnection();
            }

            conn.setCatalog(database);
            sql = "show tables;";
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("Exception in finding columns. SQL: {}", sql, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return tables;
    }

    public List<String> getColumns(String database, String tableName, DataSource ds) throws Exception {
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<>();
        String sql = null;
        try {
            conn = ds.getConnection();
            sql = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'",
                database, tableName);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                columns.add(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("Exception in finding columns. SQL: {}, database {}, table {}", sql, database, tableName, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return columns;
    }

    public List<String> getColumnsByDesc(String database, String tableName, DataSource ds) throws Exception {
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        List<String> columns = new ArrayList<>();
        String sql = null;
        try {
            conn = ds.getConnection();
            sql = String.format("DESC `%s`.`%s`", MetaBuilder.escape(database), MetaBuilder.escape(tableName));
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                columns.add(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("Exception in finding columns. SQL: {}, database {}, table {}", sql, database, tableName, e);
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return columns;
    }

    public String getCreateTableSql(String table) throws SQLException {
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = srcDs.getConnection();
            sql = String.format("show create table %s", table);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                return rs.getString(2);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return "";
    }

    public int getCount(String table) throws SQLException {
        PreparedStatement stmt = null;
        Connection conn = null;
        ResultSet rs = null;
        String sql = null;
        try {
            conn = srcDs.getConnection();
            sql = String.format("select count(*) from %s", table);
            stmt = conn.prepareStatement(sql);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            DataSourceUtil.closeQuery(rs, stmt, conn);
        }
        return 0;
    }

    public String generateChecksumSQL(String dbName, String tableName, List<String> columns) {
        // ISNULL(`id`), ISNULL(`name`), ISNULL(`order_id`)
        StringBuilder concatSb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            if (i == 0) {
                concatSb.append(String.format("ISNULL(`%s`)", columns.get(i)));
            } else {
                concatSb.append(String.format(", ISNULL(`%s`)", columns.get(i)));
            }
        }
        // ',', `id`, `name`, `order_od`, CONCAT(ISNULL(`id`), ISNULL(`name`), ISNULL(`order_id`)))
        StringBuilder concatWsSb = new StringBuilder();
        // ',' + space
        concatWsSb.append("',', ");
        for (String column : columns) {
            concatWsSb.append(String.format("`%s`, ", column));
        }
        concatWsSb.append(concatSb);
        // CONCAT_WS(',', `id`, `name`, `order_od`, CONCAT(ISNULL(`id`), ISNULL(`name`), ISNULL(`order_id`))))
        String concatWs = String.format("CONCAT_WS(%s)", concatWsSb);

        return String.format(
            "SELECT BIT_XOR(CAST(CRC32(%s) AS UNSIGNED)) AS checksum FROM `%s`.`%s`", concatWs,
            MetaBuilder.escape(dbName), MetaBuilder.escape(tableName));
    }

    public void checkStatus() {
        String masterUrl = configProp.getProperty("cdcCheckUrlMaster", "");
        String result1 = HttpClientHelper.doGet(masterUrl);
        String slaveUrl = configProp.getProperty("cdcCheckUrlSlave", "");
        String result2 = HttpClientHelper.doGet(slaveUrl);

        if ("OK".equals(result1) && "OK".equals(result2)) {
            log.info("master and slave is ok");
        } else {
            throw new TddlNestableRuntimeException(
                String.format("master or slave is not ok, master status is %s, slave status is %s", result1, result2));
        }
    }

    public static void main(String args[]) {
        String s1 = ".*";
        Pattern pattern1 = Pattern.compile(s1);
        Matcher m1 = pattern1.matcher("aaa");
        System.out.println(m1.find());
        System.out.println("+++++++++++++");

        String s2 = "aaa\\.bbb";
        Pattern pattern2 = Pattern.compile(s2);
        Matcher m2 = pattern2.matcher("aaaobbb");
        Matcher m21 = pattern2.matcher("aaa.bbb");
        Matcher m22 = pattern2.matcher("aaa.ccc");
        Matcher m23 = pattern2.matcher("aaabbb");
        System.out.println(m2.find());
        System.out.println(m21.find());
        System.out.println(m22.find());
        System.out.println(m23.find());
        System.out.println("+++++++++++++");

        String s3 = ".*\\.bb_.*";
        Pattern pattern3 = Pattern.compile(s3);
        Matcher m3 = pattern3.matcher("aaaobbb");
        Matcher m31 = pattern3.matcher("aaa.bbb");
        Matcher m32 = pattern3.matcher("aaa.bb_");
        Matcher m33 = pattern3.matcher("aaa.bb_cc");
        System.out.println(m3.find());
        System.out.println(m31.find());
        System.out.println(m32.find());
        System.out.println(m33.find());
    }
}
