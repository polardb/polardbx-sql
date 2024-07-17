package com.alibaba.polardbx.qatest.storagepool.importDatabase;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.GmsJdbcUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.math.Rand;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.INST_KIND_MASTER;
import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_READY;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportDatabaseBase extends BaseTestCase {
    public static final Logger LOG = LoggerFactory.getLogger(ImportDatabaseBase.class);

    protected List<StorageInfoRecord> queryStorageInfos() {
        final String queryStorageSql = "select * from storage_info";
        List<StorageInfoRecord> storageInfos = new ArrayList<>();
        try (Connection metaConn = getMetaConnection();
            ResultSet result = JdbcUtil.executeQuerySuccess(metaConn, queryStorageSql)
        ) {
            while (result.next()) {
                StorageInfoRecord record = new StorageInfoRecord().fill(result);
                storageInfos.add(record);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LOG.info("getStorageInfo: " + storageInfos);
        return storageInfos;
    }

    @Override
    public synchronized Connection getPolardbxConnection() {
        final int neverTimeOut = 24 * 60 * 60 * 1000;
        Connection conn = super.getPolardbxConnection();
        Executor noOpExecutor = Runnable::run;
        try {
            conn.setNetworkTimeout(noOpExecutor, neverTimeOut);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        return conn;
    }

    @Override
    public synchronized Connection getPolardbxConnection(String db) {
        final int neverTimeOut = 24 * 60 * 60 * 1000;
        Connection conn = super.getPolardbxConnection(db);
        Executor noOpExecutor = Runnable::run;
        try {
            conn.setNetworkTimeout(noOpExecutor, neverTimeOut);
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        }
        return conn;
    }

    protected static Connection buildJdbcConnectionByStorageInfo(StorageInfoRecord storageInfoRecord) {
        String ip = storageInfoRecord.ip;
        Integer port = storageInfoRecord.port;
        String user = storageInfoRecord.user;
        String passwdEnc = storageInfoRecord.passwdEnc;
        Map<String, String> connPropsMap = new HashMap<>();
        connPropsMap.put("socketTimeout", "0");
        String connProps = GmsJdbcUtil.getJdbcConnPropsFromPropertiesMap(connPropsMap);

        return buildJdbcConnection(ip, port, "information_schema", user, passwdEnc, connProps);
    }

    public Connection buildJdbcConnectionByStorageInstId(String instId) {
        List<StorageInfoRecord> allAvailableStorageInst = getAvailableStorageInfo();
        StorageInfoRecord targetInst = allAvailableStorageInst.stream()
            .filter(x -> x.storageInstId.equalsIgnoreCase(instId))
            .findFirst()
            .get();

        return buildJdbcConnectionByStorageInfo(targetInst);
    }

    protected static Connection buildJdbcConnection(String host, int port, String dbName, String user, String passwdEnc,
                                                    String connProps) {
        String passwd = PasswdUtil.decrypt(passwdEnc);
        String url = String.format("jdbc:mysql://%s:%s/%s?%s", host, port, dbName, connProps);
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return DriverManager.getConnection(url, user, passwd);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, ex,
                String.format("Failed to create connection to [%s]", url));
        }
    }

    public List<StorageInfoRecord> getAvailableStorageInfo() {
        List<StorageInfoRecord> records = queryStorageInfos();
        return records.stream()
            .filter(x -> x.instKind == INST_KIND_MASTER)
            .filter(x -> x.instKind == STORAGE_STATUS_READY).distinct().collect(Collectors.toList());
    }

    public void preparePhyDatabase(String phyNameToBeCreated, Connection storageInstConn) {
        String createSql = "create database `" + phyNameToBeCreated + "`";
        JdbcUtil.executeUpdateSuccess(storageInstConn, createSql);
    }

    public void preparePhyTables(List<String> tables, Connection storageInstConn, String database) {
        String useSql = "use `" + database + "`";
        JdbcUtil.executeUpdateSuccess(storageInstConn, useSql);
        for (String tb : tables) {
            JdbcUtil.executeUpdateSuccess(storageInstConn, tb);
        }
    }

    public void cleanDatabse(Connection conn, String database) {
        String cleanSql = "drop database if exists `" + database + "`";
        JdbcUtil.executeUpdateSuccess(conn, cleanSql);
    }

    public boolean checkDatabaseExist(Connection conn, String database) {
        String showSql = "show databases";
        Set<String> dbs = new TreeSet<>(String::compareToIgnoreCase);
        ResultSet rs = null;
        try {
            rs = JdbcUtil.executeQuerySuccess(conn, showSql);
            while (rs.next()) {
                String dbName = rs.getString(1);
                dbs.add(dbName);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
        }

        return dbs.contains(database);
    }

    public boolean checkTableExist(Connection conn, String database, String table) {
        String showSql = "show tables";
        String useSql = "use " + database;
        Set<String> tbs = new TreeSet<>(String::compareToIgnoreCase);
        ResultSet rs = null;
        try {
            JdbcUtil.executeUpdateSuccess(conn, useSql);
            rs = JdbcUtil.executeQuerySuccess(conn, showSql);
            while (rs.next()) {
                String tbName = rs.getString(1);
                tbs.add(tbName);
            }
        } catch (Exception e) {
            throw new TddlNestableRuntimeException(e);
        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception ignore) {
                }
            }
        }
        return tbs.contains(table);
    }

    public void checkRebalanceResult(String schema) {
        final int maxRetryTimes = 300;
        final int sleepElapseSeconds = 5;
        int retryCnt = 0;
        boolean checkResult = false;
        final String querySql =
            "select * from information_schema.ddl_plan where table_schema = '" + schema + "' order by gmt_created desc";

        do {
            try {
                Thread.sleep(1000 * sleepElapseSeconds);
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            } finally {
                retryCnt++;
            }

            //query ddl plan
            try (Connection polardbxConnection = getPolardbxConnection();
                ResultSet rs = JdbcUtil.executeQuerySuccess(polardbxConnection, querySql)) {
                if (rs.next()) {
                    String state = rs.getString("state");
                    if ("SUCCESS".equalsIgnoreCase(state)) {
                        checkResult = true;
                    }
                }
            } catch (Exception e) {
                throw new TddlNestableRuntimeException(e);
            }

        } while (retryCnt < maxRetryTimes && !checkResult);

        Assert.assertTrue(checkResult);
    }

    public String randomChooseOne(List<String> candidateInst, Set<String> tobeAvoidInst) {
        Random r = new Random();
        int offset = r.nextInt(10000);
        for (int i = 0; i < candidateInst.size(); i++) {
            int idx = (i + offset) % candidateInst.size();
            String newCandidate = candidateInst.get(idx);
            if (!tobeAvoidInst.contains(newCandidate)) {
                return newCandidate;
            }
        }
        return null;
    }

}
