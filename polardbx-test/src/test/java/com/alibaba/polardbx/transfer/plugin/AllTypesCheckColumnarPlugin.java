package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.AllTypesTestUtils;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.transfer.utils.AllTypesTestUtils.COLUMNAR_INDEX_NAME;

/**
 * @author yaozhili
 */
public class AllTypesCheckColumnarPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesWriteOnlyPlugin.class);

    private final List<String> allColumns = new ArrayList<>();
    private final Map<Long, Long> lastTsoMap = new ConcurrentHashMap<>();

    public AllTypesCheckColumnarPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("check_columnar");
        if (null == config) {
            enabled = false;
            return;
        }
        enabled = config.getBoolean("enabled", false);
        threads = Math.toIntExact(config.getLong("threads", 1L));
        boolean bigColumn = TomlConfig.getConfig().getBoolean("big_column", false);
        allColumns.addAll(AllTypesTestUtils.getColumns());
        if (bigColumn) {
            allColumns.addAll(AllTypesTestUtils.getBigColumns());
        }
        if (enabled) {
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    // Create columnar index.
                    String createSql = AllTypesTestUtils.FULL_TYPE_TABLE_COLUMNAR_INDEX;
                    stmt.execute(createSql);
                } catch (Throwable t) {
                    if (t.getMessage().contains("Duplicate index name")) {
                        // ignore.
                    } else {
                        logger.error("create columnar index failed, skip AllTypesCheckColumnarPlugin.", t);
                        enabled = false;
                    }
                }
            });
        }
    }

    @Override
    protected void runInternal() {
        long lastTso = lastTsoMap.getOrDefault(Thread.currentThread().getId(), -1L);
        List<String> checkResults = new ArrayList<>();
        // Cci fast checker.
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=true */ CHECK COLUMNAR INDEX "
                    + COLUMNAR_INDEX_NAME;
                ResultSet rs = stmt.executeQuery(checkSql);
                while (rs.next()) {
                    checkResults.add(rs.getString("DETAILS"));
                }
                stmt.execute("SELECT SLEEP(1)");
            } catch (SQLException e) {
                logger.error("Write only error.", e);
                error.set(e);
            }
        });
        if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
            errorAllTypesTest1(checkResults, "Cci fast checker failed.");
        }

        // Cci naive checker.
        checkResults.clear();
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=false */ CHECK COLUMNAR INDEX "
                    + COLUMNAR_INDEX_NAME;
                ResultSet rs = stmt.executeQuery(checkSql);
                while (rs.next()) {
                    checkResults.add(rs.getString("DETAILS"));
                }
                stmt.execute("SELECT SLEEP(1)");
            } catch (SQLException e) {
                logger.error("Write only error.", e);
                error.set(e);
            }
        });
        if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
            errorAllTypesTest1(checkResults, "Cci naive checker failed.");
        }

        // Cci increment checker.
        if (lastTso > 0) {
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    long currentTso = columnarFlushAndGetTso(stmt);
                    if (currentTso > lastTso) {
                        lastTsoMap.put(Thread.currentThread().getId(), currentTso);
                        String checkSql = "CHECK COLUMNAR INDEX "
                            + COLUMNAR_INDEX_NAME + " INCREMENT " + lastTso + " " + currentTso + " " + currentTso;
                        ResultSet rs = stmt.executeQuery(checkSql);
                        while (rs.next()) {
                            checkResults.add(rs.getString("DETAILS"));
                        }
                        stmt.execute("SELECT SLEEP(1)");
                    } else {
                        checkResults.add("Fail to get current tso.");
                    }
                } catch (SQLException e) {
                    logger.error("Write only error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, "Cci increment checker failed.");
            }
        }

        // Cci snapshot fast checker.
        if (lastTso > 0) {
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    long currentTso = columnarFlushAndGetTso(stmt);
                    if (currentTso > lastTso) {
                        lastTsoMap.put(Thread.currentThread().getId(), currentTso);
                        String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=true */ CHECK COLUMNAR INDEX "
                            + COLUMNAR_INDEX_NAME + " SNAPSHOT " + lastTso + " " + lastTso;
                        ResultSet rs = stmt.executeQuery(checkSql);
                        while (rs.next()) {
                            checkResults.add(rs.getString("DETAILS"));
                        }
                        stmt.execute("SELECT SLEEP(1)");
                    } else {
                        checkResults.add("Fail to get current tso.");
                    }
                } catch (SQLException e) {
                    logger.error("Write only error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, "Cci increment checker failed.");
            }
        }

        // Cci snapshot naive checker.
        if (lastTso > 0) {
            checkResults.clear();
            getConnectionAndExecute(dsn, (conn, error) -> {
                try (Statement stmt = conn.createStatement()) {
                    long currentTso = columnarFlushAndGetTso(stmt);
                    if (currentTso > lastTso) {
                        lastTsoMap.put(Thread.currentThread().getId(), currentTso);
                        String checkSql = "/*+TDDL:ENABLE_CCI_FAST_CHECKER=false */ CHECK COLUMNAR INDEX "
                            + COLUMNAR_INDEX_NAME + " SNAPSHOT " + lastTso + " " + lastTso;
                        ResultSet rs = stmt.executeQuery(checkSql);
                        while (rs.next()) {
                            checkResults.add(rs.getString("DETAILS"));
                        }
                        stmt.execute("SELECT SLEEP(1)");
                    } else {
                        checkResults.add("Fail to get current tso.");
                    }
                } catch (SQLException e) {
                    logger.error("Write only error.", e);
                    error.set(e);
                }
            });
            if (checkResults.isEmpty() || !checkResults.get(0).startsWith("OK")) {
                errorAllTypesTest1(checkResults, "Cci increment checker failed.");
            }
        }
    }

    static private long columnarFlushAndGetTso(Statement stmt) throws SQLException {
        ResultSet rs = stmt.executeQuery("call polardbx.columnar_flush()");
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            return -1;
        }
    }
}
