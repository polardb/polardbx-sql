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

/**
 * @author yaozhili
 */
public class AllTypesWriteOnlyPlugin extends BasePlugin {
    private static final Logger logger = LoggerFactory.getLogger(AllTypesWriteOnlyPlugin.class);

    private final List<String> allColumns = new ArrayList<>();

    public AllTypesWriteOnlyPlugin() {
        super();
        Toml config = TomlConfig.getConfig().getTable("write_only");
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
    }

    @Override
    protected void runInternal() {
        getConnectionAndExecute(dsn, (conn, error) -> {
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("set sql_mode=''");
                stmt.execute("begin");

                String sql = null;
                try {
                    // Insert a new record.
                    sql = AllTypesTestUtils.buildInsertSql(1, allColumns);
                    stmt.execute(sql);

                    // Update a random row.
                    sql = AllTypesTestUtils.buildSelectRandomSql();
                    ResultSet rs = stmt.executeQuery(sql);
                    long id = 0;
                    if (rs.next()) {
                        id = rs.getLong(1);
                    }
                    sql = AllTypesTestUtils.buildUpdateSql(id, allColumns);
                    stmt.execute(sql);

                    // Delete a random row.
                    sql = AllTypesTestUtils.buildSelectRandomSql();
                    rs = stmt.executeQuery(sql);
                    id = 0;
                    if (rs.next()) {
                        id = rs.getLong(1);
                    }
                    sql = AllTypesTestUtils.buildDeleteSql(id);
                    stmt.execute(sql);

                    // Commit.
                    stmt.execute("commit");
                } catch (Throwable t) {
                    logger.error("Write only error, sql: " + sql, t);
                    stmt.execute("rollback");
                }

            } catch (SQLException e) {
                logger.error("Write only error.", e);
                error.set(e);
            }
        });
    }
}
