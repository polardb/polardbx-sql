package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class AlterTableCompatBaseTest extends DDLBaseNewDBTestCase {

    protected void executeAndCheck(String sqlTemplate, String tableName) {
        String sql = String.format(sqlTemplate, tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);
        checkTable(tableName);
    }

    protected void executeAndFail(String sqlTemplate, String tableName, String errorMessage) {
        String sql = String.format(sqlTemplate, tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errorMessage);
        checkTable(tableName);
    }

    protected void checkTable(String tableName) {
        List<Pair<String, String>> results = new ArrayList<>();
        String sql = String.format("check table %s", tableName);
        try (PreparedStatement ps = tddlConnection.prepareStatement(sql);
            ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                String msgType = rs.getString("MSG_TYPE");
                String msgText = rs.getString("MSG_TEXT");
                if (!TStringUtil.equalsIgnoreCase(msgType, "status") || !TStringUtil.equalsIgnoreCase(msgText, "OK")) {
                    results.add(Pair.of(msgType, msgText));
                }
            }
        } catch (SQLException e) {
            Assert.fail(String.format("Failed to check table: %s", e.getMessage()));
        }
        if (GeneralUtil.isNotEmpty(results)) {
            StringBuilder buf = new StringBuilder();
            buf.append("\nUnexpected: Check Table is not OK\n");
            for (Pair<String, String> result : results) {
                buf.append("MSG_TYPE: ").append(result.getKey()).append(", ");
                buf.append("MSG_TEXT: ").append(result.getValue()).append("\n");
            }
            Assert.fail(buf.toString());
        }
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
