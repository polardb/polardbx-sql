package com.alibaba.polardbx.qatest.ddl.cdc;

import com.alibaba.polardbx.common.cdc.DdlScope;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlCheckContext;
import com.alibaba.polardbx.qatest.ddl.cdc.entity.DdlRecordInfo;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * description:
 * author: ziyang.lb
 * create: 2023-08-30 17:43
 **/
@Slf4j
public class CdcSequenceMarkTest extends CdcBaseTest {

    @Test
    public void testCdcDdlRecord() throws SQLException, InterruptedException {
        try (Statement stmt = tddlConnection.createStatement()) {
            DdlCheckContext checkContext = newDdlCheckContext();
            checkContext.updateAndGetMarkList("polardbx");
            stmt.executeUpdate("use polardbx");

            for (int i = 0; i < 30; i++) {
                try {
                    executeAndCheck(checkContext, stmt, "CONVERT ALL SEQUENCES FROM NEW TO GROUP", "*");
                    break;
                } catch (Throwable t) {
                    log.error("convert all sequence failed!! will retry", t);
                    Thread.sleep(1000);
                }
            }

            // create database
            stmt.executeUpdate("drop database if exists cdc_sequence_test");
            stmt.executeUpdate("create database cdc_sequence_test  mode = 'auto'");
            stmt.executeUpdate("use cdc_sequence_test");

            // execute and check
            checkContext = newDdlCheckContext();
            checkContext.updateAndGetMarkList("cdc_sequence_test");
            executeAndCheck(checkContext, stmt, "CREATE NEW SEQUENCE newseq START WITH 1000", "newseq");
            executeAndCheck(checkContext, stmt,
                "CREATE NEW SEQUENCE newseq2 START WITH 1 INCREMENT BY 2 MAXVALUE 100 CYCLE",
                "newseq2");
            executeAndCheck(checkContext, stmt, "CREATE NEW SEQUENCE newseq3 START WITH 1000", "newseq3");
            executeAndCheck(checkContext, stmt, "CREATE GROUP SEQUENCE groupseq", "groupseq");
            executeAndCheck(checkContext, stmt, "CREATE GROUP SEQUENCE groupseq2", "groupseq2");
            executeAndCheck(checkContext, stmt, "CREATE GROUP SEQUENCE ugroupseq UNIT COUNT 3 INDEX 0", "ugroupseq");
            executeAndCheck(checkContext, stmt, "CREATE GROUP SEQUENCE ugroupseq2 UNIT COUNT 3 INDEX 1", "ugroupseq2");
            executeAndCheck(checkContext, stmt, "CREATE GROUP SEQUENCE ugroupseq3 UNIT COUNT 3 INDEX 2", "ugroupseq3");
            executeAndCheck(checkContext, stmt, "CREATE TIME SEQUENCE seq3", "seq3");
            executeAndCheck(checkContext, stmt, "ALTER SEQUENCE newseq START WITH 1000000", "newseq");
            executeAndCheck(checkContext, stmt, "ALTER SEQUENCE newseq2 CHANGE TO GROUP START WITH 2000000", "newseq2");
            executeAndCheck(checkContext, stmt, "ALTER SEQUENCE newseq3 CHANGE TO TIME", "newseq3");
            executeAndCheck(checkContext, stmt, "ALTER SEQUENCE groupseq CHANGE TO NEW START WITH 100", "groupseq");
            executeAndCheck(checkContext, stmt, "ALTER SEQUENCE groupseq2 CHANGE TO NEW "
                + "START WITH 200 INCREMENT BY 2 MAXVALUE 300 NOCYCLE", "groupseq2");
            executeAndCheck(checkContext, stmt, "CONVERT ALL SEQUENCES FROM NEW TO GROUP FOR cdc_sequence_test", "*");
            executeAndCheck(checkContext, stmt, "RENAME SEQUENCE newseq to newseq1", "newseq");

            List<String> allSeqs = new ArrayList<>();
            ResultSet rs = JdbcUtil.executeQuery("SHOW SEQUENCES", stmt);
            while (rs.next()) {
                if (StringUtils.equals("cdc_sequence_test", rs.getString("SCHEMA_NAME"))) {
                    allSeqs.add(rs.getString("NAME"));
                }
            }
            for (String s : allSeqs) {
                executeAndCheck(checkContext, stmt, "DROP sequence " + s, s);
            }
        }
    }

    private void executeAndCheck(DdlCheckContext checkContext, Statement stmt, String sql, String sequenceName)
        throws SQLException {
        stmt.executeUpdate(sql);
        String schemaName = getCurrentDbName(stmt);
        List<DdlRecordInfo> beforeList = checkContext.getMarkList(schemaName);
        List<DdlRecordInfo> afterList = checkContext.updateAndGetMarkList(schemaName);

        Assert.assertEquals(beforeList.size() + 1, afterList.size());
        Assert.assertEquals(sql, afterList.get(0).getDdlSql());
        Assert.assertEquals(schemaName, afterList.get(0).getSchemaName());
        Assert.assertEquals(sequenceName, afterList.get(0).getTableName());
        Assert.assertEquals(DdlScope.Schema.getValue(), afterList.get(0).getDdlExtInfo().getDdlScope());
    }
}
