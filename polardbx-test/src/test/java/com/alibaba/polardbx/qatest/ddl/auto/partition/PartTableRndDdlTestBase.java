package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.util.JdbcUtil;
import com.alibaba.polardbx.qatest.constant.ConfigConstant;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.getConnectionProperties;

/**
 * @author chenghui.lch
 */
@Ignore
public class PartTableRndDdlTestBase extends PartitionTestBase {
    protected static final Log log = LogFactory.getLog(PartTableRndDdlTestBase.class);
    protected static final String SELECT_RND_DDL_INFOS =
        "select * from rnd_ddl_db.rnd_ddl_info order by db_name,tb_name, stmt_id limit 100";

    protected static class RndDdlInfo {
        public Timestamp createTs;
        public long stmtId;
        public String caseId;
        public String targetDbName;
        public String targetTgName;
        public String targetTbName;

        public String ddlSql = "";
        public String createTbSql = "";
        public int forTg = 0;// 0:for tb, 1:for tg
        public int arows = 0;
    }

    protected static class TestParameter {
        public String caseName;
        public String dbName;
        public String tbName;
        public List<RndDdlInfo> rndDdlInfos = new ArrayList<>();

        @Override
        public String toString() {
            String str = String.format("[%s]",
                String.join(",", this.caseName, this.dbName, this.tbName, String.valueOf(this.rndDdlInfos.size())));
            return str;
        }
    }

    protected PartTableRndDdlTestBase.TestParameter parameter;

    public PartTableRndDdlTestBase(PartTableRndDdlTestBase.TestParameter parameter) {
        this.parameter = parameter;
    }

    protected static void initDdlTestEnv() {
        try (Connection plxConn = ConnectionManager.getInstance().newPolarDBXConnection()) {
            try (Statement stmt = plxConn.createStatement()) {
                stmt.executeUpdate("drop database if exists d1");
                stmt.executeUpdate("drop database if exists d2");
                stmt.executeUpdate("drop database if exists d3");

                stmt.executeUpdate("create database if not exists d1 mode='auto'");
                stmt.executeUpdate("create database if not exists d2 mode='auto'");
                stmt.executeUpdate("create database if not exists d3 mode='drds'");
            }
        } catch (Throwable ex) {
            log.error(ex);
            throw new RuntimeException(ex);
        }
    }

    protected void testRandomDdl() {
        String dbName = parameter.dbName;
        try (Connection plxConn = getPolardbxConnection(dbName)) {
            try (Statement stmt = plxConn.createStatement()) {
                String createTbSql = parameter.rndDdlInfos.get(0).createTbSql;
                try {
                    stmt.executeUpdate(createTbSql);
                    log.info("run create succ, sql=" + createTbSql);
                } catch (Throwable ex) {
                    log.info("run create fail, sql=" + createTbSql);
                    ex.printStackTrace();
                    Assert.fail(ex.getMessage());
                }

                for (int i = 0; i < parameter.rndDdlInfos.size(); i++) {
                    String ddl = parameter.rndDdlInfos.get(i).ddlSql;
                    int arows = parameter.rndDdlInfos.get(i).arows;
                    long stmtId = parameter.rndDdlInfos.get(i).stmtId;
                    boolean shouldSucc = arows == 0;
                    boolean execSucc = true;
                    StringBuilder sqlLogSb = new StringBuilder();
                    sqlLogSb.append(stmtId).append(":").append(ddl);
                    try {
                        stmt.executeUpdate(ddl);
                    } catch (Throwable ex) {
                        log.error(ex);
                        execSucc = false;
                    } finally {
                        if (execSucc) {
                            log.info("run succ, " + sqlLogSb);
                        } else {
                            log.info("run failed, " + sqlLogSb);
                        }
                    }
                    Assert.assertTrue(shouldSucc == execSucc);
                }
            }
        } catch (Throwable ex) {
            log.error(ex);
            Assert.fail(ex.getMessage());
        }
    }
}
