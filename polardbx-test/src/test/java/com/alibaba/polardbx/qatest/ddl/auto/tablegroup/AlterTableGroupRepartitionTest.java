package com.alibaba.polardbx.qatest.ddl.auto.tablegroup;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class AlterTableGroupRepartitionTest extends DDLBaseNewDBTestCase {

    final static String CREATE_DB = "create database %s mode=auto";
    final static String DROP_DB = "drop database if exists %s";
    final static String TABLE_SCHEMA = "AlterTableGroupRepartitionTestDb";
    final static String CREATE_TABLE_GROUP = "create tablegroup %s";
    final static String CREATE_TABLE = "create table %s (a int, b int, unique key(b)) %s tablegroup=%s";

    final static String PARTITION_BY_KEY_KEY =
        "partition by key (a) subpartition by key(b) (partition p1 subpartitions 1)";
    final static String PARTITION_BY_KEY = "partition by key (a) partitions 1";
    final static String INSERT_DATA =
        "insert into %s values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)";
    final static String INSERT_IGNORE_DATA = "insert ignore into %s select * from %s where a=%s";
    final static String UPDATE_DATA = "update %s set b=b+100 where a=%s";

    final static String SOURCE_WRITE_ONLY_HINT =
        "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='SOURCE_WRITE_ONLY')*/";
    final static String SOURCE_DELETE_ONLY_HINT =
        "/*+TDDL:CMD_EXTRA(PHYSICAL_BACKFILL_ENABLE=false, TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG='SOURCE_DELETE_ONLY')*/";

    @Before
    public void setUp() {
        String sql = "use polardbx";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        JdbcUtil.executeUpdate(tddlConnection, String.format(DROP_DB, TABLE_SCHEMA));
        JdbcUtil.executeUpdate(tddlConnection, String.format(CREATE_DB, TABLE_SCHEMA));
    }

    private TestStrategy testStrategy;

    public AlterTableGroupRepartitionTest(TestStrategy testStrategy) {
        this.testStrategy = testStrategy;
    }

    @Test
    public void testAlterTableGroupMovePartition() {
        String sql = "use " + TABLE_SCHEMA;
        JdbcUtil.executeUpdate(tddlConnection, sql);
        String tgName = "move_tg1";

        String tbName = "mpt2";
        boolean isSubPart = testStrategy.name().contains("SUBPARTITION");
        boolean isTbLvl = testStrategy.name().startsWith("TB_");
        boolean isMove = testStrategy.name().contains("_MOVE_");
        boolean isWriteOnlyHint = testStrategy.name().contains("WRITEONLY");

        JdbcUtil.executeUpdate(tddlConnection, String.format(CREATE_TABLE_GROUP, tgName));
        JdbcUtil.executeUpdate(tddlConnection,
            String.format(CREATE_TABLE, "mpt1", isSubPart ? PARTITION_BY_KEY_KEY : PARTITION_BY_KEY, tgName));
        JdbcUtil.executeUpdate(tddlConnection,
            String.format(CREATE_TABLE, "mpt2", isSubPart ? PARTITION_BY_KEY_KEY : PARTITION_BY_KEY, tgName));
        JdbcUtil.executeUpdate(tddlConnection,
            String.format(CREATE_TABLE, "mpt3", isSubPart ? PARTITION_BY_KEY_KEY : PARTITION_BY_KEY, tgName));
        JdbcUtil.executeUpdate(tddlConnection, String.format(INSERT_DATA, "mpt1"));

        String tarDn = "";
        String partName = isSubPart ? "p1sp1" : "p1";
        if (isMove) {
            Map<String, String> partDnMap =
                getPartDnMap("mpt1", isSubPart, tddlConnection);
            List<String> storageInstIds = getStorageInstIds(TABLE_SCHEMA);
            if (storageInstIds.size() <= 1) {
                return;
            }
            String srcDn = partDnMap.get(partName);
            for (int i = 0; i < storageInstIds.size(); i++) {
                if (!storageInstIds.get(i).equalsIgnoreCase(srcDn)) {
                    tarDn = storageInstIds.get(i);
                    break;
                }
            }
        }

        String ignoreErr = "Please use SHOW DDL";
        Set<String> ignoreErrs = new HashSet<>();
        ignoreErrs.add(ignoreErr);
        StringBuilder sb = new StringBuilder();
        if (isWriteOnlyHint) {
            sb.append(SOURCE_WRITE_ONLY_HINT);
        } else {
            sb.append(SOURCE_DELETE_ONLY_HINT);
        }
        sb.append(" ");
        if (isTbLvl) {
            sb.append("alter table ");
            sb.append(tbName);
        } else {
            sb.append("alter tablegroup ");
            sb.append(tgName);
        }
        sb.append(" ");
        if (isMove) {
            sb.append("move ");
            if (isSubPart) {
                sb.append("subpartitions p1sp1 to '");
            } else {
                sb.append("partitions p1 to '");
            }
            sb.append(tarDn);
            sb.append("'");
        } else {
            sb.append("split ");
            if (isSubPart) {
                sb.append("subpartition p1sp1");
            } else {
                sb.append("partition p1");
            }
        }

        JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, sb.toString(), ignoreErrs);
        JdbcUtil.executeUpdate(tddlConnection, String.format(INSERT_IGNORE_DATA, "mpt2", "mpt1", "1"));
        JdbcUtil.executeUpdate(tddlConnection, String.format(UPDATE_DATA, "mpt2", "1"));
    }

    private Map<String, String> getPartDnMap(String tableName, boolean subPart, Connection conn) {
        ResultSet resultSet = JdbcUtil.executeQuery("show topology from " + tableName, conn);
        Map<String, String> partDnMap = new HashMap<>();
        try {
            while (resultSet.next()) {
                partDnMap.put(
                    subPart ? resultSet.getString("SUBPARTITION_NAME") : resultSet.getString("PARTITION_NAME"),
                    resultSet.getString("DN_ID"));
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return partDnMap;
    }

    @After
    public void tearDown() {
        String sql = "use polardbx";
        JdbcUtil.executeUpdate(tddlConnection, sql);
        JdbcUtil.executeUpdate(tddlConnection, String.format(DROP_DB, TABLE_SCHEMA));
    }

    @Parameterized.Parameters(name = "{index}:partitionRuleInfo={0}")
    public static List<TestStrategy[]> prepareData() {
        List<TestStrategy[]> status = new ArrayList<>();
        status.add(new TestStrategy[] {TestStrategy.TG_MOVE_WRITEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_MOVE_WRITEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_MOVE_DELETEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_MOVE_DELETEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_SPLIT_WRITEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_SPLIT_WRITEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_SPLIT_DELETEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TG_SPLIT_DELETEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_MOVE_WRITEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_MOVE_WRITEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_MOVE_DELETEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_MOVE_DELETEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_SPLIT_WRITEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_SPLIT_WRITEONLY_SUBPARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_SPLIT_DELETEONLY_PARTITION});
        status.add(new TestStrategy[] {TestStrategy.TB_SPLIT_DELETEONLY_SUBPARTITION});
        return status;
    }

    enum TestStrategy {
        TG_MOVE_WRITEONLY_PARTITION,
        TG_MOVE_WRITEONLY_SUBPARTITION,
        TG_MOVE_DELETEONLY_PARTITION,
        TG_MOVE_DELETEONLY_SUBPARTITION,
        TG_SPLIT_WRITEONLY_PARTITION,
        TG_SPLIT_WRITEONLY_SUBPARTITION,
        TG_SPLIT_DELETEONLY_PARTITION,
        TG_SPLIT_DELETEONLY_SUBPARTITION,
        TB_MOVE_WRITEONLY_PARTITION,
        TB_MOVE_WRITEONLY_SUBPARTITION,
        TB_MOVE_DELETEONLY_PARTITION,
        TB_MOVE_DELETEONLY_SUBPARTITION,
        TB_SPLIT_WRITEONLY_PARTITION,
        TB_SPLIT_WRITEONLY_SUBPARTITION,
        TB_SPLIT_DELETEONLY_PARTITION,
        TB_SPLIT_DELETEONLY_SUBPARTITION
    }
}
