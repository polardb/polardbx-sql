package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.TableMetaParser;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlCreateTable;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class TtlPartitionUtilTest extends BasePlannerTest {

    public TtlPartitionUtilTest() {
        super("optest");
    }

    @Test
    public void testTtlPartitionRouterFixPartitions() throws SQLException {

        try {

            String ddl =
                "create table ttldb1.myttl(a int, b datetime) partition by range columns(b) (partition p19700101 values less than('1970-01-01'), partition p19700201 values less than('1970-02-01'))";
            ExecutionContext ec = new ExecutionContext();
            String dbName = "ttldb1";
            ec.setParams(new Parameters());
            ec.setSchemaName(dbName);
            ec.setServerVariables(new HashMap<>());

            MySqlCreateTableStatement stmt =
                (MySqlCreateTableStatement) FastsqlUtils.parseSql(ddl).get(0);
            TableMeta tm = new TableMetaParser().parse(stmt);

            SqlCreateTable sqlCreateTable = (SqlCreateTable) FastsqlParser
                .convertStatementToSqlNode(stmt, null, ec);

            BasePlannerTest.buildLogicalCreateTable(dbName, tm, sqlCreateTable, stmt.getTableName(),
                PartitionTableType.PARTITION_TABLE, PlannerContext.fromExecutionContext(ec));

            PartitionInfo partInfo = tm.getPartitionInfo();
            TtlPartitionUtil.TtlPartitionRouter router =
                new TtlPartitionUtil.TtlPartitionRouter(tm, PartKeyLevel.PARTITION_KEY, "+08:00");

            List<Pair<String, String>> newAddPartSpecInfosOutput = new ArrayList<>();
            newAddPartSpecInfosOutput.add(new Pair<>("p19691101", "1970-01-01"));
            newAddPartSpecInfosOutput.add(new Pair<>("p19691201", "1970-01-01"));
            newAddPartSpecInfosOutput.add(new Pair<>("p19700101", "1970-01-01"));
            newAddPartSpecInfosOutput.add(new Pair<>("p19700201", "1970-02-01"));
            newAddPartSpecInfosOutput.add(new Pair<>("p19700301", "1970-03-01"));
            newAddPartSpecInfosOutput.add(new Pair<>("p19700401", "1970-03-01"));

            List<Pair<String, String>> newAddPartSpecInfosAfterFixingOutput = new ArrayList<>();
            List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput = new ArrayList<>();
            router.fixInvalidPartitionsWithPartInfo(newAddPartSpecInfosOutput, newAddPartSpecInfosAfterFixingOutput,
                fixedPartSpecIndexMappingsOutput);
            String outputMsg = "";
            for (int i = 0; i < newAddPartSpecInfosAfterFixingOutput.size(); i++) {

                Pair<String, String> partAndBnd = newAddPartSpecInfosAfterFixingOutput.get(i);
                String partAndBndStr =
                    String.format("partition `%s` values than '%s'", partAndBnd.getKey(), partAndBnd.getValue());
                if (i > 0) {
                    outputMsg += ",\n";
                }
                outputMsg += partAndBndStr;
            }
            System.out.println(outputMsg);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Override
    protected String getPlan(String testSql) {
        return "";
    }
}
