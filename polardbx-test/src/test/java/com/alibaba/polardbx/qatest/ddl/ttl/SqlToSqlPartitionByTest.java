package com.alibaba.polardbx.qatest.ddl.ttl;

import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlCreateTableParser;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLCreateTableParser;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import org.apache.calcite.rel.ddl.CreateTable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPartitionBy;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.selectBaseOneTable;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

/**
 * @author bairui.lrj@alibaba-inc.com
 * @since 5.4.9
 */

public class SqlToSqlPartitionByTest extends BaseTestCase {
    private final String table;

    public SqlToSqlPartitionByTest(String table) {
        this.table = table;
    }

    @Parameterized.Parameters(name = "{index}:table={0}")
    public static List<String[]> generateParameters() {
        return Arrays.asList(selectBaseOneTable());
    }

    @Test
    public void testWithoutSchema() {
        buildNewPartInfoForOssTable("d1", "t1");
//        selectContentSameAssert(sql, null, mysqlConnection, tddlConnection);
    }

    public static PartitionInfo buildNewPartInfoForOssTable(String ttlTblSchema,
                                                            String ttlTblName) {

        try {
            ByteString byteString = ByteString.from("PARTITION BY KEY(id) PARTITIONS 8");
            SQLCreateTableParser createTableParser = new MySqlCreateTableParser(byteString);

            SQLPartitionBy fastSqlPartByAst = createTableParser.parsePartitionBy();

            ExecutionContext ec = new ExecutionContext();
            FastSqlToCalciteNodeVisitor visitor =
                new FastSqlToCalciteNodeVisitor(new ContextParameters(false), ec);
            fastSqlPartByAst.accept(visitor);
            SqlPartitionBy sqlPartByAst = (SqlPartitionBy) visitor.getSqlNode();

//            SqlConverter sqlConverter = SqlConverter.getInstance(schemaName, ec);
//            PlannerContext plannerContext = PlannerContext.getPlannerContext(createTable.getCluster());
//            Map<SqlNode, RexNode> partRexInfoCtx = sqlConverter.convertPartition(tableGroupSqlPartitionBy, plannerContext);
//            ((CreateTable) (createTable)).getPartBoundExprInfo().putAll(partRexInfoCtx);

            System.out.println(sqlPartByAst.toString());
        } catch (Throwable ex) {
            ex.printStackTrace();
        }

        return null;
    }

}

