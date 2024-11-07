package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnarPredicatePruningVisitor;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.DateString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fangwu
 */
@RunWith(Parameterized.class)
public class PredicatePruningTest extends ColumnarPruneTest {
    private static JavaTypeFactory typeFactory;
    private String[] columns;
    private String predicateCheck;

    public PredicatePruningTest(
        Frameworks.PlannerAction<RexNode> action,
        IndexPruner indexPruner, long target,
        IndexPruneContext ipcTarget, String[] columns, String predicateCheck) {
        super(action, indexPruner, target, ipcTarget);
        this.columns = columns;
        this.predicateCheck = predicateCheck;
    }

    @Parameterized.Parameters(name = "{4}")
    public static List<Object[]> prepareCases() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        if (typeFactory == null) {
            typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        }
        List<Object[]> params = Lists.newArrayList();

        // ship_instructions <= string 'abcdefg'
        params.add(buildPredicateCase((rexBuilder, scan) -> {
                final String d = new String("abcdefg");
                return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                    rexBuilder.makeInputRef(scan, 14),
                    rexBuilder.makeLiteral(d));
            }, mockPruner(DataTypes.VarcharType), -1L, new Parameters(),
            "shipInstructions_14 LESS_THAN_OR_EQUAL abcdefg"));

        //  l_shipdate <= date '1998-09-01'
        params.add(buildPredicateCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d));
        }, mockPruner(DataTypes.DateType), -1L, new Parameters(), "shipDate_11 LESS_THAN_OR_EQUAL 1998-09-01"));

        // date '1998-09-01' >=  l_shipdate
        params.add(buildPredicateCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeDateLiteral(d),
                rexBuilder.makeInputRef(scan, 11));
        }, mockPruner(DataTypes.DateType), -1L, new Parameters(), "shipDate_11 LESS_THAN_OR_EQUAL 1998-09-01"));

        // date '1998-09-01' = l_shipdate
        params.add(buildPredicateCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeDateLiteral(d),
                rexBuilder.makeInputRef(scan, 11));
        }, mockPruner(DataTypes.DateType), -1L, new Parameters(), "shipDate_11 EQUALS 1998-09-01"));

        // l_shipdate > date '1998-09-01'
        params.add(buildPredicateCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d));
        }, mockPruner(DataTypes.DateType), -1L, new Parameters(), "shipDate_11 GREATER_THAN 1998-09-01"));

        // date '1998-09-01' > l_shipdate
        params.add(buildPredicateCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeDateLiteral(d),
                rexBuilder.makeInputRef(scan, 11));
        }, mockPruner(DataTypes.DateType), -1L, new Parameters(), "shipDate_11 LESS_THAN 1998-09-01"));
        return params;
    }

    @Test
    public void test() {
        ColumnPredicatePruningInf columnPredicate = rex.accept(new ColumnarPredicatePruningVisitor(ipcTarget));
        String real = columnPredicate.display(columns, ipcTarget).toString();
        System.out.println(real);
        System.out.println(predicateCheck);
        Assert.assertTrue(real.equals(predicateCheck));
    }

    protected static Object[] buildPredicateCase(
        TpchColumnarPruneTest.TwoFunction<RexBuilder, LogicalTableScan, RexNode> f,
        IndexPruner indexPruner,
        long target, Parameters parameters, String predicateCheck) {
        Frameworks.PlannerAction<RexNode> action = (cluster, relOptSchema, rootSchema) -> {
            rootSchema.add("tpch",
                new ReflectiveSchema(new TpchColumnarPruneTest.TpchSchema()));
            LogicalTableScan scan =
                LogicalTableScan.create(cluster,
                    relOptSchema.getTableForMember(
                        Arrays.asList("tpch", "lineItems")));
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            return f.apply(rexBuilder, scan);
        };
        IndexPruneContext ipcTarget = new IndexPruneContext();
        ipcTarget.setParameters(parameters);
        String[] columns =
            Arrays.stream(LineItem.class.getDeclaredFields()).map(field -> field.getName()).collect(Collectors.toList())
                .toArray(new String[0]);
//            new String[] {"orderKey", "partKey", "supplierKey", "lineNumber", "quantity", "extendedPrice", ""};
        return new Object[] {action, indexPruner, target, ipcTarget, columns, predicateCheck};
    }
}
