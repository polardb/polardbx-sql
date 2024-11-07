package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.BloomFilterIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnarPredicatePruningVisitor;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.DateString;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author fangwu
 */
@RunWith(Parameterized.class)
public class TpchColumnarPruneTest extends ColumnarPruneTest {
    private static JavaTypeFactory typeFactory;

    public TpchColumnarPruneTest(final Frameworks.PlannerAction<RexNode> action,
                                 IndexPruner indexPruner,
                                 long target,
                                 IndexPruneContext ipcTarget, String caseName) {
        super(action, indexPruner, target, ipcTarget);
    }

    @Parameterized.Parameters(name = "{4}")
    public static List<Object[]> prepareCases() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        if (typeFactory == null) {
            typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        }
        IndexPruner indexPruner = mockPrunerForLineitem();
        List<Object[]> params = Lists.newArrayList();

        // Q1 l_shipdate <= date '1998-09-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1998, 9, 1);
            return rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d));
        }, indexPruner, 592793L, "Q1 l_shipdate <= date '1998-09-01'"));

        // Q3 l_shipdate > '1995-03-15'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d = new DateString(1995, 3, 15);
            return rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d));
        }, indexPruner, 324456L, "Q3 l_shipdate > '1995-03-15'"));

        // Q6 l_shipdate < date '1995-01-01' and l_shipdate >= '1994-01-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d1 = new DateString(1995, 1, 1);
            final DateString d2 = new DateString(1994, 1, 1);
            RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d1));
            RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d2));
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2);
        }, indexPruner, 91500L, "Q6 l_shipdate < date '1995-01-01' and l_shipdate >= '1994-01-01'"));

        // Q6 l_shipdate < date '1995-01-01' and l_discount between 0.05 and 0.07 and l_quantity < 24 and l_shipdate >= '1994-01-01'
        params.add(buildCase((rexBuilder, scan) -> {
                final DateString d1 = new DateString(1995, 1, 1);
                final DateString d2 = new DateString(1994, 1, 1);
                RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                    rexBuilder.makeInputRef(scan, 11),
                    rexBuilder.makeDateLiteral(d1));
                RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                    rexBuilder.makeInputRef(scan, 11),
                    rexBuilder.makeDateLiteral(d2));
                RexNode r3 = rexBuilder.makeCall(SqlStdOperatorTable.BETWEEN,
                    rexBuilder.makeInputRef(scan, 7),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(0.05D)),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(0.07D)));
                RexNode r4 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                    rexBuilder.makeInputRef(scan, 5),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(24L)));
                return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2, r3, r4);
            }, indexPruner, 91500L,
            "Q6 l_shipdate < date '1995-01-01' and l_discount between 0.05 and 0.07 and l_quantity < 24 and l_shipdate >= '1994-01-01'"));

        // Q7 l_shipdate between '1995-01-01' and '1996-01-01'
        Map<Integer, ParameterContext> paramMap = new HashMap<>();
        paramMap.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, "1995-1-1"}));
        paramMap.put(2, new ParameterContext(ParameterMethod.setObject1, new Object[] {2, "1996-1-1"}));
        Parameters parameters = new Parameters(paramMap);

        params.add(buildCase((rexBuilder, scan) -> rexBuilder.makeCall(SqlStdOperatorTable.BETWEEN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.DATETIME), 1),
                rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.DATETIME), 2)),
            indexPruner,
            91500L,
            "Q7 l_shipdate between '1995-01-01' and '1996-01-01'",
            parameters));

        // Q12 l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d1 = new DateString(1995, 1, 1);
            final DateString d2 = new DateString(1994, 1, 1);
            RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 13),
                rexBuilder.makeDateLiteral(d2));
            RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(scan, 13),
                rexBuilder.makeDateLiteral(d1));
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2);
        }, indexPruner, 98750L, "Q12 l_receiptdate >= '1994-01-01' and l_receiptdate < '1995-01-01'"));

        // Q14 l_shipdate >= '1995-09-01' and l_shipdate < '1995-10-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d1 = new DateString(1995, 9, 1);
            final DateString d2 = new DateString(1995, 10, 1);
            RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d1));
            RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d2));
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2);
        }, indexPruner, 7750L, "Q14 l_shipdate >= '1995-09-01' and l_shipdate < '1995-10-01'"));

        // Q15 l_shipdate >= '1996-01-01' and l_shipdate <'1996-04-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d1 = new DateString(1996, 1, 1);
            final DateString d2 = new DateString(1996, 4, 1);
            RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d1));
            RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d2));
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2);
        }, indexPruner, 23000L, "Q15 l_shipdate >= '1996-01-01' and l_shipdate <'1996-04-01'"));

        // Q20 l_shipdate>='1994-01-01' and l_shipdate<'1995-01-01'
        params.add(buildCase((rexBuilder, scan) -> {
            final DateString d1 = new DateString(1994, 1, 1);
            final DateString d2 = new DateString(1995, 1, 1);
            RexNode r1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d1));
            RexNode r2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeDateLiteral(d2));
            return rexBuilder.makeCall(SqlStdOperatorTable.AND, r1, r2);
        }, indexPruner, 91500L, "Q20 l_shipdate>='1994-01-01' and l_shipdate<'1995-01-01'"));

        return params;
    }

    private static Object[] buildCase(TwoFunction<RexBuilder, LogicalTableScan, RexNode> f, IndexPruner indexPruner,
                                      long target, String caseName) {
        return buildCase(f, indexPruner, target, caseName, new Parameters());
    }

    private static Object[] buildCase(TwoFunction<RexBuilder, LogicalTableScan, RexNode> f, IndexPruner indexPruner,
                                      long target, String caseName, Parameters parameters) {
        Frameworks.PlannerAction<RexNode> action = (cluster, relOptSchema, rootSchema) -> {
            rootSchema.add("tpch",
                new ReflectiveSchema(new TpchSchema()));
            LogicalTableScan scan =
                LogicalTableScan.create(cluster,
                    relOptSchema.getTableForMember(
                        Arrays.asList("tpch", "lineItems")));
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            return f.apply(rexBuilder, scan);
        };
        IndexPruneContext ipcTarget = new IndexPruneContext();
        ipcTarget.setParameters(parameters);
        return new Object[] {action, indexPruner, target, ipcTarget, caseName};
    }

    @Test
    public void test() {
        // transform to ColumnPredicate
        ColumnPredicatePruningInf columnPredicate = rex.accept(new ColumnarPredicatePruningVisitor(ipcTarget));

        // load columnar index
        RoaringBitmap rb = indexPruner.prune("", Lists.newArrayList(), columnPredicate, ipcTarget);

        // check result rg list
        if (rb.getCardinality() != target) {
            Assert.fail("target rg num:" + target + ", real rg num:" + rb.getCardinality());
        }
    }

    private static IndexPruner mockPrunerForLineitem() {
        BloomFilterIndex bloomFilterIndex = mockBloomFilterIndexForLineitem();
        IndexPruner.IndexPrunerBuilder indexPrunerBuilder = new IndexPruner.IndexPrunerBuilder("mock file", false);
        indexPrunerBuilder.setBloomFilterIndex(bloomFilterIndex);
        mockSortKeyIndexForLineitem(indexPrunerBuilder);
        mockZoneMapIndexForLineitem(indexPrunerBuilder);
        return indexPrunerBuilder.build();
    }

    private static BloomFilterIndex mockBloomFilterIndexForLineitem() {
        return null;
    }

    /**
     * zone map for receipt_date, it should be in the range between ship_date + 1DAY and ship_date + 1MONTH
     */
    private static void mockZoneMapIndexForLineitem(IndexPruner.IndexPrunerBuilder indexPrunerBuilder) {
        Iterator<Long> it = getShipDateIterator();
        indexPrunerBuilder.appendZoneMap(13, DataTypes.DateType);
        while (it.hasNext()) {
            Long date = it.next();
            MysqlDateTime cur = TimeStorage.readDate(date);
            MySQLInterval interval = new MySQLInterval();
            interval.setDay(1);
            MysqlDateTime min = MySQLTimeCalculator.addInterval(cur, MySQLIntervalType.INTERVAL_DAY, interval);
            interval.setDay(30);
            MysqlDateTime max = MySQLTimeCalculator.addInterval(cur, MySQLIntervalType.INTERVAL_DAY, interval);
            indexPrunerBuilder.appendZoneMap(13, min.toPackedLong(), max.toPackedLong());
        }
    }

    @NotNull
    private static Iterator<Long> getShipDateIterator() {
        InputStream in = TpchColumnarPruneTest.class.getResourceAsStream("lineitem_index.properties");
        if (in == null) {
            throw new IllegalArgumentException("need index config lineitem_index.properties");
        }
        Properties shipDate = parserProperties(in);
        Iterator<Long> it = new Iterator<Long>() {
            private int defaultCycleTime = 250;
            private LocalDate curDate = LocalDate.of(1992, 01, 02);
            private int curCycle = 0;

            @Override
            public boolean hasNext() {
                if (curDate.isBefore(LocalDate.of(1998, 12, 1)) ||
                    (curDate.isEqual(LocalDate.of(1998, 12, 1)) &&
                        curCycle < getCycleTime(curDate))) {
                    return true;
                }
                return false;
            }

            @Override
            public Long next() {
                int maxCycle = getCycleTime(curDate);
                if (curCycle >= maxCycle) {
                    // advance next day
                    curDate = curDate.plusDays(1);
                    curCycle = 1;
                } else {
                    curCycle++;
                }
                MysqlDateTime mysqldateTime =
                    new MysqlDateTime(curDate.getYear(), curDate.getMonthValue(), curDate.getDayOfMonth(), 0, 0, 0, 0);
                return mysqldateTime.toPackedLong();
            }

            private int getCycleTime(LocalDate curDate) {
                String key = curDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                if (shipDate.get(key) == null) {
                    return defaultCycleTime;
                } else {
                    return Integer.parseInt(String.valueOf(shipDate.get(key)));
                }
            }
        };
        return it;
    }

    private static BitMapRowGroupIndex mockBitMapRowGroupIndexForLineitem() {
        return null;
    }

    /**
     * lineitem table should build sort key index by column ship_date
     * from 1992-01-02 to 1998-12-01
     */
    private static void mockSortKeyIndexForLineitem(IndexPruner.IndexPrunerBuilder indexPrunerBuilder) {
        indexPrunerBuilder.setSortKeyColId(11);
        indexPrunerBuilder.setSortKeyDataType(DataTypes.DateType);
        Iterator<Long> it = getShipDateIterator();
        int rgNum = 0;
        while (it.hasNext()) {
            Long date = it.next();
            indexPrunerBuilder.appendMockSortKeyIndex(date, date, DataTypes.LongType);
            rgNum++;
        }
        indexPrunerBuilder.setRgNum(rgNum);
    }

    public static Properties parserProperties(InputStream in) {
        Properties serverProps = new Properties();
        try {
            if (in != null) {
                serverProps.load(in);
            }
            return serverProps;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public static class TpchSchema {
        static final int lineNum = 1024;

        @Override
        public String toString() {
            return "TpchSchema";
        }

        public final LineItem[] lineItems;
//        public final Order[] orders = generate(new OrderGenerator(0.0001, 1, 1));
//        public final Part[] parts = generate(new PartGenerator(0.0001, 1, 1));
//        public final PartSupplier[] partSuppes = generate(new PartSupplierGenerator(0.0001, 1, 1));
//        public final Customer[] customers = generate(new CustomerGenerator(0.0001, 1, 1));
//        public final Supplier[] suppliers = generate(new SupplierGenerator(0.0001, 1, 1));
//        public final Nation[] nations = generate(new NationGenerator());
//        public final Region[] regions = generate(new RegionGenerator());

        {
            lineItems = new LineItem[0];
        }
    }

    public interface TwoFunction<T, V, R> {
        R apply(T var1, V var2);
    }
}
