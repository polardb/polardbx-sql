package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.executor.common.BasePlannerTest;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.utils.EclipseParameterized;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

@RunWith(EclipseParameterized.class)
public class SkewColumnTest extends BasePlannerTest {

    public SkewColumnTest(String caseName) {
        super(caseName, caseName);
    }

    @Parameterized.Parameters()
    public static List<String> prepare() {
        return Lists.newArrayList(SkewColumnTest.class.getSimpleName());
    }

    List<Row> testRow1 = ImmutableList.<Row>builder()
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .build();

    List<Set<String>> check1 = ImmutableList.<Set<String>>builder()
        .add(Sets.newHashSet("id", "order_id", "order_detail"))
        .build();

    List<Row> testRow2 = ImmutableList.<Row>builder()
        .add(new ArrayRow(new Object[] {1, "t", "r1"}))
        .add(new ArrayRow(new Object[] {1, "t", "r2"}))
        .add(new ArrayRow(new Object[] {1, "t", "r3"}))
        .add(new ArrayRow(new Object[] {1, "t", "r4"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {1, "t", "rr"}))
        .add(new ArrayRow(new Object[] {2, "t", "rr"}))
        .add(new ArrayRow(new Object[] {3, "t", "rr"}))
        .add(new ArrayRow(new Object[] {4, "t", "rr"}))
        .add(new ArrayRow(new Object[] {5, "t", "rr"}))
        .build();

    List<Set<String>> check2 = ImmutableList.<Set<String>>builder()
        .add(Sets.newHashSet("id", "order_id"))
        .add(Sets.newHashSet("order_id", "order_detail"))
        .build();

    @Override
    public void doPlanTest() {
        testSkew("t_order1", testRow1, check1);
        testSkew("t_order2", testRow2, check2);
    }

    private void testSkew(String tableName, List<Row> testRow, List<Set<String>> check) {
        List<ColumnMeta> analyzeColumnList = getColumnMetas(false, getAppName(), tableName);
        StatisticUtils.buildSkew(
            getAppName(),
            tableName,
            analyzeColumnList,
            testRow,
            1e-4f);
        List<Set<String>> skewColumns =
            StatisticManager.getInstance().getCacheLine(getAppName(), tableName).getSkewCols();
        for (int i = 0; i < skewColumns.size(); i++) {
            Assert.assertTrue(check.get(i).containsAll(skewColumns.get(i)));
            Assert.assertTrue(skewColumns.get(i).containsAll(check.get(i)));
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
