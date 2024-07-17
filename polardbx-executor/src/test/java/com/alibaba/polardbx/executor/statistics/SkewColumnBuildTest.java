package com.alibaba.polardbx.executor.statistics;

import com.alibaba.polardbx.executor.common.BasePlannerTest;
import com.alibaba.polardbx.executor.gms.util.StatisticUtils;
import com.alibaba.polardbx.executor.utils.EclipseParameterized;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

@RunWith(EclipseParameterized.class)
public class SkewColumnBuildTest extends BasePlannerTest {

    public SkewColumnBuildTest(String caseName) {
        super(caseName, caseName);
    }

    @Parameterized.Parameters()
    public static List<String> prepare() {
        return Lists.newArrayList(SkewColumnBuildTest.class.getSimpleName());
    }

    List<String> checkTb1 = ImmutableList.<String>builder()
        .add("id,order_id,buyer_id,")
        .add("order_id,buyer_id,order_date,")
        .add("id,order_id,")
        .add("order_id,buyer_id,")
        .add("buyer_id,order_detail,")
        .add("id,")
        .add("order_id,")
        .add("order_detail,")
        .build();

    List<String> checkTb2 = ImmutableList.<String>builder()
        .add("id,order_id,buyer_id,")
        .add("order_id,buyer_id,seller_id,")
        .add("order_id,buyer_id,")
        .add("order_id,")
        .build();

    @Override
    public void doPlanTest() {
        testColumnBitSet("t_order1", checkTb1);
        testColumnBitSet("t_order2", checkTb2);
    }

    private void testColumnBitSet(String tableName, List<String> check) {
        List<ColumnMeta> analyzeColumnList = getColumnMetas(false, getAppName(), tableName);

        List<List<Integer>> columnSns = StatisticUtils.buildColumnBitSet(getAppName(), tableName, analyzeColumnList);
        List<String> ansString = Lists.newArrayList();
        for (List<Integer> columnSn : columnSns) {
            StringBuilder stringBuilder = new StringBuilder();
            columnSn.stream().forEach(i -> stringBuilder.append(analyzeColumnList.get(i).getName()).append(","));
            ansString.add(stringBuilder.toString());
        }

        for (int i = 0; i < ansString.size(); i++) {
            Assert.assertEquals(check.get(i), ansString.get(i));
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
