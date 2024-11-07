package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * @author fangwu
 */
public class SelectivityTest extends PlanTestCommon {

    public SelectivityTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(
            new Object[] {"DrdsRelMdCostTest", "/com/alibaba/polardbx/optimizer/config/meta/DrdsRelMdCostTest"});
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Test
    public void testCorrelate() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String testSql =
            "select * from (select * from key_tbl union all (select * from key_tbl)) b where name='c' or bid in(select bid from hash_tbl )";
        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(testSql, executionContext);
        RelNode relNode = plan.getPlan();

        RelMetadataQuery mq = PlannerUtils.newMetadataQuery();

        Method m = BuiltInMetadata.Selectivity.class.getMethod("getSelectivity", RexNode.class);

        Filter filter = (Filter) relNode.getInput(0);
        Correlate correlate = (Correlate) filter.getInput(0);

        Metadata metadata = correlate.metadata(BuiltInMetadata.Selectivity.class, mq);
        m.invoke(metadata, filter.getCondition());
    }

    @Test
    public void testSql() {

    }
}