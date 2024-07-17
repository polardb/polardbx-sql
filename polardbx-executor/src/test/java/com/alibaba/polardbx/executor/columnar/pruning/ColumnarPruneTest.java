package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.junit.runners.Parameterized;
import org.roaringbitmap.RoaringBitmap;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author fangwu
 */
public class ColumnarPruneTest {
    public static final int RG_NUM_FOR_TEST = 1024;
    protected RexNode rex;
    protected IndexPruner indexPruner;
    protected long target;
    protected IndexPruneContext ipcTarget;

    public ColumnarPruneTest(final Frameworks.PlannerAction<RexNode> action,
                             IndexPruner indexPruner,
                             long target,
                             IndexPruneContext ipcTarget) {
        this.rex = Frameworks.withPlanner(action);
        this.indexPruner = indexPruner;
        this.target = target;
        this.ipcTarget = ipcTarget;
    }

    @Parameterized.Parameters(name = "{0}:{1}:{2}:{3}")
    public static List<Object[]> prepareCases() {
        Frameworks.PlannerAction<RexNode> action = (cluster, relOptSchema, rootSchema) -> {
            rootSchema.add("tpch",
                new ReflectiveSchema(new TpchColumnarPruneTest.TpchSchema()));
            LogicalTableScan scan =
                LogicalTableScan.create(cluster,
                    relOptSchema.getTableForMember(
                        Arrays.asList("tpch", "lineItems")));
            final RexBuilder rexBuilder = cluster.getRexBuilder();
            return rexBuilder.makeCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                rexBuilder.makeInputRef(scan, 11),
                rexBuilder.makeLiteral("35500"));
        };

        IndexPruner indexPruner = mockPruner();
        RoaringBitmap target = RoaringBitmap.bitmapOfRange(0, 35);
        IndexPruneContext ipcTarget = new IndexPruneContext();
        List<Object[]> params = Lists.newArrayList();
        params.add(new Object[] {
            action, indexPruner, target, ipcTarget
        });
        return params;
    }

    public static IndexPruner mockPruner() {
        IndexPruner.IndexPrunerBuilder indexPrunerBuilder = new IndexPruner.IndexPrunerBuilder("mock file", false);
        mockSortKeyIndex(indexPrunerBuilder);
        return indexPrunerBuilder.build();
    }

    private static void mockSortKeyIndex(IndexPruner.IndexPrunerBuilder indexPrunerBuilder) {
        Random r = new Random();
        for (int i = 0; i < RG_NUM_FOR_TEST; i++) {
            indexPrunerBuilder.appendSortKeyIndex(1000 * i + r.nextInt(500), 1000 * i + 500 + r.nextInt(500));
        }
    }

}
