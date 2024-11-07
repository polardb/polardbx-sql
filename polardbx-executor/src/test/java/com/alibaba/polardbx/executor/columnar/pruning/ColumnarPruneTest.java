package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.Frameworks;
import org.apache.hadoop.io.WritableComparator;
import org.junit.runners.Parameterized;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
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

        IndexPruner indexPruner = mockPruner(DataTypes.LongType);
        RoaringBitmap target = RoaringBitmap.bitmapOfRange(0, 35);
        IndexPruneContext ipcTarget = new IndexPruneContext();
        List<Object[]> params = Lists.newArrayList();
        params.add(new Object[] {
            action, indexPruner, target, ipcTarget
        });
        return params;
    }

    public static IndexPruner mockPruner(DataType dataType) {
        IndexPruner.IndexPrunerBuilder indexPrunerBuilder = new IndexPruner.IndexPrunerBuilder("mock file", false);
        mockSortKeyIndex(indexPrunerBuilder, dataType);
        return indexPrunerBuilder.build();
    }

    private static void mockSortKeyIndex(IndexPruner.IndexPrunerBuilder indexPrunerBuilder, DataType dt) {
        Random r = new Random();
        indexPrunerBuilder.setSortKeyDataType(dt);
        if (DataTypeUtil.isStringSqlType(dt)) {
            List<String> strings = new ArrayList<>(RG_NUM_FOR_TEST * 2);
            for (int i = 0; i < RG_NUM_FOR_TEST * 2; i++) {
                strings.add(generateRandomString(6));
            }
            strings.sort((a, b) -> {
                byte[] s1 = a.getBytes();
                byte[] s2 = b.getBytes();
                return WritableComparator.compareBytes(s1, 0, s1.length, s2, 0, s2.length);
            });
            for (int i = 0; i < RG_NUM_FOR_TEST; i++) {
                indexPrunerBuilder.appendMockSortKeyIndex(strings.get(i * 2), strings.get(i * 2 + 1),
                    DataTypes.StringType);
            }
        } else {
            for (int i = 0; i < RG_NUM_FOR_TEST; i++) {
                indexPrunerBuilder.appendMockSortKeyIndex(1000 * i + r.nextInt(500), 1000 * i + 500 + r.nextInt(500),
                    DataTypes.LongType);
            }
        }

    }

    public static String generateRandomString(int length) {
        String str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        Random random = new Random();
        StringBuffer buf = new StringBuffer();

        for (int i = 0; i < length; i++) {
            int num = random.nextInt(62);
            buf.append(str.charAt(num));
        }

        return buf.toString();
    }
}
