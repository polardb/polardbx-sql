package com.alibaba.polardbx.executor.columnar.pruning.data;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.columnar.pruning.index.BitMapRowGroupIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.List;

/**
 * @author fangwu
 */
public class PruneUtilsTest {
    @Test
    public void pruneNull() {
        IndexPruneContext ipc = new IndexPruneContext();
        ipc.setParameters(new Parameters());
        PruneUtils.transformRexToIndexMergeTree((List<RexNode>) null, ipc);
    }

    @Test
    public void pruneDate() {
        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RexBuilder rexBuilder = new RexBuilder(factory);
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
            new RexInputRef(0, factory.createSqlType(SqlTypeName.INTEGER)),
            rexBuilder.makeLiteral("2020-12-12"));

        List<RexNode> rexNodes = Lists.newArrayList();
        rexNodes.add(rexNode);

        List<ColumnMeta> columnMetas = Lists.newArrayList();
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        IndexPruneContext ipc = new IndexPruneContext();
        ipc.setParameters(new Parameters());
        System.out.println(
            PruneUtils.display(PruneUtils.transformRexToIndexMergeTree(rexNodes, ipc), columnMetas, ipc));
    }

    @Test
    public void pruneDateCast() {
        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RexBuilder rexBuilder = new RexBuilder(factory);
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
            new RexInputRef(0, factory.createSqlType(SqlTypeName.INTEGER)),
            rexBuilder.makeCastForConvertlet(factory.createSqlType(SqlTypeName.DATE),
                rexBuilder.makeLiteral("2020-12-12")));

        List<RexNode> rexNodes = Lists.newArrayList();
        rexNodes.add(rexNode);

        List<ColumnMeta> columnMetas = Lists.newArrayList();
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        IndexPruneContext ipc = new IndexPruneContext();
        ipc.setParameters(new Parameters());
        System.out.println(
            PruneUtils.display(PruneUtils.transformRexToIndexMergeTree(rexNodes, ipc), columnMetas, ipc));
    }

    @Test
    public void pruneDateNot() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        RelDataTypeFactory factory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        RexBuilder rexBuilder = new RexBuilder(factory);
        RexNode rexNode = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN,
            new RexInputRef(0, factory.createSqlType(SqlTypeName.INTEGER)),
            rexBuilder.makeCastForConvertlet(factory.createSqlType(SqlTypeName.DATE),
                rexBuilder.makeLiteral("2020-12-12")));
        // add not
        rexNode = rexBuilder.makeCall(SqlStdOperatorTable.NOT, rexNode);

        List<RexNode> rexNodes = Lists.newArrayList();
        rexNodes.add(rexNode);

        List<ColumnMeta> columnMetas = Lists.newArrayList();
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        columnMetas.add(new ColumnMeta("test_table",
            "test_col",
            null,
            new Field(DataTypes.IntegerType)));
        IndexPruneContext ipc = new IndexPruneContext();
        ipc.setParameters(new Parameters());
        System.out.println(
            PruneUtils.display(PruneUtils.transformRexToIndexMergeTree(rexNodes, ipc), columnMetas, ipc));

        ColumnPredicatePruningInf columnPredicatePruningInf = PruneUtils.transformRexToIndexMergeTree(rexNodes, ipc);

        RoaringBitmap bitmap = RoaringBitmap.bitmapOfRange(10, 100);
        columnPredicatePruningInf.sortKey(SortKeyIndex.build(0, new long[] {1, 10, 11, 100}, DataTypes.LongType), null,
            bitmap);
        columnPredicatePruningInf.zoneMap(
            ZoneMapIndex.build(1, Maps.newHashMap(), Maps.newHashMap(), Maps.newHashMap()), null, bitmap);
        columnPredicatePruningInf.bitmap(new BitMapRowGroupIndex(1, Maps.newHashMap(), Maps.newHashMap()), null,
            bitmap);

        Assert.assertTrue(bitmap.getCardinality() == 90);
    }
}
