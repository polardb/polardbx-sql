package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalIndexScan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 * logical view ut case
 *
 * @author fangwu
 */
public class LogicalViewTest extends BaseRuleTest {

    @Test
    public void testLogicalViewColumnOriginsCopy() {
        LogicalTableScan scan1 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView lv1 = LogicalView.create(scan1, scan1.getTable());

        LogicalTableScan scan2 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        LogicalView lv2 = LogicalView.create(scan1, scan1.getTable());

        List<RexNode> rexNodeList = Lists.newArrayList();
        final RelDataTypeFactory typeFactory = relOptCluster.getTypeFactory();
        RelDataType relDataType = typeFactory.createJoinType(scan1.getRowType(), scan2.getRowType());
        rexNodeList.add(relOptCluster.getRexBuilder().makeInputRef(relDataType, 1));
        rexNodeList.add(relOptCluster.getRexBuilder().makeInputRef(relDataType, 5));

        BKAJoin bkaJoin = BKAJoin.create(scan1.getTraitSet().replace(DrdsConvention.INSTANCE), lv1, lv2,
            relOptCluster.getRexBuilder().makeCall(
                SqlStdOperatorTable.EQUALS, rexNodeList), ImmutableSet.of(), JoinRelType.INNER, false,
            ImmutableList.of(), null);
        lv1.pushJoin(bkaJoin, lv2, null, null);
        lv1.setJoin(bkaJoin);
        lv1.optimize();
        int size = lv1.getColumnOrigins().size();
        Assert.assertTrue(size > 0);

        LogicalView b = (LogicalView) lv1.clone();
        int checkSize = b.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);

        // test index scan
        LogicalIndexScan logicalIndexScan = new LogicalIndexScan(b);
        checkSize = logicalIndexScan.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);

        LogicalIndexScan logicalIndexScanCopy = logicalIndexScan.copy(logicalIndexScan.getTraitSet());

        checkSize = logicalIndexScanCopy.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);

        // test LogicalModifyView
        LogicalModifyView logicalModifyView = new LogicalModifyView(b);
        checkSize = logicalModifyView.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);
        LogicalModifyView logicalModifyViewCopy = logicalModifyView.copy(logicalModifyView.getTraitSet());

        checkSize = logicalModifyViewCopy.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);

        // test oss table scan
        OSSTableScan ossTableScan = new OSSTableScan(b);
        checkSize = ossTableScan.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);
        OSSTableScan ossTableScanCopy = (OSSTableScan) ossTableScan.copy(ossTableScan.getTraitSet());

        checkSize = ossTableScanCopy.getColumnOrigins().size();
        Assert.assertTrue(size == checkSize);
    }

    @Test
    public void testFlashBack() {
        LogicalTableScan scan1 = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));
        org.junit.Assert.assertFalse(RelUtils.containFlashback(scan1));

        scan1.setFlashback(relOptCluster.getRexBuilder().makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP));
        org.junit.Assert.assertTrue(RelUtils.containFlashback(scan1));
    }
}
