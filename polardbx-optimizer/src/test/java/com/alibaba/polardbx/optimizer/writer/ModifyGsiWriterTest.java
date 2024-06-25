package com.alibaba.polardbx.optimizer.writer;

import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.BroadcastModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ShardingModifyWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyGsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.SingleModifyWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.apache.calcite.rel.core.TableModify.Operation.UPDATE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ModifyGsiWriterTest {

    @Test
    public void testBroadcastModifyGsiWriter() {
        RelOptTable targetTable = mock(RelOptTable.class);
        BroadcastModifyWriter updateWriter = mock(BroadcastModifyWriter.class);
        BroadcastModifyWriter deleteWriter = mock(BroadcastModifyWriter.class);
        TableMeta gsiMeta = mock(TableMeta.class);

        List<RelNode> resUpdate = new ArrayList<>();
        resUpdate.add(mock(RelNode.class));
        List<RelNode> resDelete = new ArrayList<>();

        when(updateWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resUpdate);
        when(deleteWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resDelete);

        BroadcastModifyGsiWriter writer =
            new BroadcastModifyGsiWriter(targetTable, UPDATE, updateWriter, deleteWriter, gsiMeta);

        try (MockedStatic<GlobalIndexMeta> mockedStatic = Mockito.mockStatic(GlobalIndexMeta.class)) {
            when(GlobalIndexMeta.canWrite(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(false);
            when(GlobalIndexMeta.canDelete(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(true);

            List<RelNode> res = writer.getInput(mock(ExecutionContext.class), mock(Function.class));
            Assert.assertEquals(1, res.size());
        }
    }

    @Test
    public void testSingleModifyGsiWriter() {
        RelOptTable targetTable = mock(RelOptTable.class);
        SingleModifyWriter updateWriter = mock(SingleModifyWriter.class);
        SingleModifyWriter deleteWriter = mock(SingleModifyWriter.class);
        TableMeta gsiMeta = mock(TableMeta.class);

        List<RelNode> resUpdate = new ArrayList<>();
        resUpdate.add(mock(RelNode.class));
        List<RelNode> resDelete = new ArrayList<>();

        when(updateWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resUpdate);
        when(deleteWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resDelete);

        SingleModifyGsiWriter writer =
            new SingleModifyGsiWriter(targetTable, UPDATE, updateWriter, deleteWriter, gsiMeta);

        try (MockedStatic<GlobalIndexMeta> mockedStatic = Mockito.mockStatic(GlobalIndexMeta.class)) {
            when(GlobalIndexMeta.canWrite(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(false);
            when(GlobalIndexMeta.canDelete(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(true);

            List<RelNode> res = writer.getInput(mock(ExecutionContext.class), mock(Function.class));
            Assert.assertEquals(1, res.size());
        }
    }

    @Test
    public void testShardingModifyGsiWriter() {
        RelOptTable targetTable = mock(RelOptTable.class);
        ShardingModifyWriter updateWriter = mock(ShardingModifyWriter.class);
        ShardingModifyWriter deleteWriter = mock(ShardingModifyWriter.class);
        TableMeta gsiMeta = mock(TableMeta.class);

        List<RelNode> resUpdate = new ArrayList<>();
        resUpdate.add(mock(RelNode.class));
        List<RelNode> resDelete = new ArrayList<>();

        when(updateWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resUpdate);
        when(deleteWriter.getInput(any(ExecutionContext.class), any())).thenReturn(resDelete);

        ShardingModifyGsiWriter writer =
            new ShardingModifyGsiWriter(targetTable, UPDATE, updateWriter, deleteWriter, gsiMeta);

        try (MockedStatic<GlobalIndexMeta> mockedStatic = Mockito.mockStatic(GlobalIndexMeta.class)) {
            when(GlobalIndexMeta.canWrite(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(false);
            when(GlobalIndexMeta.canDelete(any(ExecutionContext.class), any(TableMeta.class))).thenReturn(true);

            List<RelNode> res = writer.getInput(mock(ExecutionContext.class), mock(Function.class));
            Assert.assertEquals(1, res.size());
        }
    }
}
