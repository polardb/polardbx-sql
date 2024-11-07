package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.clearspring.analytics.util.Lists;
import io.airlift.slice.Slice;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class RexUtilsTest {
    @Test
    public void testIndexHintSerialization() {
        List<String> testLines = Lists.newArrayList();
        testLines.add("FORCE INDEXIDX_NAME");
        testLines.add("IGNORE INDEXIDXID");
        testLines.add("use INDEXIDXID1,name1,name2");

        SqlNode node = RexUtil.deSeriIndexHint(testLines);
        System.out.println(node);
        assert node instanceof SqlNodeList;
        assert ((SqlNodeList) node).getList().size() == 3;
        String rs = node.toSqlString(MysqlSqlDialect.DEFAULT).toString().toUpperCase(Locale.ROOT);
        assert rs.contains("FORCE INDEX(IDX_NAME)");
        assert rs.contains("IGNORE INDEX(IDXID)");
        assert rs.contains("USE INDEX(IDXID1, NAME1, NAME2)");
    }

    @Test
    public void testHandleDefaultExpr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexNode rexNode = mock(RexNode.class);
                Slice slice = mock(Slice.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexNode));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexNode)).thenReturn(slice);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);
                when(slice.getBytes()).thenReturn(new byte[] {1, 2, 3});

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(evalFunc).apply(rexNode);
                verify(slice).getBytes();

                ByteString str = mock(ByteString.class);
                when(str.getBytes()).thenReturn(new byte[] {1, 2, 3});
                when(evalFunc.apply(rexNode)).thenReturn(str);
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
                verify(str).getBytes();
            }
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void testHandleDefaultExprThrowErr() {
        try (MockedStatic<RexUtils> staticRexUtils = mockStatic(RexUtils.class)) {
            try (MockedStatic<InstanceVersion> staticInstance = mockStatic(InstanceVersion.class)) {
                TableMeta tableMeta = mock(TableMeta.class);
                LogicalInsert insert = mock(LogicalInsert.class);
                ExecutionContext ec = mock(ExecutionContext.class);
                ColumnMeta columnMeta = mock(ColumnMeta.class);
                RexCall rexCall = mock(RexCall.class);
                RexNode rexNode = mock(RexNode.class);
                Function<RexNode, Object> evalFunc = mock(Function.class);
                List<RexNode> operands = mock(List.class);

                staticInstance.when(() -> InstanceVersion.isMYSQL80()).thenReturn(true);
                when(tableMeta.hasDefaultExprColumn()).thenReturn(true);
                when(insert.getDefaultExprColRexNodes()).thenReturn(Collections.singletonList(rexCall));
                when(insert.getDefaultExprColMetas()).thenReturn(Collections.singletonList(columnMeta));
                when(rexCall.getOperands()).thenReturn(operands);
                when(operands.get(0)).thenReturn(rexNode);
                when(rexNode.toString()).thenReturn("UUID_TO_BIN");

                staticRexUtils.when(() -> RexUtils.getEvalFunc(any(ExecutionContext.class))).thenReturn(evalFunc);
                when(evalFunc.apply(rexCall)).thenThrow(UnsupportedOperationException.class);

                when(RexUtils.isBinaryReturnType(any(RexNode.class))).thenReturn(true);

                staticRexUtils.when(() -> RexUtils.handleDefaultExpr(any(TableMeta.class), any(LogicalInsert.class),
                    any(ExecutionContext.class))).thenCallRealMethod();
                RexUtils.handleDefaultExpr(tableMeta, insert, ec);
            }
        }
    }

}
