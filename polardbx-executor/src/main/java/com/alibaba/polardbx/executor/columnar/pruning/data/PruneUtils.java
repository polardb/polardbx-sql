/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.columnar.pruning.data;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.AndColumnPredicate;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnarPredicatePruningVisitor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.utils.RexLiteralTypeUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import io.airlift.slice.Slice;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.roaringbitmap.RoaringBitmap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author fangwu
 */
public class PruneUtils {
    public static RoaringBitmap and(long rgNum, Iterator<RoaringBitmap> rbs) {
        return RoaringBitmap.and(rbs, 0, rgNum);
    }

    public static RoaringBitmap or(long rgNum, Iterator<RoaringBitmap> rbs) {
        return RoaringBitmap.or(rbs, 0, rgNum);
    }

    public static ColumnPredicatePruningInf transformRexToIndexMergeTree(RexNode rex, IndexPruneContext ipc) {
        return rex.accept(new ColumnarPredicatePruningVisitor(ipc));
    }

    public static ColumnPredicatePruningInf transformRexToIndexMergeTree(List<RexNode> rexList, IndexPruneContext ipc) {
        if (rexList == null || rexList.size() == 0) {
            return null;
        }
        ColumnarPredicatePruningVisitor columnarPredicatePruningVisitor = new ColumnarPredicatePruningVisitor(ipc);
        AndColumnPredicate and = new AndColumnPredicate();
        and.addAll(rexList.stream().map(r -> r.accept(columnarPredicatePruningVisitor))
            .collect(Collectors.toList()));
        return and.flat();
    }

    public static String display(ColumnPredicatePruningInf columnPredicate, List<ColumnMeta> columns,
                                 IndexPruneContext indexPruneContext) {
        if (columnPredicate == null) {
            return "null";
        }
        return columnPredicate.display(
            columns.stream()
                .map(cm -> cm.getName())
                .collect(Collectors.toList())
                .toArray(new String[0]),
            indexPruneContext).toString();
    }

    public static Object getValueFromRexNode(RexNode rexNode, IndexPruneContext ipc) {
        final Map<Integer, ParameterContext> rowParameters = ipc.getParameters().getCurrentParameter();
        ExecutionContext ec = new ExecutionContext();
        ec.setParams(ipc.getParameters());
        Object result = null;
        if (rexNode instanceof RexDynamicParam) {
            final int valueIndex = ((RexDynamicParam) rexNode).getIndex();
            // compatible with JDBC expression
            ParameterContext parameterContext = rowParameters.get(valueIndex + 1);
            if (parameterContext != null) {
                result = parameterContext.getValue();
            }
        } else if (rexNode instanceof RexLiteral) {
            result = RexLiteralTypeUtils.getJavaObjectFromRexLiteral((RexLiteral) rexNode, true);
        } else {
            final Object value = RexUtils.buildRexNode(rexNode, ec).eval(null);
            if (value instanceof Decimal) {
                result = ((Decimal) value).toBigDecimal();
            } else if (value instanceof Slice) {
                if (RexUtils.isBinaryReturnType(rexNode)) {
                    result = ((Slice) value).getBytes();
                } else {
                    result = ((Slice) value).toString(CharsetName.DEFAULT_STORAGE_CHARSET_IN_CHUNK);
                }
            } else {
                result = value;
            }
        }
        return result;
    }

    public interface TriFunction<T, U, V, R> {
        R apply(T var1, U var2, V var3);
    }

    public interface FourFunction<T, U, V, W> {
        void apply(T var1, U var2, V var3, W var4);
    }
}
