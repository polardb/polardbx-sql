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

package org.apache.calcite.rel.core;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RexExplainVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

public class GroupConcatAggregateCall extends AggregateCall {
    private final ImmutableList<Integer> orderList;
    private final String separator;
    private final ImmutableList<String> ascOrDescList;

    private GroupConcatAggregateCall(
        SqlAggFunction aggFunction,
        boolean distinct,
        boolean approximate,
        List<Integer> argList,
        int filterArg,
        RelDataType type,
        String name,
        List<Integer> orderList,
        String separator,
        List<String> ascOrDescList) {
        super(aggFunction, distinct, approximate, argList, filterArg, type, name);
        if (orderList != null) {
            this.orderList = ImmutableList.copyOf(orderList);
        } else {
            this.orderList = null;
        }
        this.separator = separator;
        this.ascOrDescList = ImmutableList.copyOf(ascOrDescList);
    }

    public static GroupConcatAggregateCall create(SqlAggFunction aggFunction,
                                                  boolean distinct, boolean approximate, List<Integer> argList,
                                                  int filterArg, RelDataType type, String name,
                                                  List<Integer> orderList, String separator,
                                                  List<String> ascOrDescList) {
        return new GroupConcatAggregateCall(aggFunction, distinct, approximate, argList,
            filterArg, type, name, orderList, separator, ascOrDescList);
    }

    public final List<Integer> getOrderList() {
        return orderList;
    }

    public final String getSeparator() {
        return separator;
    }

    public final List<String> getAscOrDescList() {
        return ascOrDescList;
    }

    public GroupConcatAggregateCall copy(List<Integer> args, int filterArg, List<Integer> orderList) {
        return new GroupConcatAggregateCall(getAggregation(), isDistinct(), isApproximate(), args,
            filterArg, type, name, orderList, separator, ascOrDescList);
    }

    @Override
    public GroupConcatAggregateCall copy(List<Integer> args, int filterArg) {
        return new GroupConcatAggregateCall(aggFunction, distinct, approximate, args,
            filterArg, type, name, orderList, separator, ascOrDescList);
    }

    @Override
    public GroupConcatAggregateCall copy(List<Integer> args, int filterArg, boolean isDistinct, String newName) {
        return new GroupConcatAggregateCall(aggFunction, isDistinct, approximate, args,
            filterArg, type, newName, orderList, separator, ascOrDescList);
    }

    @Override
    public AggregateCall withDistinct(boolean distinct) {
        return distinct == this.distinct ? this
            : new GroupConcatAggregateCall(aggFunction, distinct, approximate, argList, filterArg, type, name,
            orderList, separator, ascOrDescList);
    }

    @Override
    public AggregateCall adaptTo(RelNode input, List<Integer> argList, int filterArg, int oldGroupKeyCount,
                                 int newGroupKeyCount) {
        // The return type of aggregate call need to be recomputed.
        // Since it might depend on the number of columns in GROUP BY.
        final RelDataType newType =
            oldGroupKeyCount == newGroupKeyCount
                && argList.equals(this.getArgList())
                && filterArg == this.filterArg
                ? type
                : null;

        return create(isDistinct(), argList, filterArg, newGroupKeyCount, input, newType);
    }

    public AggregateCall create(SqlAggFunction aggFunction,
                                       boolean distinct, boolean approximate, List<Integer> argList,
                                       int filterArg, RelDataType type, List<Integer> orderList) {
        return new GroupConcatAggregateCall(aggFunction, distinct, approximate, argList,
            filterArg, type, name, orderList, separator, ascOrDescList);
    }

    public GroupConcatAggregateCall create(boolean distinct, List<Integer> argList, int filterArg, int groupCount,
                                           RelNode input, RelDataType type) {
        SqlAggFunction aggFunction = getAggregation();
        if (type == null) {
            final RelDataTypeFactory typeFactory =
                input.getCluster().getTypeFactory();
            final List<RelDataType> types =
                SqlTypeUtil.projectTypes(input.getRowType(), argList);
            final Aggregate.AggCallBinding callBinding =
                new Aggregate.AggCallBinding(typeFactory, aggFunction, types,
                    groupCount, filterArg >= 0);
            type = aggFunction.inferReturnType(callBinding);
        }
        return new GroupConcatAggregateCall(aggFunction, distinct, isApproximate(), argList,
            filterArg, type, name, orderList, separator, ascOrDescList);
    }

    public String toString() {
        StringBuilder buf = new StringBuilder(getAggregation().getName());
        buf.append("(");
        if (isDistinct()) {
            buf.append((getArgList().size() == 0) ? "DISTINCT" : "DISTINCT ");
        }
        int i = -1;
        for (Integer arg : getArgList()) {
            if (++i > 0) {
                buf.append(", ");
            }
            buf.append("$");
            buf.append(arg);
        }

        if (orderList != null && orderList.size() != 0) {
            buf.append(" ORDER BY ");
            for (i = 0; i < orderList.size(); i++) {
                Integer arg = orderList.get(i);
                String ascOrDesc = ascOrDescList.get(i);
                if (i != 0) {
                    buf.append(", ");
                }
                buf.append("$");
                buf.append(arg);
                buf.append(" ");
                buf.append(ascOrDesc);
            }
        }

        if (separator != null) {
            buf.append(" SEPARATOR '");
            buf.append(separator);
            buf.append("'");
        }

        buf.append(")");
        if (hasFilter()) {
            buf.append(" FILTER $");
            buf.append(filterArg);
        }
        return buf.toString();
    }

    public void accept(RexExplainVisitor visitor) {
        visitor.visit(this);
    }
}
