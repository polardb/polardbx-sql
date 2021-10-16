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

import com.alibaba.druid.util.StringUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.List;

public class WindowAggregateCall extends AggregateCall {
    private final List<RexLiteral> constants;
    private final int offset;

    private WindowAggregateCall(
        SqlAggFunction aggFunction,
        boolean distinct,
        boolean approximate,
        List<Integer> argList,
        int filterArg,
        RelDataType type,
        String name,
        List<RexLiteral> constants,
        int offset) {
        super(aggFunction, distinct, approximate, argList, filterArg, type, name);
        if (constants != null) {
            this.constants = ImmutableList.copyOf(constants);
        } else {
            this.constants = null;
        }
        this.offset = offset;
    }

    public static WindowAggregateCall create(SqlAggFunction aggFunction,
                                             boolean distinct, boolean approximate, List<Integer> argList,
                                             int filterArg, RelDataType type, String name,
                                             List<RexLiteral> constants, int offset) {
        return new WindowAggregateCall(aggFunction, distinct, approximate, argList,
            filterArg, type, name, constants, offset);
    }

    public final List<RexLiteral> getConstants() {
        return constants;
    }

    public WindowAggregateCall copy(List<Integer> args, int filterArg, List<RexLiteral> constants, int offset) {
        return new WindowAggregateCall(getAggregation(), isDistinct(), isApproximate(), args,
            filterArg, type, name, constants, offset);
    }


    public AggregateCall create(SqlAggFunction aggFunction,
                                boolean distinct, boolean approximate, List<Integer> argList,
                                int filterArg, RelDataType type, List<RexLiteral> constants, int offset) {
        return new WindowAggregateCall(aggFunction, distinct, approximate, argList,
            filterArg, type, name, constants, offset);
    }

    public WindowAggregateCall create(boolean distinct, List<Integer> argList, int filterArg, int groupCount,
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
        return new WindowAggregateCall(aggFunction, distinct, isApproximate(), argList,
            filterArg, type, name, constants, offset);
    }

    private int getIntConstant(int offset) {
        int index = offset - this.offset;
        if (index < 0) {
            return -1;
        }
        RexLiteral rexLiteral = constants
            .get(index);
        Object value2 = rexLiteral.getValue2();
        if (!StringUtils.isNumber(String.valueOf(value2))) {
            throw new IllegalArgumentException("must be a digit, but was " + value2.toString() + ".");
        }
        return StringUtils.stringToInteger(
            String.valueOf(value2));
    }

    public int getNTHValueOffset(int offset) {
        return getIntConstant(offset);
    }

    public int getNTileOffset(int offset) {
        return getIntConstant(offset);
    }

    public int getLagLeadOffset(int offset) {
        return getIntConstant(offset);
    }

    public String getLagLeadDefaultValue(int offset) {
        int index = offset - this.offset;
        if (index < 0) {
            return null;
        }
        RexLiteral rexLiteral = constants
            .get(index);
        Object value2 = rexLiteral.getValue2();
        return value2.toString();
    }
}
