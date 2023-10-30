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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;

/**
 * @author chenghui.lch
 */
public class PartFuncMonotonicityUtil {

    public static Monotonicity getPartFuncMonotonicity(PartitionIntFunction partFn, RelDataType inputDataType) {
        Monotonicity intFunMonotonicity = partFn.getMonotonicity(DataTypeUtil.calciteToDrdsType(inputDataType));
        return intFunMonotonicity;
    }

    /**
     * build the EndPointInfo
     * <pre>
     *     endpoints[0] means cmpDirection:
     *      if endpoints[0]=false,
     *          that means constExpr is NOT the leftEndPoint of a range, such as partCol < const or partCol <= const;
     *     if endpoints[0]=true,
     *          that means constExpr is the leftEndPoint of a range, such as constExpr < partCol or constExpr <= partCol;
     *     endpoints[1] means if endpoint is included:
     *      if endpoints[1]=false,
     *          that means constExpr should NOT be included, such as partCol < const or  partCol > const;
     *     if endpoints[1]=true,
     *          that means constExpr should be included, such as partCol <= const or  partCol >= const;
     * </pre>
     *
     * @return [0]: cmpDirection, [1]: inclEndpoint
     */
    public static boolean[] buildIntervalEndPointInfo(ComparisonKind cmpKind) {

        /**
         * <pre>
         * leftEndPoint=true  <=>  const < col or const <= col, so const is the left end point,
         * leftEndPoint=false <=>  col < const or col <= const, so const is NOT the left end point,
         *
         * includeEndPoint=true <=> const <= col or col <= const
         * includeEndPoint=false <=> const < col or col < const
         * </pre>
         *
         */

        boolean[] endpoints = new boolean[2];
        switch (cmpKind) {
        case LESS_THAN: {
            endpoints[0] = false;
            endpoints[1] = false;
        }
        break;
        case LESS_THAN_OR_EQUAL: {
            endpoints[0] = false;
            endpoints[1] = true;
        }
        break;
        case GREATER_THAN: {
            endpoints[0] = true;
            endpoints[1] = false;
        }
        break;
        case GREATER_THAN_OR_EQUAL: {
            endpoints[0] = true;
            endpoints[1] = true;
        }
        break;
        case EQUAL: {
            return null;
        }
        }
        return endpoints;
    }

    /**
     * <pre>
     * enddpoint[0]=true  <=>  const < col or const <= col, so const is the left end point,
     * enddpoint[0]=false <=>  col < const or col <= const, so const is NOT the left end point,
     *
     * enddpoint[1]=true <=> const <= col or col <= const
     * enddpoint[1]=false <=> const < col or col < const
     * </pre>
     */
    public static ComparisonKind buildComparisonKind(boolean[] enddpoints) {

        ComparisonKind finalCmp = null;

        if (enddpoints == null) {
            return ComparisonKind.EQUAL;
        }

        boolean cmpDirection = enddpoints[0];
        boolean inclEp = enddpoints[1];

        if (!cmpDirection) {
            if (!inclEp) {
                finalCmp = ComparisonKind.LESS_THAN;
            } else {
                finalCmp = ComparisonKind.LESS_THAN_OR_EQUAL;
            }
        } else {
            if (!inclEp) {
                finalCmp = ComparisonKind.GREATER_THAN;
            } else {
                finalCmp = ComparisonKind.GREATER_THAN_OR_EQUAL;
            }
        }

        return finalCmp;
    }
}
