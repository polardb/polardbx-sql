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

import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;

import java.util.HashSet;
import java.util.Set;

/**
 * @author chenghui.lch
 */
public class PartFuncMonotonicityUtil {

    protected static Set<SqlOperator> partFuncOp = new HashSet<>();

    static {
        partFuncOp.add(TddlOperatorTable.YEAR);
        partFuncOp.add(TddlOperatorTable.MONTH);
        partFuncOp.add(TddlOperatorTable.TO_DAYS);
        partFuncOp.add(TddlOperatorTable.TO_SECONDS);
        partFuncOp.add(TddlOperatorTable.UNIX_TIMESTAMP);
    }

    public static Monotonicity getPartFuncMonotonicity(SqlOperator op, RelDataType inputDataType) {
        Monotonicity intFunMonotonicity = PartitionPrunerUtils.getPartitionIntFunction(op.getName())
            .getMonotonicity(DataTypeUtil.calciteToDrdsType(inputDataType));
        return intFunMonotonicity;
    }

    /**
     * build the EndPointInfo
     *
     * @return [0]: cmpDirection, [1]: inclEndpoint
     */
    public static boolean[] buildIntervalEndPointInfo(ComparisonKind cmpKind) {

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
