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

package org.apache.calcite.sql.fun;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.List;

public class SqlRuntimeFilterBuildFunction extends SqlFunction {

    private final List<Integer> runtimeFilterIds;
    private double ndv;

    public SqlRuntimeFilterBuildFunction(List<Integer> runtimeFilterIds, double ndv) {
        super(
            "RUNTIME_FILTER_BUILDER_" + runtimeFilterIds,
            SqlKind.RUNTIME_FILTER_BUILD,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.family(SqlTypeFamily.ANY),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        this.runtimeFilterIds = runtimeFilterIds;
        this.ndv = ndv;
    }

    public List<Integer> getRuntimeFilterIds() {
        return runtimeFilterIds;
    }

    public double getNdv() {
        return ndv;
    }

    public void updateNdv(double ndv) {
        this.ndv = ndv;
    }

}
