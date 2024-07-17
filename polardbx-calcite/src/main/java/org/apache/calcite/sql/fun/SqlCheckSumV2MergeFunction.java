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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * @author yaozhili
 */
public class SqlCheckSumV2MergeFunction extends SqlAggFunction {
    public SqlCheckSumV2MergeFunction() {
        super(
            "CHECK_SUM_V2_MERGE",
            null,
            SqlKind.CHECK_SUM_V2_MERGE,
            ReturnTypes.BIGINT,
            null,
            OperandTypes.ONE_OR_MORE,
            SqlFunctionCategory.NUMERIC,
            false,
            false);
    }

    @Override public <T> T unwrap(Class<T> clazz) {
        if (clazz == SqlSplittableAggFunction.class) {
            return clazz.cast(SqlSplittableAggFunction.CountSplitter.INSTANCE);
        }
        return super.unwrap(clazz);
    }
}

