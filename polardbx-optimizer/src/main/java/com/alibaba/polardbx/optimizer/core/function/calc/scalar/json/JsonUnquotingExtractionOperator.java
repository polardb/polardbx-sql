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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.json;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonUtil;

import java.util.List;

/**
 * JSON unquoting extraction operator
 * <p>
 * This is an improved, unquoting extraction operator available in MySQL 5.7.13 and later.
 * Whereas the -> operator simply extracts a value, the ->> operator in addition unquotes the extracted result.
 * In other words, given a JSON column value column and a path expression path,
 * the following three expressions return the same value:
 * 1. JSON_UNQUOTE( JSON_EXTRACT(column, path) )
 * 2. JSON_UNQUOTE(column -> path)
 * 3. column->>path
 * <p>
 * The ->> operator can be used wherever JSON_UNQUOTE(JSON_EXTRACT()) would be allowed.
 * This includes (but is not limited to) SELECT lists, WHERE and HAVING clauses, and ORDER BY and GROUP BY clauses.
 *
 * @author arnkore 2017-07-18 18:02
 */
public class JsonUnquotingExtractionOperator extends JsonExtractionOperator {
    public JsonUnquotingExtractionOperator(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"->>"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        Object obj = super.compute(args, ec);
        String str = DataTypes.StringType.convertFrom(obj);
        return JsonUtil.unquote(str);
    }
}
