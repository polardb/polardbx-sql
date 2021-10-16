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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * JsonExtract的重载符
 * <p>
 * In MySQL 5.7.9 and later, the -> operator serves as an alias for the JSON_EXTRACT() function when used with two arguments,
 * a column identifier on the left and a JSON path on the right that is evaluated against the JSON document (the column value).
 * You can use such expressions in place of column identifiers wherever they occur in SQL statements.
 *
 * @author arnkore 2017-07-18 17:09
 */
public class JsonExtractionOperator extends JsonExtract {
    public JsonExtractionOperator(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    private static final Logger logger = LoggerFactory.getLogger(JsonExtractionOperator.class);

    @Override
    public String[] getFunctionNames() {
        return new String[] {"->"};
    }
}
