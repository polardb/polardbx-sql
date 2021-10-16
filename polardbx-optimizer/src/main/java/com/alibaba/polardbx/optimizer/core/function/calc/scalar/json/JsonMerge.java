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

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JSONConstants;
import com.alibaba.polardbx.optimizer.json.JsonDocProcessor;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;

/**
 * JSON_MERGE(json_doc, json_doc[, json_doc] ...)
 * <p>
 * Merges two or more JSON documents and returns the merged result.
 * Returns NULL if any argument is NULL. An error occurs if any argument is not a valid JSON document.
 * <p>
 * Merging takes place according to the following rules. For additional information,
 * see Normalization, Merging, and Autowrapping of JSON Values.
 * <p>
 * 1. Adjacent arrays are merged to a single array.
 * 2. Adjacent objects are merged to a single object.
 * 3. A scalar value is autowrapped as an array and merged as an array.
 * 4. An adjacent array and object are merged by autowrapping the object as an array and merging the two arrays.
 *
 * @author wuheng.zxy 2016-5-21 上午11:41:22
 * @author arnkore 2017-08-15
 */
public class JsonMerge extends JsonExtraFunction {
    public JsonMerge(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_MERGE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // Returns NULL if any argument is NULL
        String[] argsStrArray = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            String arg = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            argsStrArray[i] = arg;
            if (arg == null) {
                return JSONConstants.NULL_VALUE;
            }

            try {
                JSON.parse(arg);
            } catch (Exception e) {
                throw JsonParserException.invalidArgInFunc(i + 1, getFunctionNames()[0]);
            }
        }

        return JsonDocProcessor.jsonMerge(argsStrArray);
    }
}
