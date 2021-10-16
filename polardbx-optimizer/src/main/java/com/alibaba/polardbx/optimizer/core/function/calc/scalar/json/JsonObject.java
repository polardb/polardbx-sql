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
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.collect.Maps;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;

import java.util.List;
import java.util.Map;

/**
 * JSON_OBJECT([key, val[, key, val] ...])
 * <p>
 * Evaluates a (possibly empty) list of key-value pairs and returns a JSON object containing those pairs.
 * An error occurs if any key name is NULL or the number of arguments is odd.
 *
 * @author wuheng.zxy 2016-5-21 上午11:42:42
 * @author arnkore 2017-08-15
 */
public class JsonObject extends JsonExtraFunction {
    public JsonObject(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_OBJECT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length % 2 != 0) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }

        Map<String, Object> resultJson = Maps.newHashMap();
        for (int i = 0; i < args.length; i += 2) {
            if (args[i] == null) {
                throw new JsonParserException("JSON documents may not contain NULL member names.");
            }

            if (i + 1 < args.length) {
                resultJson.put(DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]), args[i + 1]);
            } else {
                throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
            }
        }

        return JSON.toJSONString(resultJson);
    }
}
