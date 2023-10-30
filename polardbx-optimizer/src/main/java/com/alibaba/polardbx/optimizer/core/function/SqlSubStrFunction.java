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

package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/**
 * Created by chuanqin on 17/4/10.
 */
public class SqlSubStrFunction extends SqlFunction {
    private int position;
    private int length;

    public SqlSubStrFunction(String funcName) {
        super(funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.FIRST_STRING_TYPE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING);
    }

    @Override
    public Object clone() {
        SqlSubStrFunction function = new SqlSubStrFunction(this.getName());
        function.setPosition(this.position);
        function.setLength(this.length);
        return function;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }
}
