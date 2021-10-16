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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * SUBTIME() returns expr1 – expr2 expressed as a value in the same format as
 * expr1. expr1 is a time or datetime expression, and expr2 is a time
 * expression.
 *
 * <pre>
 * mysql> SELECT SUBTIME('2007-12-31 23:59:59','1:1:1');
 *         -> '2007-12-30 22:58:58.999997'
 * </pre>
 *
 * @author jianghang 2014-4-16 下午11:29:18
 * @since 5.0.7
 */
public class SubTime extends AddTime {
    public SubTime(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    /**
     * The sign of function, 1 means add and -1 means sub.
     */
    @Override
    protected int getInitialSign() {
        return -1;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"SUBTIME"};
    }
}
