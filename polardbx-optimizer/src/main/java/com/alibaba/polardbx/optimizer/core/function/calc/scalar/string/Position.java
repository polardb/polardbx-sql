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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * <pre>
 * POSITION(substr IN str)
 *
 * POSITION(substr IN str) is a synonym for LOCATE(substr,str).
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午2:37:28
 * @since 5.1.0
 */
public class Position extends Locate {
    public Position(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {

        return new String[] {"POSITION"};
    }
}
