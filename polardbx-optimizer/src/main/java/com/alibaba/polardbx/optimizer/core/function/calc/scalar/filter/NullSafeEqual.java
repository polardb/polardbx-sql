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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * Created by simiao.zw on 2014/7/18.
 */
public class NullSafeEqual extends AbstractCollationScalarFunction {
    public NullSafeEqual(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        boolean isAnull = FunctionUtils.isNull(args[0]);
        boolean isBnull = FunctionUtils.isNull(args[1]);
        if (isAnull && isBnull) {
            return 1L;
        } else if (isAnull || isBnull) {
            return 0L;
        } else {
            // Row value compare.
            DataType type = getCompareType();
            return type.compare(args[0], args[1]) == 0 ? 1l : 0l;
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"<=>"};
    }
}
