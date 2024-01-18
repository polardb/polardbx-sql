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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * @author jianghang 2014-4-15 上午11:30:52
 * @since 5.0.7
 */
public class LastInsertId extends AbstractScalarFunction {

    public static String NAME = "LAST_INSERT_ID";

    public LastInsertId(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        // MPP doesn't support LAST_INSERT_ID with parameters.
        if (args.length > 0 && !ec.getConnection().isMppConnection()) {
            if (args.length != 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_FUNCTION, "param invalid");
            }
            if (args[0] == null) {
                ec.getConnection().setLastInsertId(0);
                ec.getConnection().setReturnedLastInsertId(0);
            } else {
                ec.getConnection().setLastInsertId(new Long(args[0].toString()));
                ec.getConnection().setReturnedLastInsertId(new Long(args[0].toString()));
            }
        }
        return ec.getConnection().getLastInsertId();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {NAME};
    }
}
