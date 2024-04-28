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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.lbac;

import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LBACCheck extends AbstractScalarFunction {

    public LBACCheck(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LBAC_CHECK"};
    }

    /**
     * 参数类型为两种
     * 1. ('read','labelName1','labelName2')
     * 2. ('write','labelName')
     */
    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        boolean read = "read".
            equalsIgnoreCase(DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]));
        long result;
        if (args.length == 3) {
            if (args[2] == null) {
                return 1L;
            }
            if (args[1] == null) {
                return 0L;
            }
            String labelName1 = DataTypeUtil.convert(getOperandType(1), DataTypes.StringType, args[1]);
            String labelName2 = DataTypeUtil.convert(getOperandType(2), DataTypes.StringType, args[2]);
            result = LBACPrivilegeCheckUtils.checkSecurityLabelRW(labelName1, labelName2, read) ?
                1L : 0L;
        } else {
            if (args[1] == null) {
                return 1L;
            }
            String labelName2 = DataTypeUtil.convert(getOperandType(1), DataTypes.StringType, args[1]);
            result = LBACPrivilegeCheckUtils.checkUserRW(
                ec.getPrivilegeContext().getPolarUserInfo().getAccount(), labelName2, read) ?
                1L : 0L;
        }

        return result;
    }

}
