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

import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Returns active roles for current session.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_current-role">current
 * role</a>
 * @since 5.4.9
 */
public class CurrentRole extends AbstractScalarFunction {
    @Override
    public String[] getFunctionNames() {
        return new String[] {"CURRENT_ROLE"};
    }

    public CurrentRole(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, DataTypes.StringType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        PrivilegeContext privilegeContext = ec.getPrivilegeContext();
        String ret = PolarPrivManager.getInstance().getCurrentRole(privilegeContext.getPolarUserInfo().getAccountId(),
            privilegeContext.getActiveRoles()).stream()
            .map(PolarAccount::getIdentifier)
            .sorted()
            .collect(Collectors.joining(", "));

        if (ret.isEmpty()) {
            ret = "NONE";
        }

        return ret;
    }
}
