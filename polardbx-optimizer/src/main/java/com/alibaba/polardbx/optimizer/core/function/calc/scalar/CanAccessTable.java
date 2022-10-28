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

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PermissionCheckContext;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author dylan
 */
public class CanAccessTable extends AbstractScalarFunction {

    public static final String NAME = "CAN_ACCESS_TABLE";

    public CanAccessTable(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {NAME};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object argv : args) {
            if (FunctionUtils.isNull(argv)) {
                return null;
            }
        }

        String schemaName = DataTypes.StringType.convertFrom(args[0]);
        String tableName = DataTypes.StringType.convertFrom(args[1]);
        return verifyPrivileges(schemaName, tableName, ec);
    }

    public static boolean verifyPrivileges(String schemaName, String tableName, ExecutionContext executionContext) {
        if (!executionContext.isPrivilegeMode()) {
            return true;
        }

        // skip privilege check in mock mode
        if (ConfigDataMode.isFastMock()) {
            return true;
        }

        tableName = TStringUtil.normalizePriv(SQLUtils.normalizeNoTrim(tableName));

        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (TStringUtil.isBlank(schemaName)) {
            schemaName = pc.getSchema();
        }
        schemaName = (schemaName == null) ? "" : schemaName;
        schemaName = TStringUtil.normalizePriv(SQLUtils.normalizeNoTrim(schemaName));

        if (TStringUtil.equalsIgnoreCase(TddlConstants.INFORMATION_SCHEMA, schemaName)) {
            return false;
        }

        Permission permission = Permission.anyPermission(schemaName, tableName);
        PermissionCheckContext context = new PermissionCheckContext(pc.getPolarUserInfo().getAccountId(),
            pc.getActiveRoles(), permission);
        return PolarPrivManager.getInstance().checkPermission(context);
    }
}
