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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.sync.ClearPlanCacheSyncAction;
import com.alibaba.polardbx.executor.sync.BaselineInvalidatePlanSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

public class DdlUtils {

    private static final Logger logger = LoggerFactory.getLogger(DdlUtils.class);

    /**
     * 这个是最简的规则文件，prectrl也是在代码中当第一次分库分表时将这个给出来的，借来用
     */
    private static final String voidRuleString = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<!DOCTYPE beans PUBLIC \"-//SPRING//DTD BEAN//EN\" \"http://www.springframework.org/dtd/spring-beans.dtd\">"
        + "<beans>"
        + "<bean id=\"vtabroot\" class=\"com.taobao.tddl.interact.rule.VirtualTableRoot\" init-method=\"init\">"
        + "<property name=\"dbType\" value=\"MYSQL\" />"
        + "<property name=\"defaultDbIndex\" value=\"???\" />"
        + "<property name=\"tableRules\">" + "<map>"
        + "</map>" + "</property>" + "</bean>" + "</beans>";

    public static final String voidRuleVersoin = "V0";

    /**
     * invalidate plan cache in spm by schema and table
     */
    public static void invalidatePlan(PhyDdlTableOperation ddl, String schemaName) {
        String tableName = ddl.getLogicalTableName();
        switch (ddl.getKind()) {
        case DROP_TABLE:
            DdlUtils.invalidatePlan(schemaName, tableName, true);
            break;
        case DROP_INDEX:
        case RENAME_TABLE:
        case ALTER_TABLE:
        case CREATE_INDEX:
            DdlUtils.invalidatePlan(schemaName, tableName, false);
            break;
        case TRUNCATE_TABLE:
        case CREATE_TABLE:
        default:
            // Nothing to do with other DDLs.
            break;
        }
    }

    public static void invalidatePlan(String schema, String table, boolean isForce) {
        SyncManagerHelper.syncWithDefaultDB(new BaselineInvalidatePlanSyncAction(schema, table, isForce),
            SyncScope.ALL);
    }

    public static void invalidatePlanCache(String schema, String table) {
        SyncManagerHelper.syncWithDefaultDB(new ClearPlanCacheSyncAction(schema, table), SyncScope.ALL);
    }

    /**
     * Generate ddl version id
     */
    public static long generateVersionId(ExecutionContext ec) {
        final ITimestampOracle timestampOracle =
            ec.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }
        return timestampOracle.nextTimestamp();
    }

}
