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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlDuration;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.executor.mdl.MdlRequest;
import com.alibaba.polardbx.executor.mdl.MdlTicket;
import com.alibaba.polardbx.executor.mdl.MdlType;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.Setter;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
public class GlobalAcquireMdlLockInDbSyncAction implements ISyncAction {
    final private Set<String> schemaNames;

    public GlobalAcquireMdlLockInDbSyncAction(Set<String> schemaNames) {
        this.schemaNames = schemaNames;
    }

    @Override
    public ResultCursor sync() {
        for (String schema : schemaNames) {
            acquireAllTablesMdlWriteLockInDb(schema);
        }
        return null;
    }

    protected void acquireAllTablesMdlWriteLockInDb(String schemaName) {
        final MdlContext mdlContext = MdlManager.addContext(schemaName, true);

        SchemaManager schemaManager = OptimizerContext.getContext(schemaName).getLatestSchemaManager();

        Set<String> tableNames = getAllTablesInDatabase(schemaName);
        for (String tableName : tableNames) {
            TableMeta currentMeta = schemaManager.getTableWithNull(tableName);
            SQLRecorderLogger.ddlLogger.warn(MessageFormat.format(
                "Mdl {0}  {1}.addContext({2})", Thread.currentThread().getName(), this.hashCode(), schemaName));

            long startMillis = System.currentTimeMillis();

            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0} {1}] Mdl write lock try to acquired table[{2}]",
                Thread.currentThread().getName(), this.hashCode(), currentMeta.getDigest()));

            MdlTicket ticket = mdlContext.acquireLock(
                new MdlRequest(1L,
                    MdlKey.getTableKeyWithLowerTableName(schemaName, currentMeta.getDigest()),
                    MdlType.MDL_EXCLUSIVE,
                    MdlDuration.MDL_TRANSACTION)
            );

            long elapsedMillis = System.currentTimeMillis() - startMillis;
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "[{0} {1}] Mdl write lock acquired table[{2}] cost {3}ms",
                Thread.currentThread().getName(), this.hashCode(), currentMeta.getDigest(), elapsedMillis));
        }
    }

    protected Set<String> getAllTablesInDatabase(String schemaName) {
        final String queryTablesSql = "show tables";
        List<Map<String, Object>> result = DdlHelper.getServerConfigManager().executeQuerySql(queryTablesSql,
            schemaName,
            null);
        Set<String> tables = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Map<String, Object> map : result) {
            String tb = (String) map.get(("TABLES_IN_" + SQLUtils.normalize(schemaName)).toUpperCase());
            if (tb != null) {
                tables.add(tb);
            }
        }
        return tables;
    }
}
