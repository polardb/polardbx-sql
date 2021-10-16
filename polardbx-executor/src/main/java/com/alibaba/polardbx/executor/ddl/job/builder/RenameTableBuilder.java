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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.RenameTablePreparedData;
import org.apache.calcite.rel.core.DDL;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class RenameTableBuilder extends DdlPhyPlanBuilder {

    protected final RenameTablePreparedData preparedData;

    public RenameTableBuilder(DDL ddl, RenameTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    public static RenameTableBuilder create(DDL ddl,
                                            RenameTablePreparedData preparedData,
                                            ExecutionContext executionContext) {
        String schemaName = preparedData.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        return DbInfoManager.getInstance().isNewPartitionDb(schemaName) ?
            new RenamePartitionTableBuilder(ddl, preparedData, executionContext) :
            new RenameTableBuilder(ddl, preparedData, executionContext);
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
        fillRenamePhyTable(tableTopology, preparedData.getNewTableName(), preparedData.getTableName());
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    protected void fillRenamePhyTable(Map<String, List<List<String>>> targetDBs, String toTableName,
                                      String logicalTableName) {
        final Set<String> logicalNames = targetDBs.keySet();
        for (String logName : logicalNames) {
            final List<List<String>> lists = targetDBs.get(logName);
            for (List<String> phyList : lists) {
                final String tableName = genTargetActualTableName(logicalTableName, toTableName, phyList.get(0));
                phyList.add(tableName);
            }
        }
    }

    private String genTargetActualTableName(String origLogicalTableName, String toLocalTableName,
                                            String origActualTableName) {
        return toLocalTableName + getTargetActualTableNameSuffix(origLogicalTableName, origActualTableName);
    }

    /**
     * get the phy table name after renaming
     */
    private String getTargetActualTableNameSuffix(String origLogicalTableName, String origActualTableName) {
        // We still need to rename physical tables for logical tables created
        // before logical rename table support, otherwise user may fail to
        // create a new table with the same logical name after renaming such
        // logical tables.
        if (origLogicalTableName != null && origActualTableName != null
            && origLogicalTableName.length() < origActualTableName.length()) {
            return origActualTableName.substring(origLogicalTableName.length());
        } else {
            return "";
        }
    }

}
