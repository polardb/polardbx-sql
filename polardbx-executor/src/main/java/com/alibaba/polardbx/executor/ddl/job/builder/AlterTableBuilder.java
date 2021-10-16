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

import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlAlterTable;

/**
 * Plan builder for ALTER TABLE
 *
 * @author moyi
 * @since 2021/07
 */
public class AlterTableBuilder extends DdlPhyPlanBuilder {

    protected final AlterTablePreparedData preparedData;

    protected AlterTableBuilder(DDL ddl, AlterTablePreparedData preparedData, ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.preparedData = preparedData;
    }

    public static AlterTableBuilder create(DDL ddl,
                                           AlterTablePreparedData preparedData,
                                           ExecutionContext ec) {
        return DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName()) ?
            new AlterPartitionTableBuilder(ddl, preparedData, ec) :
            new AlterTableBuilder(ddl, preparedData, ec);
    }

    @Override
    public void buildTableRuleAndTopology() {
        buildExistingTableRule(preparedData.getTableName());
        buildChangedTableTopology(preparedData.getSchemaName(), preparedData.getTableName());
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(preparedData.getTableName());
    }

    @Override
    protected void buildSqlTemplate() {
        super.buildSqlTemplate();
        this.sequenceBean = ((SqlAlterTable) this.sqlTemplate).getAutoIncrement();
    }

}
