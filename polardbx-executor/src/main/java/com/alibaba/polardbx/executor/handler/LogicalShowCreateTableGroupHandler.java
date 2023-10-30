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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.common.ResultSetHelper;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowCreateTable;
import org.apache.calcite.sql.SqlShowCreateTableGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luoyanxin
 */
public class LogicalShowCreateTableGroupHandler extends HandlerCommon {

    public LogicalShowCreateTableGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowCreateTableGroup showCreateTableGroup = (SqlShowCreateTableGroup) show.getNativeSqlNode();

        String schemaName = show.getSchemaName();

        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            ArrayResultCursor result = new ArrayResultCursor("Create TableGroup");
            result.addColumn("TableGroup", DataTypes.StringType, false);
            result.addColumn("Create TableGroup", DataTypes.StringType, false);
            result.initMeta();
            final String tableGroupName = RelUtils.lastStringValue(showCreateTableGroup.getTableName());
            TableGroupConfig tableGroupConfig = OptimizerContext.getContext(schemaName).getTableGroupInfoManager()
                .getTableGroupConfigByName(tableGroupName);
            if (tableGroupConfig == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_TABLE_GROUP_NOT_EXISTS, tableGroupName);
            }
            result.addRow(
                new Object[] {tableGroupName, tableGroupConfig.getTableGroupRecord().getPartition_definition()});
            return result;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "do not support [show create tablegroup] in drds mode database");
        }
    }

}
