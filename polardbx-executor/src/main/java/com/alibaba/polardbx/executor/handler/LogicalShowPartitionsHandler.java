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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowPartitions;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenmo.cm
 */
public class LogicalShowPartitionsHandler extends HandlerCommon {
    public LogicalShowPartitionsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowPartitions showPartitions = (SqlShowPartitions) show.getNativeSqlNode();

        String schemaName = ((LogicalShow) logicalPlan).getSchemaName();
        String tableName = RelUtils.lastStringValue(showPartitions.getTableName());

        final OptimizerContext context = OptimizerContext.getContext(schemaName);
        context.getLatestSchemaManager().getTable(tableName);

        ArrayResultCursor result = new ArrayResultCursor("DBPARTITIONS");
        result.addColumn("KEYS", DataTypes.StringType);
        result.initMeta();

        schemaName = TStringUtil.isNotEmpty(schemaName) ? schemaName : executionContext.getSchemaName();
        boolean isTableWithoutPrivileges = !CanAccessTable.verifyPrivileges(
            schemaName,
            tableName,
            executionContext);
        if (isTableWithoutPrivileges) {
            return result;
        }

        TableRule tableRule = context.getRuleManager().getTableRule(tableName);
        if (tableRule == null) {
            result.addRow(new Object[] {"null"});
        } else {
            String keys = StringUtils.join(tableRule.getShardColumns(), ",");
            result.addRow(new Object[] {keys});
        }
        return result;
    }
}
