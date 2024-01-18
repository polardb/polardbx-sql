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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlShowTables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author chenmo.cm
 */
public class LogicalShowTablesMyHandler extends LogicalInfoSchemaQueryHandler {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowTablesMyHandler.class);

    private long cacheSize = 1000;
    private long expireTime = 5 * 60 * 1000;
    private Cache<String, List<Object[]>> tableCaches = CacheBuilder.newBuilder()
        .maximumSize(cacheSize)
        .expireAfterWrite(expireTime, TimeUnit.MILLISECONDS)
        .softValues()
        .build();

    public LogicalShowTablesMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(final RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalShow showNode = (LogicalShow) logicalPlan;
        SqlShowTables show = (SqlShowTables) showNode.getNativeSqlNode();

        ArrayResultCursor result = new ArrayResultCursor("TABLES");

        boolean full = show.isFull();
        result.addColumn("Tables_in_" + showNode.getSchemaName(), DataTypes.StringType, false);
        if (full) {
            result.addColumn("Table_type", DataTypes.StringType, false);
            result.addColumn("Auto_partition", DataTypes.StringType, false);
        }
        result.initMeta();
        List<Object[]> tables;

        if (InformationSchema.NAME.equalsIgnoreCase(showNode.getSchemaName())) {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("select table_name ");
            if (full) {
                sqlBuilder.append(", table_type ");
                sqlBuilder.append(", 'NO' as auto_partition ");
            }
            sqlBuilder.append(" from `information_schema`.`tables` where table_schema = 'information_schema'");

            if (show.like instanceof SqlCharStringLiteral) {
                String like = ((SqlCharStringLiteral) show.like).getNlsString().getValue();
                sqlBuilder.append(" and table_name like '").append(like.replace("'", "\\'")).append("'");
            }
            ExecutionContext newExecutionContext = executionContext.copy();
            ExecutionPlan executionPlan = Planner.getInstance().plan(sqlBuilder.toString(), newExecutionContext);
            return ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
        } else {
            LogicalInfoSchemaContext infoSchemaContext = new LogicalInfoSchemaContext(executionContext);
            infoSchemaContext.setTargetSchema(showNode.getSchemaName());
            infoSchemaContext.prepareContextAndRepository(repo);
            boolean needCache = executionContext.getParamManager().getBoolean(ConnectionParams.SHOW_TABLES_CACHE);
            if (needCache) {
                String key = showNode.getDbIndex() + ":" + show.toString();
                try {
                    tables = tableCaches.get(key, () -> showFullTables(showNode, infoSchemaContext));
                } catch (Throwable e) {
                    tableCaches.invalidate(key);
                    throw GeneralUtil.nestedException(e);
                }
            } else {
                tables = showFullTables(showNode, infoSchemaContext);
            }
            if (!ConfigDataMode.isFastMock()) {
                // views
                Cursor viewCursor = null;
                try {
                    StringBuilder sqlBuilder = new StringBuilder();
                    sqlBuilder.append("select table_name ");
                    sqlBuilder.append(" from `information_schema`.`views` where table_schema = '")
                        .append(executionContext.getSchemaName()).append("'");

                    if (show.like instanceof SqlCharStringLiteral) {
                        String like = ((SqlCharStringLiteral) show.like).getNlsString().getValue();
                        sqlBuilder.append(" and table_name like '").append(like.replace("'", "\\'")).append("'");
                    }
                    ExecutionContext newExecutionContext = executionContext.copy();
                    ExecutionPlan executionPlan =
                        Planner.getInstance().plan(sqlBuilder.toString(), newExecutionContext);
                    viewCursor = ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
                    Row row;
                    while ((row = viewCursor.next()) != null) {
                        String viewName = row.getString(0);
                        if (full) {
                            tables.add(new Object[] {viewName, "VIEW", "NO"});
                        } else {
                            tables.add(new Object[] {viewName});
                        }
                    }
                } catch (TddlRuntimeException t) {
                    logger.error(t);
                    throw t;
                } finally {
                    if (viewCursor != null) {
                        viewCursor.close(new ArrayList<>());
                    }
                }
            }

        }
        for (Object[] table : tables) {
            result.addRow(table);
        }
        return result;
    }

}
