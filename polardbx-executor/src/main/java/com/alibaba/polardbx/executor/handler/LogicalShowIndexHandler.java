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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexColumnMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiIndexMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiTableMetaBean;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.InformationSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.PolarDbXSystemTableView;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.PerformanceSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_LOCAL_INDEX_PREFIX;
import static com.alibaba.polardbx.common.TddlConstants.IMPLICIT_COL_NAME;

/**
 * @author chenmo.cm
 */
public class LogicalShowIndexHandler extends BaseDalHandler {

    public LogicalShowIndexHandler(IRepository repo) {
        super(repo);
    }

    private Cursor handleForShardingDatabase(RelNode logicalPlan, ExecutionContext executionContext) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        final SqlShowIndex showIndex = (SqlShowIndex) dal.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(showIndex.getTableName());

        if (dal.getNativeSqlNode() instanceof SqlShow) {
            // handle show index from view
            final SqlShow desc = (SqlShow) dal.getNativeSqlNode();
            final String schemaName = executionContext.getSchemaName();
            ViewManager viewManager;
            if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.informationSchema(desc.getTableName())) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.mysqlSchema(desc.getTableName())) {
                viewManager = MysqlSchemaViewManager.getInstance();
            } else if (RelUtils.performanceSchema(desc.getTableName())) {
                viewManager = PerformanceSchemaViewManager.getInstance();
            } else {
                viewManager = OptimizerContext.getContext(schemaName).getViewManager();
            }
            SystemTableView.Row row = viewManager.select(tableName);
            if (row != null) {
                // empty set for view index
                ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
                resultCursor.addColumn("", DataTypes.StringType);
                return resultCursor;
            }
        }

        final GsiMetaBean tableMeta = OptimizerContext.getContext(((BaseDalOperation) logicalPlan).getSchemaName())
            .getLatestSchemaManager()
            .getGsi(tableName, IndexStatus.ALL);

        Cursor indexFromMain = null;
        if (dal instanceof LogicalDal) {
            indexFromMain = super.handle(logicalPlan.getInput(0), executionContext);
        } else {
            indexFromMain = super.handle(logicalPlan, executionContext);
        }

        ArrayResultCursor result = new ArrayResultCursor("STATISTICS");
        result.addColumn("Table", null, DataTypes.StringType);
        result.addColumn("Non_unique", null, DataTypes.IntegerType);
        result.addColumn("Key_name", null, DataTypes.StringType);
        result.addColumn("Seq_in_index", null, DataTypes.IntegerType);
        result.addColumn("Column_name", null, DataTypes.StringType);
        result.addColumn("Collation", null, DataTypes.StringType);
        result.addColumn("Cardinality", null, DataTypes.LongType);
        result.addColumn("Sub_part", null, DataTypes.IntegerType);
        result.addColumn("Packed", null, DataTypes.StringType);
        result.addColumn("Null", null, DataTypes.StringType);
        result.addColumn("Index_type", null, DataTypes.StringType);
        result.addColumn("Comment", null, DataTypes.StringType);
        result.addColumn("Index_comment", null, DataTypes.StringType);
        result.initMeta();

        Row row = null;
        while (null != (row = indexFromMain.next())) {
            if (IMPLICIT_COL_NAME.equalsIgnoreCase(row.getString(4))) {
                continue;
            }
            final Object[] objects = row.getValues().toArray();
            objects[0] = tableName.toLowerCase();
            result.addRow(objects);
        }

        indexFromMain.close(new ArrayList<>());

        if (tableMeta.withGsi(tableName)) {
            final GsiTableMetaBean gsiTableMetaBean = tableMeta.getTableMeta().get(tableName);
            for (Entry<String, GsiIndexMetaBean> indexEntry : gsiTableMetaBean.indexMap.entrySet()) {
                final String indexName = indexEntry.getKey();
                final GsiIndexMetaBean indexDetail = indexEntry.getValue();

                // Ignore GSI which is not public.
                if (indexDetail.indexStatus != IndexStatus.PUBLIC) {
                    continue;
                }

                for (GsiIndexColumnMetaBean column : indexDetail.indexColumns) {
                    List<Object> indexRow = new ArrayList<>();
                    indexRow.add(tableName.toLowerCase());
                    indexRow.add(column.nonUnique ? 1 : 0);
                    indexRow.add(indexName);
                    indexRow.add(column.seqInIndex);
                    indexRow.add(column.columnName);
                    indexRow.add(column.collation);
                    indexRow.add(column.cardinality);
                    indexRow.add(column.subPart);
                    indexRow.add(column.packed);
                    indexRow.add(column.nullable);
                    indexRow.add("GLOBAL");
                    indexRow.add("INDEX");
                    indexRow.add(indexDetail.indexComment);
                    result.addRow(indexRow.toArray());
                }

                for (GsiIndexColumnMetaBean column : indexDetail.coveringColumns) {
                    List<Object> indexRow = new ArrayList<>();
                    indexRow.add(tableName.toLowerCase());
                    indexRow.add(column.nonUnique ? 1 : 0);
                    indexRow.add(indexName);
                    indexRow.add(column.seqInIndex);
                    indexRow.add(column.columnName);
                    indexRow.add(column.collation);
                    indexRow.add(column.cardinality);
                    indexRow.add(column.subPart);
                    indexRow.add(column.packed);
                    indexRow.add(column.nullable);
                    indexRow.add("GLOBAL");
                    indexRow.add("COVERING");
                    indexRow.add(indexDetail.indexComment);
                    result.addRow(indexRow.toArray());
                }
            }

        }

        return result;
    }

    private Cursor handleForPartitionDatabase(RelNode logicalPlan, ExecutionContext executionContext) {
        final BaseDalOperation dal = (BaseDalOperation) logicalPlan;
        final SqlShowIndex showIndex = (SqlShowIndex) dal.getNativeSqlNode();
        final String tableName = RelUtils.lastStringValue(showIndex.getTableName());

        if (dal.getNativeSqlNode() instanceof SqlShow) {
            // handle show index from view
            final SqlShow desc = (SqlShow) dal.getNativeSqlNode();
            final String schemaName = executionContext.getSchemaName();
            ViewManager viewManager;
            if (InformationSchema.NAME.equalsIgnoreCase(schemaName)) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.informationSchema(desc.getTableName())) {
                viewManager = InformationSchemaViewManager.getInstance();
            } else if (RelUtils.mysqlSchema(desc.getTableName())) {
                viewManager = MysqlSchemaViewManager.getInstance();
            } else {
                viewManager = OptimizerContext.getContext(schemaName).getViewManager();
            }
            SystemTableView.Row row = viewManager.select(tableName);
            if (row != null) {
                // empty set for view index
                ArrayResultCursor resultCursor = new ArrayResultCursor(tableName);
                resultCursor.addColumn("", DataTypes.StringType);
                return resultCursor;
            }
        }

        final GsiMetaBean tableMeta = OptimizerContext.getContext(((BaseDalOperation) logicalPlan).getSchemaName())
            .getLatestSchemaManager()
            .getGsi(tableName, IndexStatus.ALL);

        Cursor indexFromMain = null;
        if (dal instanceof LogicalDal) {
            indexFromMain = super.handle(logicalPlan.getInput(0), executionContext);
        } else {
            indexFromMain = super.handle(logicalPlan, executionContext);
        }

        ArrayResultCursor result = new ArrayResultCursor("STATISTICS");
        result.addColumn("Table", null, DataTypes.StringType);
        result.addColumn("Non_unique", null, DataTypes.IntegerType);
        result.addColumn("Key_name", null, DataTypes.StringType);
        result.addColumn("Seq_in_index", null, DataTypes.IntegerType);
        result.addColumn("Column_name", null, DataTypes.StringType);
        result.addColumn("Collation", null, DataTypes.StringType);
        result.addColumn("Cardinality", null, DataTypes.LongType);
        result.addColumn("Sub_part", null, DataTypes.IntegerType);
        result.addColumn("Packed", null, DataTypes.StringType);
        result.addColumn("Null", null, DataTypes.StringType);
        result.addColumn("Index_type", null, DataTypes.StringType);
        result.addColumn("Comment", null, DataTypes.StringType);
        result.addColumn("Index_comment", null, DataTypes.StringType);
        result.initMeta();

        Row row = null;
        while (null != (row = indexFromMain.next())) {
            if (IMPLICIT_COL_NAME.equalsIgnoreCase(row.getString(4))) {
                continue;
            }
            final Object[] objects = row.getValues().toArray();
            objects[0] = tableName.toLowerCase();
            final String indexName = objects[2].toString();
            if (indexName.startsWith(AUTO_LOCAL_INDEX_PREFIX)) {
                objects[2] = indexName.substring(AUTO_LOCAL_INDEX_PREFIX.length()); // Fake one.
            }
            result.addRow(objects);
        }

        indexFromMain.close(new ArrayList<>());

        // Ignore GSI.
        return result;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        String schemaName = ((BaseDalOperation) logicalPlan).getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }

        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return handleForPartitionDatabase(logicalPlan, executionContext);
        } else {
            return handleForShardingDatabase(logicalPlan, executionContext);
        }
    }
}
