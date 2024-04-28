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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableConstraints;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;

/**
 * @author shengyu
 */
public class InformationSchemaTableConstraintsHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaTableConstraintsHandler.class);

    public InformationSchemaTableConstraintsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableConstraints;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaTableConstraints informationSchemaTableConstraints =
            (InformationSchemaTableConstraints) virtualView;
        InformationSchemaTables informationSchemaTables =
            new InformationSchemaTables(informationSchemaTableConstraints.getCluster(),
                informationSchemaTableConstraints.getTraitSet());

        informationSchemaTables.copyFilters(informationSchemaTableConstraints);

        Cursor tablesCursor = null;
        try {
            tablesCursor = virtualViewHandler.handle(informationSchemaTables, executionContext);

            Row row;
            while ((row = tablesCursor.next()) != null) {
                String tableSchema = row.getString(1);
                String tableName = row.getString(2);
                if (InformationSchema.NAME.equalsIgnoreCase(tableSchema)) {
                    continue;
                }

                try {
                    TableMeta tableMeta =
                        Objects.requireNonNull(OptimizerContext.getContext(tableSchema)).getLatestSchemaManager()
                            .getTable(tableName);
                    for (IndexMeta indexMeta : tableMeta.getIndexes()) {
                        //for primary key and unique key, they must be in the same table
                        if (indexMeta.isPrimaryKeyIndex()) {
                            cursor.addRow(new Object[] {
                                "def",
                                tableSchema,
                                indexMeta.getPhysicalIndexName(),
                                tableSchema,
                                tableName,
                                "PRIMARY KEY",
                                "YES"});
                        } else if (indexMeta.isUniqueIndex()) {
                            cursor.addRow(new Object[] {
                                "def",
                                tableSchema,
                                indexMeta.getPhysicalIndexName(),
                                tableSchema,
                                tableName,
                                "UNIQUE",
                                "YES"});
                        }
                    }
                    for (Map.Entry<String, ForeignKeyData> entry : tableMeta.getForeignKeys().entrySet()) {
                        cursor.addRow(new Object[] {
                            "def",
                            tableSchema,
                            entry.getValue().constraint,
                            tableSchema,
                            tableName,
                            "FOREIGN KEY",
                            "YES"});
                    }
                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        } finally {
            if (tablesCursor != null) {
                tablesCursor.close(new ArrayList<>());
            }
        }

        return cursor;
    }
}
