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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.rel.RelNode;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @author chenmo.cm
 */
public class LogicalShowProcedureStatusMyHandler extends HandlerCommon {

    public LogicalShowProcedureStatusMyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        Cursor cursor = null;
        try {
            ArrayResultCursor result = new ArrayResultCursor("SOUTINES");
            result.addColumn("Db", DataTypes.StringType);
            result.addColumn("Name", DataTypes.StringType);
            result.addColumn("Type", DataTypes.StringType);
            result.addColumn("Definer", DataTypes.StringType);
            result.addColumn("Modified", DataTypes.DatetimeType);
            result.addColumn("Created", DataTypes.DatetimeType);
            result.addColumn("Security_type", DataTypes.StringType);
            result.addColumn("Comment", DataTypes.StringType);
            result.addColumn("character_set_client", DataTypes.StringType);
            result.addColumn("collation_connection", DataTypes.StringType);
            result.addColumn("Database Collation", DataTypes.StringType);
            result.initMeta();

            final LogicalShow show = (LogicalShow) logicalPlan;
            cursor = repo.getCursorFactory().repoCursor(executionContext,
                new PhyShow(show.getCluster(),
                    show.getTraitSet(),
                    show.getNativeSqlNode(),
                    show.getRowType(),
                    show.getDbIndex(),
                    show.getPhyTable(),
                    show.getSchemaName()));
            Row row = null;
            while ((row = cursor.next()) != null) {
                String Db = null;
                if (!TStringUtil.isEmpty(executionContext.getSchemaName())) {
                    /**
                     * should replace it by appname if appName is avaiable in
                     * server
                     */
                    Db = executionContext.getSchemaName();
                } else {
                    Db = row.getString(0);
                }
                String Name = row.getString(1);
                String Type = row.getString(2);
                String Definer = row.getString(3);
                Timestamp Modified = row.getTimestamp(4);
                Timestamp Created = row.getTimestamp(5);
                String Security_type = row.getString(6);
                String Comment = row.getString(7);
                String character_set_client = row.getString(8);
                String collation_connection = row.getString(9);
                String Database_Collation = row.getString(10);

                result.addRow(new Object[] {
                    Db, Name, Type, Definer, Modified, Created, Security_type, Comment,
                    character_set_client, collation_connection, Database_Collation});
            }

            return result;
        } finally {
            // 关闭cursor
            if (cursor != null) {
                cursor.close(new ArrayList<Throwable>());
            }
        }
    }
}
