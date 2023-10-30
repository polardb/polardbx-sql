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

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbSysForeignCols;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author wenki
 */
public class InformationSchemaInnodbSysForeignColsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaInnodbSysForeignColsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbSysForeignCols;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        if (!ConfigDataMode.isPolarDbX()) {
            return cursor;
        }

        try (Connection connection = MetaDbUtil.getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("select * from foreign_key_cols");

            while (rs.next()) {
                String schema = rs.getString("schema_name");
                String tableName = rs.getString("table_name");
                String indexName = rs.getString("index_name");
                String forColName = rs.getString("for_col_name");
                String refColName = rs.getString("ref_col_name");
                long pos = rs.getLong("pos");
                String constraintName = "";

                ResultSet fkRs = stmt.executeQuery(String.format(
                    "select * from foreign_key where schema_name = '%s' and table_name = '%s' and index_name = '%s'",
                    schema,
                    tableName, indexName));
                if (fkRs.next()) {
                    constraintName = fkRs.getString("constraint_name");
                }

                String id = schema + "/" + constraintName;
                cursor.addRow(new Object[] {id, forColName, refColName, pos});
            }

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }

        return cursor;
    }
}
