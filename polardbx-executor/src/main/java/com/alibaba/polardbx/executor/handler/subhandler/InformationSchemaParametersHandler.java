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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaParameters;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InformationSchemaParametersHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaParametersHandler.class);

    public InformationSchemaParametersHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaParameters;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        try (Connection metaDbConn = MetaDbUtil.getConnection();
            PreparedStatement statement = metaDbConn.prepareStatement("SELECT * FROM " + GmsSystemTables.PARAMETERS);
            ResultSet rs = statement.executeQuery();) {
            while (rs.next()) {
                cursor.addRow(new Object[] {
                    rs.getString("SPECIFIC_CATALOG"),
                    rs.getString("SPECIFIC_SCHEMA"),
                    rs.getString("SPECIFIC_NAME"),
                    rs.getInt("ORDINAL_POSITION"),
                    rs.getString("PARAMETER_MODE"),
                    rs.getString("PARAMETER_NAME"),
                    rs.getString("DATA_TYPE"),
                    rs.getInt("CHARACTER_MAXIMUM_LENGTH"),
                    rs.getInt("CHARACTER_OCTET_LENGTH"),
                    rs.getLong("NUMERIC_PRECISION"),
                    rs.getInt("NUMERIC_SCALE"),
                    rs.getLong("DATETIME_PRECISION"),
                    rs.getString("CHARACTER_SET_NAME"),
                    rs.getString("COLLATION_NAME"),
                    rs.getString("DTD_IDENTIFIER"),
                    rs.getString("ROUTINE_TYPE")
                });
            }
        } catch (SQLException ex) {
            logger.error("get information schema parameters failed!", ex);
        }
        return cursor;
    }
}
