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

package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TableNamesRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TableListListener extends GenericObjectListListener {

    public TableListListener(String schemaName) {
        super(schemaName);
    }

    @Override
    protected List<SystemTableRecord> fetchTablesName() {
        List<TableNamesRecord> tablesRecords = fetchVisibleTableNames();
        List<SystemTableRecord> records = new ArrayList<>(tablesRecords.size());
        records.addAll(tablesRecords);
        return records;
    }

    @Override
    protected String getDataId(String tableSchema, String tableName) {
        return MetaDbDataIdBuilder.getTableDataId(tableSchema, tableName);
    }

    @Override
    protected String getDataIdPrefix() {
        return MetaDbDataIdBuilder.getTableDataIdPrefix(schemaName);
    }

    @Override
    protected ConfigListener getObjectListener(String tableSchema, String tableName) {
        return new TableMetaListener(tableSchema, tableName);
    }

    private List<TableNamesRecord> fetchVisibleTableNames() {
        TableInfoManager tableInfoManager = new TableInfoManager();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            tableInfoManager.setConnection(metaDbConn);
            return tableInfoManager.queryVisibleTableNames(schemaName);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        } finally {
            tableInfoManager.setConnection(null);
        }
    }

}
