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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoAccessor;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaStoragePoolInfo;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @author moyi
 * @since 2021/01
 */
public class InformationSchemaStoragePoolInfoHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaStoragePoolInfoHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaStoragePoolInfo;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            List<StoragePoolInfoRecord> records = accessor.getAllStoragePoolInfoRecord();
            String idleDnIds = "";
            for (StoragePoolInfoRecord storagePoolInfoRecord : records) {
                cursor.addRow(new Object[] {
                    storagePoolInfoRecord.id,
                    storagePoolInfoRecord.name,
                    storagePoolInfoRecord.dnIds,
                    idleDnIds,
                    storagePoolInfoRecord.undeletableDnId,
                    storagePoolInfoRecord.extras,
                    storagePoolInfoRecord.gmtCreated,
                    storagePoolInfoRecord.gmtModified,
                });
            }
            //TODO: add storage Info.
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

        return cursor;
    }

}

