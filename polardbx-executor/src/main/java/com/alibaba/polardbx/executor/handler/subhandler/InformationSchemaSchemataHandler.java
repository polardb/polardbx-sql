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
import com.alibaba.polardbx.gms.metadb.table.SchemataAccessor;
import com.alibaba.polardbx.gms.metadb.table.SchemataRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaSchemata;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Set;

public class InformationSchemaSchemataHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaSchemataHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaSchemata;
    }

    /**
     * CATALOG_NAME is always "def"
     * SQL_PATH is always NULL
     * DEFAULT_ENCRYPTION for compatibility with mysql8.0, value is "NO"
     */
    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<SchemataRecord> allSchemata = SchemataAccessor.getAllSchemata();
        for (SchemataRecord schemata : allSchemata) {
            boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemata.schemaName);
            String mode = isNewPart ? "auto" : "drds";
            cursor.addRow(new Object[] {
                "def", schemata.schemaName, schemata.defaultCharSetName, schemata.defaultCollationName,
                null, "NO", mode});
        }
        return cursor;
    }
}
