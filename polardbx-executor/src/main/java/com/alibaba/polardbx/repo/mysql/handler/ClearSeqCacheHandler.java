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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ClearSeqCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.sql.SqlClearSeqCache;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.List;

/**
 * Created by chensr on 2017/8/28.
 */
public class ClearSeqCacheHandler extends AbstractDalHandler {

    public ClearSeqCacheHandler(IRepository repo) {
        super(repo);
    }

    @Override
    Cursor doHandle(LogicalDal logicalPlan, ExecutionContext context) {
        SqlClearSeqCache stmt = (SqlClearSeqCache) logicalPlan.getNativeSqlNode();

        List<String> names = ((SqlIdentifier) stmt.getName()).names;
        String schemaName = names.size() == 2 ? names.get(0) : context.getSchemaName();
        String seqName = names.get(names.size() - 1);
        boolean isAll = "ALL".equalsIgnoreCase(seqName);

        SyncManagerHelper.sync(new ClearSeqCacheSyncAction(schemaName, seqName, isAll, true));

        return new AffectRowCursor(isAll ? 0 : 1);
    }

}
