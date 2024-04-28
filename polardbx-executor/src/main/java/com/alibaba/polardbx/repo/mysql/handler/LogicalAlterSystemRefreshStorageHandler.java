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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.AlterSystemRefreshStorageSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemRefreshStorage;
import org.apache.calcite.rel.RelNode;

/**
 * @author chenghui.lch
 */
public class LogicalAlterSystemRefreshStorageHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterSystemRefreshStorageHandler.class);

    public LogicalAlterSystemRefreshStorageHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        // god privilege check.
        TableValidator.checkGodPrivilege(executionContext);
        LogicalAlterSystemRefreshStorage refreshStorage = (LogicalAlterSystemRefreshStorage) logicalPlan;
        String dnId = refreshStorage.getDnId();
        String vipAddr = refreshStorage.getVipAddr();
        String user = refreshStorage.getUserName();
        String passwdEnc = refreshStorage.getPasswdEnc();
        syncRefreshStorage(dnId, vipAddr, user, passwdEnc);
        return buildResultCursor();
    }

    private void syncRefreshStorage(String dnId, String vipAddr, String user, String encPasswd) {
        try {
            SyncManagerHelper.sync(new AlterSystemRefreshStorageSyncAction(dnId, vipAddr, user, encPasswd),
                SyncScope.ALL);
        } catch (Throwable e) {
            logger.error(e);
            throw new TddlNestableRuntimeException(e);
        }
    }

    protected Cursor buildResultCursor() {
        // Always return 0 rows affected or throw an exception to report error messages.
        // SHOW DDL RESULT can provide more result details for the DDL execution.
        return new AffectRowCursor(new int[] {0});
    }
}


