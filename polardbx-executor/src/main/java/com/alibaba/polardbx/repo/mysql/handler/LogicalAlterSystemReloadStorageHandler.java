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
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.AlterSystemReloadStorageSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.ha.HaSwitchParams;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalAlterSystemReloadStorage;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang.StringUtils;

import java.util.Date;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class LogicalAlterSystemReloadStorageHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalAlterSystemReloadStorageHandler.class);

    public LogicalAlterSystemReloadStorageHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        // god privilege check.
        TableValidator.checkGodPrivilege(executionContext);
        LogicalAlterSystemReloadStorage reloadStorage = (LogicalAlterSystemReloadStorage) logicalPlan;
        List<String> dnList = reloadStorage.getDnIdList();
        syncReloadStorage(dnList);
        return buildResultCursor();
    }

    private void syncReloadStorage(List<String> dnList) {
        try {
            SyncManagerHelper.sync(new AlterSystemReloadStorageSyncAction(dnList));
        } catch (Throwable e) {
            logger.error(e);
            throw new TddlNestableRuntimeException(e);
        }
    }

    private ArrayResultCursor buildResult(List<StorageInstHaContext> haContexts) {

        ArrayResultCursor result = new ArrayResultCursor("Reload Storage");
        result.addColumn("DN", DataTypes.StringType);
        result.addColumn("RW_DN", DataTypes.StringType);
        result.addColumn("KIND", DataTypes.StringType);

        result.addColumn("NODE", DataTypes.StringType);
        result.addColumn("USER", DataTypes.StringType);
        result.addColumn("PASSWD_ENC", DataTypes.StringType);

        result.addColumn("ROLE", DataTypes.StringType);
        result.addColumn("IS_VIP", DataTypes.BooleanType);
        result.addColumn("FROM", DataTypes.StringType);
        result.initMeta();

        for (StorageInstHaContext haContext : haContexts) {
            String dnId = haContext.getStorageInstId();
            String rwDnId = haContext.getStorageMasterInstId();
            String kind = StorageInfoRecord.getInstKind(haContext.getStorageKind());

            // add vip info if vip exists
            String vipAddr = haContext.getStorageVipAddr();
            if (!StringUtils.isEmpty(vipAddr)) {
                result.addRow(new Object[] {
                    dnId,
                    rwDnId,
                    kind,
                    haContext,
                    haContext.getStorageVipInfo().user,
                    haContext.getStorageVipInfo().passwdEnc,
                    "",
                    true});
            }

            String currNode = haContext.getCurrAvailableNodeAddr();
            String currUser = haContext.getUser();
            String currPasswdEnc = haContext.getEncPasswd();

            String node = haContext.getStorageVipAddr();
            String user = null;
            String passwdEnc = null;
            String role = null;
            Boolean isVip = false;
            String fromArea = null;

            result.addRow(new Object[] {
                dnId,
                rwDnId,
                kind,
                node,
                user,
                passwdEnc,
                role,
                isVip});
        }

        return result;
    }

    protected Cursor buildResultCursor() {
        // Always return 0 rows affected or throw an exception to report error messages.
        // SHOW DDL RESULT can provide more result details for the DDL execution.
        return new AffectRowCursor(new int[] {0});
    }
}
