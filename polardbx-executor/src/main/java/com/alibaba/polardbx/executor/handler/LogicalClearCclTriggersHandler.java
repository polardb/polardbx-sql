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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.ccl.CclRuleAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerAccessor;
import com.alibaba.polardbx.gms.metadb.ccl.CclTriggerRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalCcl;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2021/4/18 4:49 下午
 */
public class LogicalClearCclTriggersHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalClearCclTriggersHandler.class);

    public LogicalClearCclTriggersHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalCcl plan = (LogicalCcl) logicalPlan;
        Connection outMetaDbConn = null;
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            outMetaDbConn = metaDbConn;
            CclTriggerAccessor cclTriggerAccessor = new CclTriggerAccessor();
            cclTriggerAccessor.setConnection(metaDbConn);
            List<CclTriggerRecord> cclTriggerRecordList = cclTriggerAccessor.query();
            if (CollectionUtils.isNotEmpty(cclTriggerRecordList)) {
                List<Integer> triggerPriorities = cclTriggerRecordList.stream().map((e) -> e.priority).collect(
                    Collectors.toList());
                CclRuleAccessor cclRuleAccessor = new CclRuleAccessor();
                cclRuleAccessor.setConnection(metaDbConn);
                cclRuleAccessor.deleteByTriggers(triggerPriorities);
            }
            int affectedRowCount = cclTriggerAccessor.deleteAll();
            String dataId = MetaDbDataIdBuilder.getCclRuleDataId(InstIdUtil.getInstId());
            MetaDbConfigManager metaDbConfigManager = MetaDbConfigManager.getInstance();
            metaDbConfigManager.notify(dataId, metaDbConn);
            metaDbConn.commit();
            metaDbConfigManager.sync(dataId);
            return new AffectRowCursor(affectedRowCount);
        } catch (Throwable e) {
            if (outMetaDbConn != null) {
                try {
                    outMetaDbConn.rollback();
                } catch (Throwable throwable) {
                    logger.error("Failed to rollback", throwable);
                }
            }
            throw new TddlRuntimeException(ErrorCode.ERR_CCL, " Failed to CLEAR CCL_TRIGGERS", e);
        }
    }
}
