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

package com.alibaba.polardbx.gms.metadb.audit;

import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.metadb.scheduler.Cleanable;
import org.apache.commons.lang.StringUtils;

import java.sql.PreparedStatement;

/**
 * @author chenghui.lch
 */
public class AuditLogAccessor extends AbstractAccessor implements Cleanable {
    private static final Logger logger = LoggerFactory.getLogger(AuditLogAccessor.class);
    private static final String AUDIT_LOG = GmsSystemTables.AUDIT_LOG;

    private static final String INSERT_IGNORE_AUDIT_LOG =
        "insert ignore into `" + AUDIT_LOG
            + "` (`user_name`,`host`,`port`,`schema`,`audit_info`,`action`,`trace_id`) values (?,?,?,?,?,?,?)";
    private static final String CLEAN_AUDIT_LOG =
        "delete from `" + AUDIT_LOG + "` where gmt_created < ?";

    public void addAuditLog(String userName, String host, int port, String schema, String auditInfo,
                            AuditAction auditAction, String traceId) {

        try (PreparedStatement preparedStmt = this.connection.prepareStatement(INSERT_IGNORE_AUDIT_LOG)) {
            preparedStmt.setString(1, userName);
            preparedStmt.setString(2, host);
            preparedStmt.setInt(3, port);
            preparedStmt.setString(4, schema);
            preparedStmt.setString(5, auditInfo);
            preparedStmt.setString(6, auditAction.name());
            preparedStmt.setString(7, traceId);
            preparedStmt.addBatch();
            preparedStmt.executeBatch();
        } catch (Throwable e) {
            logger.error("Failed to query the system table '" + AUDIT_LOG + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                AUDIT_LOG,
                e.getMessage());
        }
    }

    @Override
    public String getCleanSql() {
        return CLEAN_AUDIT_LOG;
    }

    @Override
    public int getDelayDays() {
        String delayPeriod =
            MetaDbInstConfigManager.getInstance()
                .getInstProperty(ConnectionProperties.MAX_AUDIT_LOG_CLEAN_DELAY_DAYS);
        if (StringUtils.isEmpty(delayPeriod)) {
            return 1;
        }
        try {
            return Integer.valueOf(delayPeriod);
        } catch (Exception e) {
            logger.warn("Loading MAX_AUDIT_LOG_CLEAN_DELAY_DAYS", e);
            return 1;
        }
    }

    @Override
    public int getKeepDays() {
        String keepDays =
            MetaDbInstConfigManager.getInstance().getInstProperty(ConnectionProperties.MAX_AUDIT_LOG_CLEAN_KEEP_DAYS);
        if (StringUtils.isEmpty(keepDays)) {
            return 180;
        }
        try {
            return Integer.valueOf(keepDays);
        } catch (Exception e) {
            logger.warn("Loading MAX_AUDIT_LOG_CLEAN_KEEP_DAYS", e);
            return 180;
        }
    }
}
