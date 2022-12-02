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

package com.alibaba.polardbx.net.util;

import com.alibaba.polardbx.common.audit.AuditAction;
import com.alibaba.polardbx.common.audit.AuditUtils;
import com.alibaba.polardbx.gms.metadb.audit.AuditLogAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

public class AuditUtil {
    public static void logAuditInfo(String instId, String database, String user, String host, int port,
                                    AuditAction action) {
        if (!AuditUtils.isEnableLogAudit()) {
            return;
        }
        // `gmt_create`, `instance_name`, `user_name`, `db_name`, `host`, `action`
        AuditUtils.logAuditInfo(instId + ',' + user
            + ',' + database + ',' + host, action);
        AuditLogAccessor auditLogAccessor = new AuditLogAccessor();
        try (Connection conn = MetaDbUtil.getConnection()) {
            auditLogAccessor.setConnection(conn);
            auditLogAccessor.addAuditLog(user, host, port, database, null, action, null);
        } catch (SQLException e) {
            //ignore;
        }
    }
}
