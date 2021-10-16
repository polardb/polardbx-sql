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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.SqlType;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.privilege.Permission;
import com.alibaba.polardbx.gms.privilege.PermissionCheckContext;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.privilege.PrivilegeKind;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.commons.lang3.StringUtils;

import java.util.Set;

/**
 * @author shicai.xsc 2020/3/5 17:54
 * @since 5.0.0.0
 */
public class PolarPrivilegeUtils {

    static Set<SqlType> allowedSqlTypeOfDefaultDb = Sets.newHashSet();

    static {
        allowedSqlTypeOfDefaultDb.add(SqlType.SELECT);
    }

    public static void checkPrivilege(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        // set privilegeVerifyItems to logicalPlan and clear
        // privilegeVerifyItems in privilegeContext
        //PrivilegeContext pc = PrivilegeContext.getPrivilegeContext();
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (pc != null && pc.getPrivilegeVerifyItems() != null) {
            executionPlan.setPrivilegeVerifyItems(pc.getPrivilegeVerifyItems());
            pc.setPrivilegeVerifyItems(null);
        }

        // verify privilege
        if (executionContext.isPrivilegeMode()) {
            verifyPrivilege(executionPlan, executionContext);
        }
    }

    private static void verifyPrivilege(ExecutionPlan executionPlan, ExecutionContext executionContext) {
        // verify privilege
        if (executionPlan.getPrivilegeVerifyItems() != null) {
            for (PrivilegeVerifyItem item : executionPlan.getPrivilegeVerifyItems()) {
                verifyPrivilege(item.getDb(), item.getTable(), item.getPrivilegePoint(), executionContext);
            }
        }
    }

    private static void verifyPrivilege(String db, String tb, PrivilegePoint priv, ExecutionContext executionContext) {
        // Don't check for system tables;
        tb = TStringUtil.normalizePriv(SQLUtils.normalizeNoTrim(tb));
        if (tb != null && SystemTables.contains(tb)) {
            return;
        }

        // the privilegeContext may not initialized
        // since some sqls may be executed from DRDS inner, but privilegeContext
        // is initialized by innerExecute() of FrontEndConnection.java
        PrivilegeContext pc = executionContext.getPrivilegeContext();
        if (StringUtils.isBlank(db)) {
            db = pc.getSchema();
        }
        db = (db == null) ? "" : db;
        db = TStringUtil.normalizePriv(SQLUtils.normalizeNoTrim(db));

        if (pc.getPolarUserInfo() == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_TABLE,
                priv.name(),
                tb,
                pc.getUser(),
                pc.getHost(),
                db);
        }

//        PolarInstPriv instPriv = pc.getPolarUserInfo().getInstPriv();
//        PolarDbPriv dbPriv = pc.getPolarUserInfo().getDbPriv(db);
//        PolarTbPriv tbPriv = pc.getPolarUserInfo().getTbPriv(db, tb);

        // check for meta_db
        if (StringUtils.equalsIgnoreCase(MetaDbSchema.NAME, db)) {
            return;
        }

        // check for default db
        if (StringUtils.equalsIgnoreCase(DefaultDbSchema.NAME, db)) {
            if (!allowedSqlTypeOfDefaultDb.contains(executionContext.getSqlType())) {
                throw new TddlRuntimeException(ErrorCode.ERR_NO_DB_ERROR, "No database selected");
            }
        }

        // check for information_schema
//        if (StringUtils.equalsIgnoreCase(InformationSchema.NAME, db)) {
//            if (priv != PrivilegePoint.SELECT) {
//                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_DB,
//                    pc.getUser(),
//                    pc.getHost(),
//                    db);
//            }
//            return;
//        }

        // check for cdc db
        if (StringUtils.equalsIgnoreCase(SystemDbHelper.CDC_DB_NAME, db)) {
            if (!StringUtils.equals(pc.getUser(), PolarPrivUtil.POLAR_ROOT)) {
                throw new TddlRuntimeException(ErrorCode.ERR_NO_DB_ERROR, "No database selected");
            }
        }

        PrivilegeKind privilege;
        switch (priv) {
        case INSERT:
            privilege = PrivilegeKind.INSERT;
            break;
        case DELETE:
            privilege = PrivilegeKind.DELETE;
            break;
        case UPDATE:
            privilege = PrivilegeKind.UPDATE;
            break;
        case SELECT:
            privilege = PrivilegeKind.SELECT;
            break;
        case CREATE:
            privilege = PrivilegeKind.CREATE;
            break;
        case DROP:
            privilege = PrivilegeKind.DROP;
            break;
        case INDEX:
            privilege = PrivilegeKind.INDEX;
            break;
        case ALTER:
            privilege = PrivilegeKind.ALTER;
            break;
        case CREATE_VIEW:
            privilege = PrivilegeKind.CREATE_VIEW;
            break;
        case SHOW_VIEW:
            privilege = PrivilegeKind.SHOW_VIEW;
            break;
        case REPLICATION_CLIENT:
            privilege = PrivilegeKind.REPLICATION_CLIENT;
            break;
        case REPLICATION_SLAVE:
            privilege = PrivilegeKind.REPLICATION_SLAVE;
            break;
        default:
            return;
        }

        Permission permission = Permission.from(db, tb, privilege);
        PermissionCheckContext context = new PermissionCheckContext(pc.getPolarUserInfo().getAccountId(),
            pc.getActiveRoles(), permission);
        boolean hasPrivilege = PolarPrivManager.getInstance().checkPermission(context);

        if (!hasPrivilege) {
            if (StringUtils.isNotBlank(tb)) {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_TABLE,
                    priv.name(),
                    tb,
                    pc.getUser(),
                    pc.getHost(),
                    db);
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_CHECK_PRIVILEGE_FAILED_ON_DB,
                    pc.getUser(),
                    pc.getHost(),
                    db);
            }
        }
    }
}
