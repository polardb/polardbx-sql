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

package com.alibaba.polardbx.gms.privilege;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;

/**
 * Privilege on some object.
 *
 * <p>
 * When privilege is null, it's used to check whether has usage(any) privilege on object.
 * </p>
 *
 * @author bairui.lrj
 * @since 5.4.9
 */
public class Permission {
    private static final String EMPTY = "";
    private final String database;
    private final String table;

    /**
     * Null when used to check any permission.
     */
    @Nullable
    private final PrivilegeKind privilege;
    private final PrivilegeScope scope;
    private final boolean isAnyTable;

    private Permission(String database, String table, PrivilegeKind privilege, PrivilegeScope scope) {
        this.database = database;
        this.table = table;
        this.privilege = privilege;
        this.scope = scope;
        this.isAnyTable = false;
    }

    private Permission(String database, String table,
                       PrivilegeKind privilege, PrivilegeScope scope,
                       boolean isAnyTable) {
        this.database = database;
        this.table = table;
        this.privilege = privilege;
        this.scope = scope;
        this.isAnyTable = isAnyTable;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public boolean isAnyTable() {
        return isAnyTable;
    }

    @Nullable
    public PrivilegeKind getPrivilege() {
        return privilege;
    }

    public boolean isAnyPermission() {
        return getPrivilege() == null;
    }

    public PrivilegeScope getScope() {
        return scope;
    }

    public static Permission instancePermission(PrivilegeKind privilege) {
        return new Permission(EMPTY, EMPTY, privilege, PrivilegeScope.INSTANCE);
    }

    public static Permission databasePermission(String database, PrivilegeKind privilege) {
        Preconditions.checkArgument(StringUtils.isNotBlank(database), "Database can't be blank!");

        return new Permission(database, EMPTY, privilege, PrivilegeScope.DATABASE);
    }

    public static Permission databasePermission(String database, boolean isAnyTable, PrivilegeKind privilege) {
        Preconditions.checkArgument(StringUtils.isNotBlank(database), "Database can't be blank!");

        return new Permission(database, EMPTY, privilege, PrivilegeScope.DATABASE, isAnyTable);
    }

    public static Permission tablePermission(String database, String table, PrivilegeKind privilege) {
        Preconditions.checkArgument(StringUtils.isNotBlank(database), "Database can't be blank!");
        Preconditions.checkArgument(StringUtils.isNotBlank(table), "Table can't be blank!");

        return new Permission(database, table, privilege, PrivilegeScope.TABLE);
    }

    public static Permission from(String database, String table, boolean isAnyTable, PrivilegeKind privilege) {
        Preconditions.checkNotNull(privilege, "Privilege can't be null!");
        if (StringUtils.isNotBlank(table)) {
            return tablePermission(database, table, privilege);
        } else if (StringUtils.isNotBlank(database)) {
            return databasePermission(database, isAnyTable, privilege);
        } else {
            return instancePermission(privilege);
        }
    }

    public static Permission anyPermission(String database, String table) {
        if (StringUtils.isNotBlank(table)) {
            return tablePermission(database, table, null);
        } else if (StringUtils.isNotBlank(database)) {
            return databasePermission(database, null);
        } else {
            return instancePermission(null);
        }
    }
}
