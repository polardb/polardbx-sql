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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Privileges that can be granted to accounts.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/grant.html">Grant</a>
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/revoke.html">Revoke</a>
 * @since 5.4.9
 */
public enum PrivilegeKind {
    /**
     * Select privilege.
     */
    SELECT("SELECT", "select_priv", PrivilegeScope.ALL),
    /**
     * Insert privilege.
     */
    INSERT("INSERT", "insert_priv", PrivilegeScope.ALL),
    /**
     * Update privilege.
     */
    UPDATE("UPDATE", "update_priv", PrivilegeScope.ALL),
    /**
     * Delete privilege.
     */
    DELETE("DELETE", "delete_priv", PrivilegeScope.ALL),
    /**
     * Create privilege.
     */
    CREATE("CREATE", "create_priv", PrivilegeScope.ALL),
    /**
     * Drop privilege.
     */
    DROP("DROP", "drop_priv", PrivilegeScope.ALL),
    /**
     * Index privilege.
     */
    INDEX("INDEX", "index_priv", PrivilegeScope.ALL),
    /**
     * Alter privilege.
     */
    ALTER("ALTER", "alter_priv", PrivilegeScope.ALL),
    /**
     * Show view privilege.
     */
    SHOW_VIEW("SHOW VIEW", "show_view_priv", PrivilegeScope.ALL),
    /**
     * Create view privilege.
     */
    CREATE_VIEW("CREATE VIEW", "create_view_priv", PrivilegeScope.ALL),
    /**
     * Create user privilege.
     */
    CREATE_USER("CREATE USER", "create_user_priv", PrivilegeScope.INSTANCE_ONLY),
    /**
     * Grant option privilege.
     */
    GRANT_OPTION("GRANT OPTION", "grant_priv", PrivilegeScope.ALL),
    /**
     * Check audit log.
     */
    SHOW_AUDIT_LOG("SHOW AUDIT LOG", "show_audit_log_priv", PrivilegeScope.INSTANCE_ONLY),

    REPLICATION_CLIENT("REPLICATION CLIENT", "replication_client_priv", PrivilegeScope.INSTANCE_ONLY),

    REPLICATION_SLAVE("REPLICATION SLAVE", "replication_slave_priv", PrivilegeScope.INSTANCE_ONLY),

    /**
     * metaDbPriv is special, user can NOT grant metaDbPriv to others, metDbPriv can be only set to 1 by updating meta
     * db directly.
     */
    METADB(null, "meta_db_priv", PrivilegeScope.INSTANCE_ONLY);

    private static final Map<PrivilegeScope, EnumSet<PrivilegeKind>> SCOPE_TO_KINDS = new HashMap<>();

    static {
        for (PrivilegeScope scope : PrivilegeScope.values()) {
            EnumSet<PrivilegeKind> kinds = EnumSet.copyOf(Arrays.stream(PrivilegeKind.values())
                .filter(kind -> kind.scopes.contains(scope))
                .collect(Collectors.toList()));

            SCOPE_TO_KINDS.put(scope, kinds);
        }
    }

    private static final Map<String, PrivilegeKind> SQL_TO_KIND = new HashMap<>(PrivilegeKind.values().length);

    static {
        for (PrivilegeKind kind : PrivilegeKind.values()) {
            if (!kind.hasSqlName()) {
                continue;
            }
            SQL_TO_KIND.put(kind.getSqlName(), kind);
        }
    }

    public static final String SQL_ALL = "ALL";
    public static final String SQL_ALL_PRIVILEGES = "ALL PRIVILEGES";

    /**
     * The keyword that appears in sql statement used to identify privilege kind.
     *
     * <p>
     * For example, in sql statement <code>GRANT <b>SELECT</b> ON *.* TO a;</code>, the keyword
     * <code><b>SELECT</b></code> is used to identify privilege kind {@link #SELECT}.
     * </p>
     *
     * <p>
     * This filed is <i>null </i>for some special privilege, which is not exposed throught sql statement, e.g. {@link
     * #METADB}.
     * </p>
     */
    private final String sqlName;
    /**
     * The column name which is used to store this privilege.
     *
     * <p>
     * Each privilege has a column in metadb, used to mark whether this privilege is granted to account.
     * </p>
     */
    private final String columnName;
    /**
     * Scopes that this privilege can belongs to.
     *
     * <p>
     * For example, one can grant SELECT privilege on {@literal instance(*.*)}, {@literal database(a.*)}, or {@literal
     * table(a.b)} to account, so scope for SELECT is all.
     * </p>
     *
     * @see PrivilegeScope
     */
    private final EnumSet<PrivilegeScope> scopes;

    PrivilegeKind(String sqlName, String columnName, EnumSet<PrivilegeScope> scopes) {
        this.sqlName = sqlName;
        this.columnName = columnName;
        this.scopes = scopes;
    }

    public String getSqlName() {
        return sqlName;
    }

    public boolean hasSqlName() {
        return Optional.ofNullable(getSqlName()).isPresent();
    }

    public String getColumnName() {
        return columnName;
    }

    public EnumSet<PrivilegeScope> getScopes() {
        return scopes;
    }

    public static EnumSet<PrivilegeKind> kindsByScope(PrivilegeScope scope) {
        return SCOPE_TO_KINDS.get(scope);
    }

    public static PrivilegeKind kindsByName(String name) {
        String normalizedName = name.toUpperCase();

        return Optional.ofNullable(SQL_TO_KIND.get(normalizedName))
            .orElseThrow(() -> new TddlNestableRuntimeException("Unrecognized privilege name: " + name));
    }

    public static boolean isAll(String name) {
        String normalizedName = normalizeName(name);
        return SQL_ALL.equals(normalizedName) || SQL_ALL_PRIVILEGES.equals(normalizedName);
    }

    private static String normalizeName(String name) {
        return name.toUpperCase();
    }
}
