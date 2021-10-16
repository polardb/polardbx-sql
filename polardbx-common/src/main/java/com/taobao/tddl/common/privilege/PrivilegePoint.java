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

package com.taobao.tddl.common.privilege;

import java.io.Serializable;
import java.util.Comparator;

public enum PrivilegePoint implements Serializable {
    CREATE("CREATE", "create_priv"),
    DROP("DROP", "drop_priv"),
    ALTER("ALTER", "alter_priv"),
    INDEX("INDEX", "index_priv"),
    INSERT("INSERT", "insert_priv"),
    DELETE("DELETE", "delete_priv"),
    UPDATE("UPDATE", "update_priv"),
    SELECT("SELECT", "select_priv"),
    RELOAD("RELOAD", "reload_priv"),
    SHUTDOWN("SHUTDOWN", "shutdown_priv"),
    CREATE_VIEW("CREATE_VIEW", "create_view_priv"),
    SHOW_VIEW("SHOW_VIEW", "show_view_priv"),
    REPLICATION_CLIENT("REPLICATION_CLIENT", "replication_client_priv"),
    REPLICATION_SLAVE("REPLICATION_SLAVE", "replication_slave_priv"),
    ALL("ALL", "all_priv");

    private static final long serialVersionUID = 8976433752080985737L;

    private String name;

    private String columnName;

    public transient static final Comparator<PrivilegePoint> PRIVILEGE_POINT_COMPARATOR =
        new PrivilegePointComparator();

    public static String[] ALL_NAMES = new String[PrivilegePoint.values().length];

    static {
        for (PrivilegePoint pp : values()) {
            ALL_NAMES[pp.ordinal()] = pp.getName();
        }
    }

    private PrivilegePoint(String name, String columnName) {
        this.name = name;
        this.columnName = columnName;
    }

    public static PrivilegePoint parseByColumnName(String columnName) {
        for (PrivilegePoint privPoint : values()) {
            if (privPoint.getColumnName().equals(columnName)) {
                return privPoint;
            }
        }

        return null;
    }

    public static String[] stringValues() {
        return ALL_NAMES;
    }

    public String getName() {
        return name;
    }

    public String getColumnName() {
        return columnName;
    }

    public static class PrivilegePointComparator implements Comparator<PrivilegePoint>, Serializable {

        private static final long serialVersionUID = 4988167526033361217L;

        @Override
        public int compare(PrivilegePoint p1, PrivilegePoint p2) {
            int p1_ordinal, p2_ordinal;
            if (p1 == null && p2 == null) {
                return 0;
            }

            p1_ordinal = (p1 == null) ? -1 : p1.ordinal();
            p2_ordinal = (p2 == null) ? -1 : p2.ordinal();
            return p1_ordinal - p2_ordinal;
        }
    }
}
