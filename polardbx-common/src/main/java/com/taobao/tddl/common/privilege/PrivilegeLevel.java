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
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.taobao.tddl.common.privilege.PrivilegePoint.PRIVILEGE_POINT_COMPARATOR;

public enum PrivilegeLevel implements Serializable {
    GLOBAL("GLOBAL", "user_priv", Privs.GLOBAL),
    DATABASE("DATABASE", "db_priv", Privs.DATABASE),
    TABLE("TABLE", "table_priv", Privs.TABLE);

    private static final long serialVersionUID = 462871818139310740L;

    private String name;

    private String table;

    private SortedSet<PrivilegePoint> privs;

    private PrivilegeLevel(String name, String table, SortedSet<PrivilegePoint> privs) {
        this.name = name;
        this.table = table;
        this.privs = privs;
    }

    public String getName() {
        return name;
    }

    public String getTable() {
        return table;
    }

    public SortedSet<PrivilegePoint> getPrivs() {
        return privs;
    }

    public static class Privs {
        public static final SortedSet<PrivilegePoint> GLOBAL = getGlobal();

        public static final SortedSet<PrivilegePoint> DATABASE = getDatabase();

        public static final SortedSet<PrivilegePoint> TABLE = getTable();

        private static SortedSet<PrivilegePoint> getGlobal() {
            SortedSet<PrivilegePoint> resultSet = new TreeSet<PrivilegePoint>(PRIVILEGE_POINT_COMPARATOR);
            resultSet.add(PrivilegePoint.CREATE);
            resultSet.add(PrivilegePoint.DROP);
            resultSet.add(PrivilegePoint.ALTER);
            resultSet.add(PrivilegePoint.INDEX);
            resultSet.add(PrivilegePoint.INSERT);
            resultSet.add(PrivilegePoint.DELETE);
            resultSet.add(PrivilegePoint.UPDATE);
            resultSet.add(PrivilegePoint.SELECT);
            resultSet.add(PrivilegePoint.RELOAD);
            resultSet.add(PrivilegePoint.SHUTDOWN);

            return Collections.unmodifiableSortedSet(resultSet);
        }

        private static SortedSet<PrivilegePoint> getDatabase() {
            SortedSet<PrivilegePoint> resultSet = new TreeSet<PrivilegePoint>(PRIVILEGE_POINT_COMPARATOR);
            resultSet.add(PrivilegePoint.CREATE);
            resultSet.add(PrivilegePoint.DROP);
            resultSet.add(PrivilegePoint.ALTER);
            resultSet.add(PrivilegePoint.INDEX);
            resultSet.add(PrivilegePoint.INSERT);
            resultSet.add(PrivilegePoint.DELETE);
            resultSet.add(PrivilegePoint.UPDATE);
            resultSet.add(PrivilegePoint.SELECT);
            return Collections.unmodifiableSortedSet(resultSet);
        }

        private static SortedSet<PrivilegePoint> getTable() {
            SortedSet<PrivilegePoint> resultSet = new TreeSet<PrivilegePoint>(PRIVILEGE_POINT_COMPARATOR);
            resultSet.add(PrivilegePoint.CREATE);
            resultSet.add(PrivilegePoint.DROP);
            resultSet.add(PrivilegePoint.ALTER);
            resultSet.add(PrivilegePoint.INDEX);
            resultSet.add(PrivilegePoint.INSERT);
            resultSet.add(PrivilegePoint.DELETE);
            resultSet.add(PrivilegePoint.UPDATE);
            resultSet.add(PrivilegePoint.SELECT);
            return Collections.unmodifiableSortedSet(resultSet);
        }
    }
}
