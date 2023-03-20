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

package com.alibaba.polardbx.common.privilege;

import com.taobao.tddl.common.privilege.PrivilegePoint;

public class PrivilegeVerifyItem {
    private String db;
    private String table;
    private boolean isAnyTable = false;
    private PrivilegePoint privilegePoint;

    public PrivilegeVerifyItem(String db, String table, PrivilegePoint privilegePoint) {
        this.db = db;
        this.table = table;
        this.privilegePoint = privilegePoint;
    }

    public PrivilegeVerifyItem(String db, String table, boolean isAnyTable, PrivilegePoint privilegePoint) {
        this.db = db;
        this.table = table;
        this.isAnyTable = isAnyTable;
        this.privilegePoint = privilegePoint;
    }

    public PrivilegeVerifyItem() {

    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public PrivilegePoint getPrivilegePoint() {
        return privilegePoint;
    }

    public void setPrivilegePoint(PrivilegePoint privilegePoint) {
        this.privilegePoint = privilegePoint;
    }

    public boolean isAnyTable() {
        return isAnyTable;
    }

}
