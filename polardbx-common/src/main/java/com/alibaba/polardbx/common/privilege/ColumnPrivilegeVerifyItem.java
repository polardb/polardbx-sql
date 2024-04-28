package com.alibaba.polardbx.common.privilege;

import com.taobao.tddl.common.privilege.PrivilegePoint;

/**
 * @author pangzhaoxing
 */
public class ColumnPrivilegeVerifyItem extends PrivilegeVerifyItem {
    private String column;

    public ColumnPrivilegeVerifyItem(String db, String table, String column, PrivilegePoint privilegePoint) {
        super(db, table, privilegePoint);
        this.column = column;
    }

    public String getColumn() {
        return column;
    }
}
