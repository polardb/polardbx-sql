package com.alibaba.polardbx.gms.topology;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class ImportTableResult {
    protected String tableName;

    protected String action;
    protected String physicalCreateTableSql;
    protected String logicalCreateTableSql;
    protected String errMsg;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPhysicalCreateTableSql() {
        return physicalCreateTableSql;
    }

    public void setPhysicalCreateTableSql(String physicalCreateTableSql) {
        this.physicalCreateTableSql = physicalCreateTableSql;
    }

    public String getLogicalCreateTableSql() {
        return logicalCreateTableSql;
    }

    public void setLogicalCreateTableSql(String logicalCreateTableSql) {
        this.logicalCreateTableSql = logicalCreateTableSql;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }
}
