package com.alibaba.polardbx.cdc.entity;

/**
 * Created by ziyang.lb
 **/
public class DDLExtInfo {
    /**
     * special for new ddl engine
     */
    private Long taskId;

    /**
     * 为CDC Meta组件提供创建物理表的依据
     */
    private String createSql4PhyTable;

    public Long getTaskId() {
        return taskId;
    }

    public void setTaskId(Long taskId) {
        this.taskId = taskId;
    }

    public String getCreateSql4PhyTable() {
        return createSql4PhyTable;
    }

    public void setCreateSql4PhyTable(String createSql4PhyTable) {
        this.createSql4PhyTable = createSql4PhyTable;
    }
}
