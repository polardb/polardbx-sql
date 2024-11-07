package com.alibaba.polardbx.optimizer.core.rel.util;

import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class TargetTableInfo {

    /**
     * <pre>
     *     key : grpKey
     *     val:
     *      List[List<String>] : the same position of (sub)part for multi tables with join
     *            the i-th element of List<String>: the one phy table name of the multi tables with join on the same position
     * </pre>
     */
    protected Map<String, List<List<String>>> targetTables = null;

    /**
     * <pre>
     *   List[i]: the target table info for the i-th logical table name
     * </pre>
     */
    protected List<TargetTableInfoOneTable> targetTableInfoList;

    public TargetTableInfo() {
    }

    public List<TargetTableInfoOneTable> getTargetTableInfoList() {
        return targetTableInfoList;
    }

    public void setTargetTableInfoList(
        List<TargetTableInfoOneTable> targetTableInfoList) {
        this.targetTableInfoList = targetTableInfoList;
    }

    public Map<String, List<List<String>>> getTargetTables() {
        return targetTables;
    }

    public void setTargetTables(Map<String, List<List<String>>> targetTables) {
        this.targetTables = targetTables;
    }
}
