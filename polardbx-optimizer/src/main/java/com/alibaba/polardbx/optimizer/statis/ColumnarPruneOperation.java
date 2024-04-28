package com.alibaba.polardbx.optimizer.statis;

import org.roaringbitmap.RoaringBitmap;

import java.sql.Timestamp;
import java.util.function.Function;

/**
 * @author fangwu
 */
public class ColumnarPruneOperation {
    private String scanId;
    private String tableName;
    private String orcFileName;
    private String filter;
    private String action;
    private long period;
    private String result;
    private Timestamp timestamp;

    public ColumnarPruneOperation(
        String orcFileName,
        String filter,
        String action,
        long period,
        String result
    ) {
        this.orcFileName = orcFileName;
        this.filter = filter;
        this.action = action;
        this.period = period;
        this.result = result;
        this.timestamp = new Timestamp(System.currentTimeMillis());
    }

    public String getScanId() {
        return scanId;
    }

    public void setScanId(String scanId) {
        this.scanId = scanId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getOrcFileName() {
        return orcFileName;
    }

    public void setOrcFileName(String orcFileName) {
        this.orcFileName = orcFileName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public long getPeriod() {
        return period;
    }

    public void setPeriod(long period) {
        this.period = period;
    }

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }
}
