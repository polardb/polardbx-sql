package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author fangwu
 */
public class ColumnarPruneRecord {
    private String tableName;
    private String filter;
    public AtomicLong initIndexTime = new AtomicLong();
    public AtomicLong indexPruneTime = new AtomicLong();
    public AtomicInteger fileNum = new AtomicInteger();
    public AtomicInteger stripeNum = new AtomicInteger();
    public AtomicInteger rgNum = new AtomicInteger();
    public AtomicInteger rgLeftNum = new AtomicInteger();
    public AtomicInteger sortKeyPruneNum = new AtomicInteger();
    public AtomicInteger zoneMapPruneNum = new AtomicInteger();
    public AtomicInteger bitMapPruneNum = new AtomicInteger();

    public ColumnarPruneRecord(
        String tableName,
        String filter
    ) {
        this.tableName = tableName;
        this.filter = filter;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    @JsonCreator
    public ColumnarPruneRecord(@JsonProperty("tableName") String tableName,
                               @JsonProperty("filter") String filter,
                               @JsonProperty("initIndexTime") AtomicLong initIndexTime,
                               @JsonProperty("indexPruneTime") AtomicLong indexPruneTime,
                               @JsonProperty("fileNum") AtomicInteger fileNum,
                               @JsonProperty("stripeNum") AtomicInteger stripeNum,
                               @JsonProperty("rgNum") AtomicInteger rgNum,
                               @JsonProperty("rgLeftNum") AtomicInteger rgLeftNum,
                               @JsonProperty("sortKeyPruneNum") AtomicInteger sortKeyPruneNum,
                               @JsonProperty("zoneMapPruneNum") AtomicInteger zoneMapPruneNum,
                               @JsonProperty("bitMapPruneNum") AtomicInteger bitMapPruneNum
    ) {
        this.tableName = tableName;
        this.filter = filter;
        this.initIndexTime = initIndexTime;
        this.indexPruneTime = indexPruneTime;
        this.fileNum = fileNum;
        this.stripeNum = stripeNum;
        this.rgNum = rgNum;
        this.rgLeftNum = rgLeftNum;
        this.sortKeyPruneNum = sortKeyPruneNum;
        this.zoneMapPruneNum = zoneMapPruneNum;
        this.bitMapPruneNum = bitMapPruneNum;
    }

}
