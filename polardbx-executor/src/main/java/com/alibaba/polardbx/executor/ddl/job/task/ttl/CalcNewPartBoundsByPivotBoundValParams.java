package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class CalcNewPartBoundsByPivotBoundValParams {
    protected TtlDefinitionInfo ttlInfo;
    protected String pivotBoundValue;
    protected String ttlTimeZone;
    protected Integer preBuildCnt;
    protected Integer postBuildCnt;
    protected String preBuildTargetBoundValue;
    protected TtlTimeUnit arcPartUnit;
    protected Long arcPartInterval;
    protected boolean useExpireOver;
    protected boolean ignoreRoutingCheck;
    protected TtlPartitionUtil.TtlPartitionRouter ttlPartitionRouter;
    protected ColumnMeta partColMeta;
    protected PartKeyLevel partKeyLevel;
    protected boolean usePartFunc;
    protected ExecutionContext ec;
    protected List<Pair<String, String>> newAddPartSpecInfosOutput;
    protected Map<String, TtlPartitionUtil.TtlColBoundValue> newAddPartSpecMappingsOutput =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    public CalcNewPartBoundsByPivotBoundValParams() {
    }

    public String getPivotBoundValue() {
        return pivotBoundValue;
    }

    public void setPivotBoundValue(String pivotBoundValue) {
        this.pivotBoundValue = pivotBoundValue;
    }

    public String getTtlTimeZone() {
        return ttlTimeZone;
    }

    public void setTtlTimeZone(String ttlTimeZone) {
        this.ttlTimeZone = ttlTimeZone;
    }

    public Integer getPreBuildCnt() {
        return preBuildCnt;
    }

    public void setPreBuildCnt(Integer preBuildCnt) {
        this.preBuildCnt = preBuildCnt;
    }

    public Integer getPostBuildCnt() {
        return postBuildCnt;
    }

    public void setPostBuildCnt(Integer postBuildCnt) {
        this.postBuildCnt = postBuildCnt;
    }

    public String getPreBuildTargetBoundValue() {
        return preBuildTargetBoundValue;
    }

    public void setPreBuildTargetBoundValue(String preBuildTargetBoundValue) {
        this.preBuildTargetBoundValue = preBuildTargetBoundValue;
    }

    public TtlTimeUnit getArcPartUnit() {
        return arcPartUnit;
    }

    public void setArcPartUnit(TtlTimeUnit arcPartUnit) {
        this.arcPartUnit = arcPartUnit;
    }

    public Long getArcPartInterval() {
        return arcPartInterval;
    }

    public void setArcPartInterval(Long arcPartInterval) {
        this.arcPartInterval = arcPartInterval;
    }

    public boolean isUseExpireOver() {
        return useExpireOver;
    }

    public void setUseExpireOver(boolean useExpireOver) {
        this.useExpireOver = useExpireOver;
    }

    public boolean isIgnoreRoutingCheck() {
        return ignoreRoutingCheck;
    }

    public void setIgnoreRoutingCheck(boolean ignoreRoutingCheck) {
        this.ignoreRoutingCheck = ignoreRoutingCheck;
    }

    public TtlPartitionUtil.TtlPartitionRouter getTtlPartitionRouter() {
        return ttlPartitionRouter;
    }

    public void setTtlPartitionRouter(
        TtlPartitionUtil.TtlPartitionRouter ttlPartitionRouter) {
        this.ttlPartitionRouter = ttlPartitionRouter;
    }

    public ColumnMeta getPartColMeta() {
        return partColMeta;
    }

    public void setPartColMeta(ColumnMeta partColMeta) {
        this.partColMeta = partColMeta;
    }

    public PartKeyLevel getPartKeyLevel() {
        return partKeyLevel;
    }

    public void setPartKeyLevel(PartKeyLevel partKeyLevel) {
        this.partKeyLevel = partKeyLevel;
    }

    public boolean isUsePartFunc() {
        return usePartFunc;
    }

    public void setUsePartFunc(boolean usePartFunc) {
        this.usePartFunc = usePartFunc;
    }

    public ExecutionContext getEc() {
        return ec;
    }

    public void setEc(ExecutionContext ec) {
        this.ec = ec;
    }

    public List<Pair<String, String>> getNewAddPartSpecInfosOutput() {
        return newAddPartSpecInfosOutput;
    }

    public void setNewAddPartSpecInfosOutput(
        List<Pair<String, String>> newAddPartSpecInfosOutput) {
        this.newAddPartSpecInfosOutput = newAddPartSpecInfosOutput;
    }

    public TtlDefinitionInfo getTtlInfo() {
        return ttlInfo;
    }

    public void setTtlInfo(TtlDefinitionInfo ttlInfo) {
        this.ttlInfo = ttlInfo;
    }

    public Map<String, TtlPartitionUtil.TtlColBoundValue> getNewAddPartSpecMappingsOutput() {
        return newAddPartSpecMappingsOutput;
    }

    public void setNewAddPartSpecMappingsOutput(
        Map<String, TtlPartitionUtil.TtlColBoundValue> newAddPartSpecMappingsOutput) {
        this.newAddPartSpecMappingsOutput = newAddPartSpecMappingsOutput;
    }
}
