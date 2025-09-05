package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;

import java.math.BigDecimal;

public class TtlJobContext {

    /**
     * The definition of ttl_info
     */
    protected TtlDefinitionInfo ttlInfo;

    /**
     * The current datetime, using the timezone of ttl
     * and iso formatter: yyyy-MM-dd HH:mm:ss
     */
    protected String currentDateTime;

    /**
     * The formated current datetime which is formated by the timeUnit defined by ttl_expr,
     * it is just using for computing the cleanupUpperBound
     * <pre>
     * For example,
     *   current time is 2024-06-26 11:04:56
     *    if the ttlTimeUnit is YEAR, so the formated current datetime str is : 2024-01-01 00:00:00;
     *   if the ttlTimeUnit is MONTH, so the formated current datetime str is : 2024-06-01 00:00:00;
     *   if the ttlTimeUnit is DAY, so the formated current datetime str is : 2024-06-26 00:00:00;
     *   if the ttlTimeUnit is HOUR, so the formated current datetime str is : 2024-06-26 11:00:00;
     * </pre>
     */
    protected String currentDateTimeFormatedByTtlExprUnit;

    /**
     * The formated current datetime which is formated by the timeUnit defined by ttl_part_interval,
     * it is just using for computing the newBoundValues
     * of new prebuild partitions of partition-level-based ttl-tbl and the archive cci
     * <pre>
     * For example,
     *   current time is 2024-06-26 11:04:56
     *    if the ttlTimeUnit is YEAR, so the formated current datetime str is : 2024-01-01 00:00:00;
     *   if the ttlTimeUnit is MONTH, so the formated current datetime str is : 2024-06-01 00:00:00;
     *   if the ttlTimeUnit is DAY, so the formated current datetime str is : 2024-06-26 00:00:00;
     *   if the ttlTimeUnit is HOUR, so the formated current datetime str is : 2024-06-26 11:00:00;
     * </pre>
     */
    protected String currentDateTimeFormatedByArcPartUnit;

    /**
     * The upper bound string of the expired data to be cleanup
     * <pre>
     *     For example:
     *     T1 is the following, and ttl expired interval 3 months:
     *          id     ttl_col
     *          1       2023-01-01
     *          2       2023-05-01
     *          3       2023-09-01
     *          4       2023-11-01
     *     , now assume that current time is 2024-01-01 00:00:00
     *     then the  cleanUpUpperBound is
     *          2024-01-01(now_time) - (3 months, ttl expired interval) = 2023-10-01
     *      so all the data of ttl_col < '2023-10-01' are expired data
     *      and can be cleanup.
     * </pre>
     */
    protected String cleanUpUpperBound;

    /**
     * The lowe bound string of the expired data to be cleanup
     * <pre>
     *     For example:
     *     T1 is the following, and ttl expired interval 3 months:
     *          id     ttl_col
     *          1       2023-02-03
     *          2       2023-05-01
     *          3       2023-09-01
     *          4       2023-11-01
     *     , now assume that current time is 2024-01-01 00:00:00
     *     then the  cleanUpLowerBound is
     *          2023-02-01( ttl_col min value after normalizing by the unit month ) + (1 month, ttl_unit is month) = 2023-03-01
     *      so 2023-03-01 is  the lower bound to be clean up.
     * </pre>
     */
    protected String cleanUpLowerBound;

    /**
     * The min value of ttl col, maybe null or zero
     */
    protected String ttlColMinValue;

    /**
     * Label if ttl_col_min_val is null value
     */
    protected Boolean ttlColMinValueIsNull = false;

    /**
     * Label if ttl_col_min_val is zero value
     */
    protected Boolean ttlColMinValueIsZero = false;

    /**
     * Label if ttl table is empty table
     */
    protected Boolean ttlTblIsEmpty = false;

    /**
     * The local index of ttl-col
     */
    protected String ttlColIndexName;

    /**
     * The force index expr for ttl col, default is ""
     * <pre>
     *     if found a local index for ttl col,
     *     the all the delete/select sql will
     *     use the force index expr,
     *     such as FORCE INDEX(idx_xxx)
     * </pre>
     */
    protected String ttlColForceIndexExpr = "";

    /**
     * The batch size of insert or delete on ttl_tbl
     */
    protected int dmlBatchSize = 1024;

    /**
     * The part position of the partition that the ttl_col min value located, start with 1
     */
    protected Integer partPositionForTtlColMinVal;

    /**
     * The part name of the partition that the ttl_col min value located
     */
    protected String partNameForTtlColMinVal;

    /**
     * The bound values of the previous partition of the ttl_col min value
     */
    protected String previousPartBoundOfTtlColMinVal;

    /**
     * The part position of the partition that the cleanup lower bound located, start with 1
     */
    protected Integer partPositionForCleanupLowerBound;

    /**
     * The part name of the partition that the cleanup lower bound located
     */
    protected String partNameForCleanupLowerBound;

    /**
     * The part position of the partition that the cleanup upper bound located, start with 1
     */
    protected Integer partPositionForCleanupUpperBound;

    /**
     * The part name of the partition that the cleanup upper bound located
     */
    protected String partNameForCleanupUpperBound;

    /**
     * Label if need to sync ttl_tmp tbl because its part state of ttl_tmp is changed
     */
    protected Boolean needChangeTtlTmpTblState = false;

    /**
     * The auto add missing parts sql of ttl-tmp table
     */
    protected String arcTmpTblAddPartsSql;

    /**
     * The auto drop expired parts sql of cci archived table
     */
    protected String arcTmpTblDropPartsSql;

//    /**
//     * The auto add missing parts sql of cci archived table
//     */
//    protected String arcTblAddPartsSql;

    /**
     * The auto add new parts sql of ttl table
     */
    protected String ttlTblAddPartsSql;

    /**
     * The maxBoundVal (ignore maxvalue part) of ttlTbl after adding new parts
     * during archiving by partition/subpartition
     */
    protected String ttlTblNewMaxBoundValAfterAddingNewParts;

    /**
     * The count of new added parts on ttl-tbl
     */
    protected Integer newAddedPartsCount = 0;

    /**
     * The auto drop expired parts sql of ttl table
     */
    protected String ttlTblDropPartsSql;

    /**
     * Label if need to add missing parts for ttl-tmp table
     */
    protected Boolean needAddPartsForArcTmpTbl = false;

    /**
     * Label if need to add new parts for ttl table
     */
    protected Boolean needAddPartsForTtlTbl = false;

    /**
     * Label if need to drop expired parts for ttl table
     */
    protected Boolean needDropPartsForTtlTbl = false;

    /**
     * Label if a archive task of some partition of curr oss table is running
     */
    protected Boolean partitionArchivingRunning = false;

    /**
     * The table rows of the whole arc-cci table
     */
    protected Long arcTmpTableRows;

    /**
     * The data length of the whole arc cci table before running
     */
    protected Long arcTmpDataLengthBeforeRunning;

    /**
     * Label if change the arcState of the parts of arc_tmp tbl
     * between ttlColMinValPartPosition and ttlColCleanupUpperBoundPartPosition
     */
    protected Boolean needChangeReusingState = false;

    /**
     * Label if current arcTmpTable if stop cleaning up the expired data right now
     * because its the disk space of arcTmpTbl has exceeded the disk limit and
     * wait the parts between ttlColMinValPartPosition and ttlColCleanupUpperBoundPartPosition
     * to perform oss archiving firstly to release disk space.
     */
    protected Boolean stopCleaningUpExpiredDataNow = false;

    /**
     * Label if need perform optimize-table operation for ttl-table on curr cleaning-up job
     */
    protected Boolean needPerformOptiTable = false;

    /**
     * The JobId for the new Submitted opti table of curr ttl-job
     */
    protected Long newOptiTableDdlJobId = 0L;

    /**
     * The data free of the primary of ttl-table
     */
    protected Long dataFreeOfTtlTblPrim = 0L;

    /**
     * The (data length + index length) of the primary of ttl-table
     */
    protected Long ttlTblPrimDataLength = 0L;

    /**
     * The max allowed percent of data free of ttl-table
     */
    protected BigDecimal dataFreePercentOfTtlTblPrim = new BigDecimal(0);

    /**
     * The percentAvg of data free of primary table and gsi table of ttl-table
     */
    protected Long dataFreePercentAvgOfTtlTbl = 0L;

    /**
     * The row lengthAvg of ttl table, unit: byte
     */
    protected Long rowLengthAvgOfTtlTbl = 0L;

    /**
     * The completed create ci sql of arcTbl
     */
    protected String createColumnarIndexSqlForArcTbl;

    /**
     * Use archive policy for dml-trans of ttl-jobs
     */
    protected Boolean useArcTrans = Boolean.valueOf(ConnectionParams.TTL_USE_ARCHIVE_TRANS_POLICY.getDefault());

    public TtlJobContext() {
    }

    public static TtlJobContext buildFromTtlInfo(TtlDefinitionInfo ttlInfo) {
        TtlJobContext ttlJobContext = new TtlJobContext();
        ttlJobContext.setTtlInfo(ttlInfo);
        ttlJobContext.setDmlBatchSize(TtlConfigUtil.getTtlJobDefaultBatchSize());
        ttlJobContext.setUseArcTrans(TtlConfigUtil.isUseArchiveTransPolicy());
        return ttlJobContext;
    }

    public TtlDefinitionInfo getTtlInfo() {
        return ttlInfo;
    }

    public void setTtlInfo(TtlDefinitionInfo ttlInfo) {
        this.ttlInfo = ttlInfo;
    }

    public String getCurrentDateTime() {
        return currentDateTime;
    }

    public void setCurrentDateTime(String currentDateTime) {
        this.currentDateTime = currentDateTime;
    }

    public String getCurrentDateTimeFormatedByTtlExprUnit() {
        return currentDateTimeFormatedByTtlExprUnit;
    }

    public void setCurrentDateTimeFormatedByTtlExprUnit(String currentDateTimeFormatedByTtlExprUnit) {
        this.currentDateTimeFormatedByTtlExprUnit = currentDateTimeFormatedByTtlExprUnit;
    }

    public String getCurrentDateTimeFormatedByArcPartUnit() {
        return currentDateTimeFormatedByArcPartUnit;
    }

    public void setCurrentDateTimeFormatedByArcPartUnit(String currentDateTimeFormatedByArcPartUnit) {
        this.currentDateTimeFormatedByArcPartUnit = currentDateTimeFormatedByArcPartUnit;
    }

    public String getCleanUpUpperBound() {
        return cleanUpUpperBound;
    }

    public void setCleanUpUpperBound(String cleanUpUpperBound) {
        this.cleanUpUpperBound = cleanUpUpperBound;
    }

    public String getCleanUpLowerBound() {
        return cleanUpLowerBound;
    }

    public void setCleanUpLowerBound(String cleanUpLowerBound) {
        this.cleanUpLowerBound = cleanUpLowerBound;
    }

    public String getTtlColMinValue() {
        return ttlColMinValue;
    }

    public void setTtlColMinValue(String ttlColMinValue) {
        this.ttlColMinValue = ttlColMinValue;
    }

    public Boolean getTtlColMinValueIsNull() {
        return ttlColMinValueIsNull;
    }

    public void setTtlColMinValueIsNull(Boolean ttlColMinValueIsNull) {
        this.ttlColMinValueIsNull = ttlColMinValueIsNull;
    }

    public Boolean getTtlColMinValueIsZero() {
        return ttlColMinValueIsZero;
    }

    public void setTtlColMinValueIsZero(Boolean ttlColMinValueIsZero) {
        this.ttlColMinValueIsZero = ttlColMinValueIsZero;
    }

    public Boolean getTtlTblIsEmpty() {
        return ttlTblIsEmpty;
    }

    public void setTtlTblIsEmpty(Boolean ttlTblIsEmpty) {
        this.ttlTblIsEmpty = ttlTblIsEmpty;
    }

    public String getTtlColIndexName() {
        return ttlColIndexName;
    }

    public void setTtlColIndexName(String ttlColIndexName) {
        this.ttlColIndexName = ttlColIndexName;
    }

    public String getTtlColForceIndexExpr() {
        return ttlColForceIndexExpr;
    }

    public void setTtlColForceIndexExpr(String ttlColForceIndexExpr) {
        this.ttlColForceIndexExpr = ttlColForceIndexExpr;
    }

    public int getDmlBatchSize() {
        return dmlBatchSize;
    }

    public void setDmlBatchSize(int dmlBatchSize) {
        this.dmlBatchSize = dmlBatchSize;
    }

    public Integer getPartPositionForTtlColMinVal() {
        return partPositionForTtlColMinVal;
    }

    public void setPartPositionForTtlColMinVal(Integer partPositionForTtlColMinVal) {
        this.partPositionForTtlColMinVal = partPositionForTtlColMinVal;
    }

    public String getPartNameForTtlColMinVal() {
        return partNameForTtlColMinVal;
    }

    public void setPartNameForTtlColMinVal(String partNameForTtlColMinVal) {
        this.partNameForTtlColMinVal = partNameForTtlColMinVal;
    }

    public String getPreviousPartBoundOfTtlColMinVal() {
        return previousPartBoundOfTtlColMinVal;
    }

    public void setPreviousPartBoundOfTtlColMinVal(String previousPartBoundOfTtlColMinVal) {
        this.previousPartBoundOfTtlColMinVal = previousPartBoundOfTtlColMinVal;
    }

    public Integer getPartPositionForCleanupLowerBound() {
        return partPositionForCleanupLowerBound;
    }

    public void setPartPositionForCleanupLowerBound(Integer partPositionForCleanupLowerBound) {
        this.partPositionForCleanupLowerBound = partPositionForCleanupLowerBound;
    }

    public String getPartNameForCleanupLowerBound() {
        return partNameForCleanupLowerBound;
    }

    public void setPartNameForCleanupLowerBound(String partNameForCleanupLowerBound) {
        this.partNameForCleanupLowerBound = partNameForCleanupLowerBound;
    }

    public Integer getPartPositionForCleanupUpperBound() {
        return partPositionForCleanupUpperBound;
    }

    public void setPartPositionForCleanupUpperBound(Integer partPositionForCleanupUpperBound) {
        this.partPositionForCleanupUpperBound = partPositionForCleanupUpperBound;
    }

    public String getPartNameForCleanupUpperBound() {
        return partNameForCleanupUpperBound;
    }

    public void setPartNameForCleanupUpperBound(String partNameForCleanupUpperBound) {
        this.partNameForCleanupUpperBound = partNameForCleanupUpperBound;
    }

    public Boolean getNeedChangeTtlTmpTblState() {
        return needChangeTtlTmpTblState;
    }

    public void setNeedChangeTtlTmpTblState(Boolean needChangeTtlTmpTblState) {
        this.needChangeTtlTmpTblState = needChangeTtlTmpTblState;
    }

    public String getArcTmpTblAddPartsSql() {
        return arcTmpTblAddPartsSql;
    }

    public void setArcTmpTblAddPartsSql(String arcTmpTblAddPartsSql) {
        this.arcTmpTblAddPartsSql = arcTmpTblAddPartsSql;
    }

    public String getArcTmpTblDropPartsSql() {
        return arcTmpTblDropPartsSql;
    }

    public void setArcTmpTblDropPartsSql(String arcTmpTblDropPartsSql) {
        this.arcTmpTblDropPartsSql = arcTmpTblDropPartsSql;
    }

    public String getTtlTblAddPartsSql() {
        return ttlTblAddPartsSql;
    }

    public void setTtlTblAddPartsSql(String ttlTblAddPartsSql) {
        this.ttlTblAddPartsSql = ttlTblAddPartsSql;
    }

    public String getTtlTblDropPartsSql() {
        return ttlTblDropPartsSql;
    }

    public void setTtlTblDropPartsSql(String ttlTblDropPartsSql) {
        this.ttlTblDropPartsSql = ttlTblDropPartsSql;
    }

    public Boolean getNeedAddPartsForArcTmpTbl() {
        return needAddPartsForArcTmpTbl;
    }

    public void setNeedAddPartsForArcTmpTbl(Boolean needAddPartsForArcTmpTbl) {
        this.needAddPartsForArcTmpTbl = needAddPartsForArcTmpTbl;
    }

    public Boolean getNeedAddPartsForTtlTbl() {
        return needAddPartsForTtlTbl;
    }

    public void setNeedAddPartsForTtlTbl(Boolean needAddPartsForTtlTbl) {
        this.needAddPartsForTtlTbl = needAddPartsForTtlTbl;
    }

    public Boolean getNeedDropPartsForTtlTbl() {
        return needDropPartsForTtlTbl;
    }

    public void setNeedDropPartsForTtlTbl(Boolean needDropPartsForTtlTbl) {
        this.needDropPartsForTtlTbl = needDropPartsForTtlTbl;
    }

    public Boolean getPartitionArchivingRunning() {
        return partitionArchivingRunning;
    }

    public void setPartitionArchivingRunning(Boolean partitionArchivingRunning) {
        this.partitionArchivingRunning = partitionArchivingRunning;
    }

    public Long getArcTmpTableRows() {
        return arcTmpTableRows;
    }

    public void setArcTmpTableRows(Long arcTmpTableRows) {
        this.arcTmpTableRows = arcTmpTableRows;
    }

    public Long getArcTmpDataLengthBeforeRunning() {
        return arcTmpDataLengthBeforeRunning;
    }

    public void setArcTmpDataLengthBeforeRunning(Long arcTmpDataLengthBeforeRunning) {
        this.arcTmpDataLengthBeforeRunning = arcTmpDataLengthBeforeRunning;
    }

    public Boolean getNeedChangeReusingState() {
        return needChangeReusingState;
    }

    public void setNeedChangeReusingState(Boolean needChangeReusingState) {
        this.needChangeReusingState = needChangeReusingState;
    }

    public Boolean getStopCleaningUpExpiredDataNow() {
        return stopCleaningUpExpiredDataNow;
    }

    public void setStopCleaningUpExpiredDataNow(Boolean stopCleaningUpExpiredDataNow) {
        this.stopCleaningUpExpiredDataNow = stopCleaningUpExpiredDataNow;
    }

    public Boolean getNeedPerformOptiTable() {
        return needPerformOptiTable;
    }

    public void setNeedPerformOptiTable(Boolean needPerformOptiTable) {
        this.needPerformOptiTable = needPerformOptiTable;
    }

    public Long getNewOptiTableDdlJobId() {
        return newOptiTableDdlJobId;
    }

    public void setNewOptiTableDdlJobId(Long newOptiTableDdlJobId) {
        this.newOptiTableDdlJobId = newOptiTableDdlJobId;
    }

    public Long getDataFreeOfTtlTblPrim() {
        return dataFreeOfTtlTblPrim;
    }

    public void setDataFreeOfTtlTblPrim(Long dataFreeOfTtlTblPrim) {
        this.dataFreeOfTtlTblPrim = dataFreeOfTtlTblPrim;
    }

    public Long getTtlTblPrimDataLength() {
        return ttlTblPrimDataLength;
    }

    public void setTtlTblPrimDataLength(Long ttlTblPrimDataLength) {
        this.ttlTblPrimDataLength = ttlTblPrimDataLength;
    }

    public BigDecimal getDataFreePercentOfTtlTblPrim() {
        return dataFreePercentOfTtlTblPrim;
    }

    public void setDataFreePercentOfTtlTblPrim(BigDecimal dataFreePercentOfTtlTblPrim) {
        this.dataFreePercentOfTtlTblPrim = dataFreePercentOfTtlTblPrim;
    }

    public Long getDataFreePercentAvgOfTtlTbl() {
        return dataFreePercentAvgOfTtlTbl;
    }

    public void setDataFreePercentAvgOfTtlTbl(Long dataFreePercentAvgOfTtlTbl) {
        this.dataFreePercentAvgOfTtlTbl = dataFreePercentAvgOfTtlTbl;
    }

    public Long getRowLengthAvgOfTtlTbl() {
        return rowLengthAvgOfTtlTbl;
    }

    public void setRowLengthAvgOfTtlTbl(Long rowLengthAvgOfTtlTbl) {
        this.rowLengthAvgOfTtlTbl = rowLengthAvgOfTtlTbl;
    }

    public String getCreateColumnarIndexSqlForArcTbl() {
        return createColumnarIndexSqlForArcTbl;
    }

    public void setCreateColumnarIndexSqlForArcTbl(String createColumnarIndexSqlForArcTbl) {
        this.createColumnarIndexSqlForArcTbl = createColumnarIndexSqlForArcTbl;
    }

    public Boolean getUseArcTrans() {
        return useArcTrans;
    }

    public void setUseArcTrans(Boolean useArcTrans) {
        this.useArcTrans = useArcTrans;
    }

    public String getTtlTblNewMaxBoundValAfterAddingNewParts() {
        return ttlTblNewMaxBoundValAfterAddingNewParts;
    }

    public void setTtlTblNewMaxBoundValAfterAddingNewParts(String ttlTblNewMaxBoundValAfterAddingNewParts) {
        this.ttlTblNewMaxBoundValAfterAddingNewParts = ttlTblNewMaxBoundValAfterAddingNewParts;
    }

    public Integer getNewAddedPartsCount() {
        return newAddedPartsCount;
    }

    public void setNewAddedPartsCount(Integer newAddedPartsCount) {
        this.newAddedPartsCount = newAddedPartsCount;
    }
}
