package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
@Data
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
     * The formated currrent datetime
     * <pre>
     * For example,
     *   current time is 2024-06-26 11:04:56
     *    if the ttlTimeUnit is YEAR, so the formated current datetime str is : 2024-01-01 00:00:00;
     *   if the ttlTimeUnit is MONTH, so the formated current datetime str is : 2024-06-01 00:00:00;
     *   if the ttlTimeUnit is DAY, so the formated current datetime str is : 2024-06-26 00:00:00;
     *   if the ttlTimeUnit is HOUR, so the formated current datetime str is : 2024-06-26 11:00:00;
     * </pre>
     */
    protected String formatedCurrentDateTime;

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
     * The auto add missing parts sql of archived table
     */
    protected String arcTblAddPartsSql;

    /**
     * Label if need to add missing parts for ttl-tmp table
     */
    protected Boolean needAddPartsForArcTmpTbl = false;

    /**
     * Label if a archive task of some partition of curr oss table is running
     */
    protected Boolean partitionArchivingRunning = false;

    /**
     * The table rows of the whole arc-tmp table
     */
    protected Long arcTmpTableRows;

    /**
     * The data length of the whole arc-tmp table before running
     */
    protected Long arcTmpDataLengthBeforeRunning;

    /**
     * Label if change the arcState of the parts of arc_tmp tbl
     * between ttlColMinValPartPosition and ttlColCleanupUpperBoundPartPosition
     */
    protected Boolean needChangeReusingState = false;

    /**
     * New submitted the ready-state / reready-state parts to do oss archiving
     */
    protected List<String> newSubmittedArcParts = new ArrayList<>();

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
}
