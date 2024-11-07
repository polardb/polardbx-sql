package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import lombok.Data;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author chenghui.lch
 */
@Data
public class TtlTblFullDataFreeInfo {

    protected OneTblDataFreeInfo primDataFreeInfo = new OneTblDataFreeInfo();
    protected Map<String, OneTblDataFreeInfo> gsiDataFreeInfoMap =
        new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

    public TtlTblFullDataFreeInfo() {
    }

    public long getDataFreePercentAvg() {
        long dataLenSum = primDataFreeInfo.getDataLength().get();
        long dataFreeSum = primDataFreeInfo.getDataFree().get();
        for (Map.Entry<String, OneTblDataFreeInfo> dfInfoItem : gsiDataFreeInfoMap.entrySet()) {
            OneTblDataFreeInfo dfInfo = dfInfoItem.getValue();
            dataLenSum += dfInfo.getDataLength().get();
            dataFreeSum += dfInfo.getDataFree().get();
        }
        if (dataFreeSum == 0) {
            return 0;
        }
        long percentAvg = Math.round(dataFreeSum * 100 / dataLenSum);
        return percentAvg;
    }

    public long getRowDataLengthAvg() {
        long dataLenSum = primDataFreeInfo.getDataLength().get();
        for (Map.Entry<String, OneTblDataFreeInfo> dfInfoItem : gsiDataFreeInfoMap.entrySet()) {
            OneTblDataFreeInfo dfInfo = dfInfoItem.getValue();
            dataLenSum += dfInfo.getDataLength().get();
        }
        long tableRows = primDataFreeInfo.getTableRows().get();
        if (tableRows == 0) {
            return 0l;
        }
        long rowLenAvg = Math.round(dataLenSum / tableRows);
        return rowLenAvg;
    }

    @Data
    public static class OneTblDataFreeInfo {
        protected TableMeta tblMeta;
        protected AtomicLong dataLength = new AtomicLong(0L);
        protected AtomicLong dataFree = new AtomicLong(0L);
        protected AtomicLong tableRows = new AtomicLong(0L);

        public OneTblDataFreeInfo() {
        }

        public BigDecimal getDataFreePercent() {
            if (dataLength.get() == 0) {
                return new BigDecimal(0);
            }

            BigDecimal dataFreeDec = new BigDecimal(dataFree.get());
            BigDecimal dataLengthDec = new BigDecimal(dataLength.get());
            double dfPercentVal =
                dataFreeDec.doubleValue() * 100 / (dataLengthDec.doubleValue() + dataFreeDec.doubleValue());
            BigDecimal dfRatioDec = new BigDecimal(dfPercentVal);
            dfRatioDec = dfRatioDec.setScale(2, RoundingMode.HALF_UP);
            return dfRatioDec;
        }
    }

}
