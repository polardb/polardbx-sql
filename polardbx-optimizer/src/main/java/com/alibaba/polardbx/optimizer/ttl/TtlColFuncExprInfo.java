package com.alibaba.polardbx.optimizer.ttl;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlNode;

/**
 * @author chenghui.lch
 */
public class TtlColFuncExprInfo {

    /**
     * The func expr for ttl-col from int-type to datetime-type
     */
    /**
     * The ttl col func expr replaced ttl-col as dynamic params
     */
    protected String normalizedTtlColFuncExprStr = null;
    protected boolean treatTtlColAsUnixTimestampSeconds = false;
    protected boolean treatTtlColAsUnixTimestampMillSeconds = false;
    protected boolean treatTtlColAsToDaysNumber = false;

    protected TtlColFuncExprInfo() {
    }

    public static TtlColFuncExprInfo buildTtlColFuncExprInfoByTtlColAst(SqlNode ttlColNodeAst) {
        boolean useFuncExprDef = (ttlColNodeAst instanceof SqlBasicCall);

        if (!useFuncExprDef) {
            return null;
        }

        TtlColFuncExprInfo funcExprInfo = new TtlColFuncExprInfo();
        SqlBasicCall ttlColFuncExpr = (SqlBasicCall) ttlColNodeAst;
        TtlUtil.TtlColumnFinder ttlColumnFinder = new TtlUtil.TtlColumnFinder();
        ttlColumnFinder.find(ttlColFuncExpr);
        TtlUtil.TtlColumnAsSqlDynamicParamReplacer
            dynamicNodeReplacer = new TtlUtil.TtlColumnAsSqlDynamicParamReplacer();
        SqlNode ttlFuncExprWithDynamicParamInput = ttlColFuncExpr.accept(dynamicNodeReplacer);

        String ttlFuncExprStrWithDynamicParamInput = ttlFuncExprWithDynamicParamInput.toString();
        boolean ttlColUseFromUnixTimeWithoutDiv = ttlColumnFinder.ttlColUseFromUnixTimeFuncWithoutDiv();
        boolean ttlColUseFromUnixTimeWithDiv = ttlColumnFinder.ttlColUseFromUnixTimeFuncWithDiv();
        boolean ttlColUseFromDays = ttlColumnFinder.ttlColUseFromDays();

        funcExprInfo.setNormalizedTtlColFuncExprStr(ttlFuncExprStrWithDynamicParamInput);
        funcExprInfo.setTreatTtlColAsUnixTimestampSeconds(ttlColUseFromUnixTimeWithoutDiv);
        funcExprInfo.setTreatTtlColAsUnixTimestampMillSeconds(ttlColUseFromUnixTimeWithDiv);
        funcExprInfo.setTreatTtlColAsToDaysNumber(ttlColUseFromDays);

        return funcExprInfo;
    }

    public String getNormalizedTtlColFuncExprStr() {
        return normalizedTtlColFuncExprStr;
    }

    public void setNormalizedTtlColFuncExprStr(String normalizedTtlColFuncExprStr) {
        this.normalizedTtlColFuncExprStr = normalizedTtlColFuncExprStr;
    }

    public boolean isTreatTtlColAsUnixTimestampSeconds() {
        return treatTtlColAsUnixTimestampSeconds;
    }

    public void setTreatTtlColAsUnixTimestampSeconds(boolean treatTtlColAsUnixTimestampSeconds) {
        this.treatTtlColAsUnixTimestampSeconds = treatTtlColAsUnixTimestampSeconds;
    }

    public boolean isTreatTtlColAsUnixTimestampMillSeconds() {
        return treatTtlColAsUnixTimestampMillSeconds;
    }

    public void setTreatTtlColAsUnixTimestampMillSeconds(boolean ttlColUseFromUnixTimeWithDiv) {
        this.treatTtlColAsUnixTimestampMillSeconds = ttlColUseFromUnixTimeWithDiv;
    }

    public boolean isTreatTtlColAsToDaysNumber() {
        return treatTtlColAsToDaysNumber;
    }

    public void setTreatTtlColAsToDaysNumber(boolean treatTtlColAsToDaysNumber) {
        this.treatTtlColAsToDaysNumber = treatTtlColAsToDaysNumber;
    }

    public TtlColFuncExprInfo copy() {
        TtlColFuncExprInfo newTtlColFuncExprInfo = new TtlColFuncExprInfo();
        newTtlColFuncExprInfo.setNormalizedTtlColFuncExprStr(normalizedTtlColFuncExprStr);
        newTtlColFuncExprInfo.setTreatTtlColAsUnixTimestampMillSeconds(treatTtlColAsUnixTimestampMillSeconds);
        newTtlColFuncExprInfo.setTreatTtlColAsUnixTimestampSeconds(treatTtlColAsUnixTimestampSeconds);
        newTtlColFuncExprInfo.setTreatTtlColAsToDaysNumber(treatTtlColAsToDaysNumber);
        return newTtlColFuncExprInfo;
    }
}
