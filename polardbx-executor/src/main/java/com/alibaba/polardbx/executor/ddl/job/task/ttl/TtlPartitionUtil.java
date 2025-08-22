package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.Monotonicity;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionFunctionBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import lombok.Data;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author chenghui.lch
 */
public class TtlPartitionUtil {

    @Data
    public static class CreateArcCciPartByDefCalcParams {

        protected TableMeta ttlTblMeta;
        protected TtlDefinitionInfo ttlInfo;
        protected String arcTableSchema;
        protected String arcTableName;
        protected Boolean buildForCci;
        protected PartKeyLevel targetPartLevel;
        protected String pivotPointValStr;
        protected String preBuildTargetBoundValStr;
        protected ExecutionContext ec;

        public CreateArcCciPartByDefCalcParams() {
        }
    }

    @Data
    public static class CreateArcCciPartByDefResult {
        protected CreateArcCciPartByDefCalcParams params;

        /**
         * Item of PartSpec:
         * key of pair: newPartSpecName
         * val of pair: newPartSpecBoundStr
         */
        protected List<Pair<String, String>> newCreatedPartSpecInfos = new ArrayList<>();

        public CreateArcCciPartByDefResult() {
        }

        public String generateCreateCciPartBySql() {
            TtlDefinitionInfo ttlDefinitionInfo = params.getTtlInfo();
            String arcTblSchema = params.getArcTableSchema();
            String arcTblName = params.getArcTableName();
            ExecutionContext executionContext = params.getEc();

            List<String> allPartNamesToBeAdded = new ArrayList<>();
            List<String> allPartBndValsToBeAdded = new ArrayList<>();
            for (int i = 0; i < newCreatedPartSpecInfos.size(); i++) {
                Pair<String, String> newSpec = newCreatedPartSpecInfos.get(i);
                allPartNamesToBeAdded.add(newSpec.getKey());
                allPartBndValsToBeAdded.add(newSpec.getValue());
            }

            String createCiSqlForArcTbl =
                TtlTaskSqlBuilder.buildCreateColumnarIndexSqlForArcTbl(ttlDefinitionInfo, arcTblSchema, arcTblName,
                    allPartBndValsToBeAdded,
                    allPartNamesToBeAdded, executionContext);

            return createCiSqlForArcTbl;
        }
    }

    @Data
    public static class BuildPrePartCalcParams {

        protected TtlDefinitionInfo ttlInfo;
        protected Boolean buildForCci;
        protected TableMeta tarTblMeta;
        protected PartKeyLevel targetPartLevel;
        protected String pivotPointValStr;
        protected String preBuildTargetBoundValStr;

        protected Integer preBuildNewPartCount;
        protected Integer postBuildNewPartCount;
        protected Boolean expiredByOverPartCount;
        protected ExecutionContext ec;

        public BuildPrePartCalcParams() {
        }

        public Boolean getBuildForCci() {
            return buildForCci;
        }

        public void setBuildForCci(Boolean buildForCci) {
            this.buildForCci = buildForCci;
        }

        public String getPreBuildTargetBoundValStr() {
            return preBuildTargetBoundValStr;
        }

        public void setPreBuildTargetBoundValStr(String preBuildTargetBoundValStr) {
            this.preBuildTargetBoundValStr = preBuildTargetBoundValStr;
        }
    }

    @Data
    public static class BuildPrePartCalcResult {
        protected BuildPrePartCalcParams params;
        protected Boolean needAddNewPartCount = false;
        protected Boolean foundMaxValPart = false;
        protected String maxValPartName = null;
        /**
         * Item of PartSpec:
         * key of pair: newPartSpecName
         * val of pair: newPartSpecBoundStr
         */
        protected List<Pair<String, String>> newAddPartSpecInfos = new ArrayList<>();

        public BuildPrePartCalcResult() {
        }

        public boolean needAddParts() {
            return !newAddPartSpecInfos.isEmpty();
        }

        public String getNewMaxBoundValueAfterAddingParts() {
            if (newAddPartSpecInfos.isEmpty()) {
                return null;
            }
            String newMaxBndValStr = newAddPartSpecInfos.get(newAddPartSpecInfos.size() - 1).getValue();
            return newMaxBndValStr;
        }

        public String generateAddPartsSql(String sqlHint) {

            boolean isAlterPartFofCci = this.params.getBuildForCci();
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            String primTableSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String primTblName = ttlInfo.getTtlInfoRecord().getTableName();
            String cciName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
            PartKeyLevel tarPartLevel = this.params.getTargetPartLevel();
            ExecutionContext ec = params.getEc();
            boolean useSubPart = tarPartLevel == PartKeyLevel.SUBPARTITION_KEY;

            List<String> newPartNameList = new ArrayList<>();
            List<String> newPartBndStrList = new ArrayList<>();
            for (int i = 0; i < newAddPartSpecInfos.size(); i++) {
                Pair<String, String> newPart = newAddPartSpecInfos.get(i);
                String newPartName = newPart.getKey();
                String newPartBndStr = newPart.getValue();

                newPartNameList.add(newPartName);
                newPartBndStrList.add(newPartBndStr);
            }

            String partBoundDefs = "";
            List<String> normalizedNewAddPartBoundList =
                normalizedRangePartBoundValueList(ttlInfo, newPartBndStrList, isAlterPartFofCci, tarPartLevel, ec);
            for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
                String bndStr = normalizedNewAddPartBoundList.get(i);
                String partNameStr = newPartNameList.get(i);
                String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
                String part = null;
                if (useSubPart) {
                    part = String.format("SUBPARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                } else {
                    part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
                }
                if (!partBoundDefs.isEmpty()) {
                    partBoundDefs += ",\n";
                }
                partBoundDefs += part;
            }

            String alterContentPart = "";

            if (foundMaxValPart) {
                if (useSubPart) {
                    alterContentPart = String.format(
                        "SPLIT SUBPARTITION `%s` INTO ( \n%s, SUBPARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                        maxValPartName, partBoundDefs, maxValPartName);
                } else {
                    alterContentPart =
                        String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) )",
                            maxValPartName, partBoundDefs, maxValPartName);
                }

            } else {
                if (useSubPart) {
                    alterContentPart = String.format("ADD SUBPARTITION ( %s )", partBoundDefs);
                } else {
                    alterContentPart = String.format("ADD PARTITION ( %s )", partBoundDefs);
                }
            }

            String skipDdlTasks =
                ec.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
            String queryHint = sqlHint;
            if (!StringUtils.isEmpty(skipDdlTasks)) {
                queryHint =
                    TtlTaskSqlBuilder.addCciHint(queryHint, String.format("SKIP_DDL_TASKS=\"%s\"", skipDdlTasks));
            }

            String alterPartStmt = "";
            if (!isAlterPartFofCci) {
                alterPartStmt = String.format("%s ALTER TABLE `%s`.`%s` %s", queryHint, primTableSchema, primTblName,
                    alterContentPart);
            } else {
                alterPartStmt =
                    String.format("%s ALTER INDEX `%s` ON TABLE `%s`.`%s` %s", queryHint, cciName, primTableSchema,
                        primTblName, alterContentPart);
            }

            return alterPartStmt;
        }
    }

    @Data
    public static class CleanupPostPartsCalcParams {
        protected TtlDefinitionInfo ttlInfo;
        protected boolean cleanupPostPastForCci;
        protected PartKeyLevel targetPartLevel;
        protected String cleanupUpperBoundDatetimeStr;
        protected Integer newAddPartsCount = 0;
        protected ExecutionContext ec;

        public CleanupPostPartsCalcParams() {
        }
    }

    @Data
    public static class CleanupPostPartsCalcResult {
        protected CleanupPostPartsCalcParams params;
        protected List<String> partNamesToBeDropped = new ArrayList<>();

        public CleanupPostPartsCalcResult() {
        }

        public boolean needDropParts() {
            return !partNamesToBeDropped.isEmpty();
        }

        public String generateDropPartsStmtSql(String queryHint) {

            if (partNamesToBeDropped.isEmpty()) {
                return "";
            }

            if (queryHint == null) {
                queryHint = "";
            }

            String partNameListStr = "";
            for (int i = 0; i < partNamesToBeDropped.size(); i++) {
                if (i > 0) {
                    partNameListStr += ",";
                }
                partNameListStr += String.format("`%s`", partNamesToBeDropped.get(i));
            }

            String dropPartSpecListSql = "";
            PartKeyLevel partKeyLevel = params.getTargetPartLevel();
            if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
                dropPartSpecListSql = String.format("DROP SUBPARTITION %s", partNameListStr);
            } else {
                dropPartSpecListSql = String.format("DROP PARTITION %s", partNameListStr);
            }

            String alterTblDropStmt = "";
            TtlDefinitionInfo ttlInfo = params.getTtlInfo();
            boolean isDropPartsForCci = params.isCleanupPostPastForCci();
            String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            String ttlTblCciName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
            if (isDropPartsForCci) {
                alterTblDropStmt =
                    String.format("%s ALTER INDEX %s ON TABLE `%s`.`%s` %s", queryHint, ttlTblCciName, ttlTblSchema,
                        ttlTblName, dropPartSpecListSql);
            } else {
                alterTblDropStmt = String.format("%s ALTER TABLE `%s`.`%s` %s", queryHint, ttlTblSchema, ttlTblName,
                    dropPartSpecListSql);
            }

            return alterTblDropStmt;
        }
    }

    public static BuildPrePartCalcResult calcBuildPrePartSpecs(BuildPrePartCalcParams params) {
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        TableMeta tarTblMeta = params.getTarTblMeta();
        PartKeyLevel partLevel = params.getTargetPartLevel();
        boolean calcPrePartSpecsForCci = params.getBuildForCci();

        ExecutionContext ec = params.getEc();
        String pivotPointValStr = params.getPivotPointValStr();
        String preBuildTargetBoundValStr = params.getPreBuildTargetBoundValStr();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        Integer arcPartInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
        Boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();

        Integer preBuildPartCnt = params.getPreBuildNewPartCount();
        Integer postBuildPartCnt = params.getPostBuildNewPartCount();
        List<Pair<String, String>> newAddPartSpecInfosOutputResult = new ArrayList<>();

        PivotPointResult pivotPointResult =
            calcAndBuildPivotPointResult(pivotPointValStr, preBuildTargetBoundValStr, calcPrePartSpecsForCci, ttlInfo,
                ec);

        CalcNewPartBoundsByPivotBoundValParams calcParams = new CalcNewPartBoundsByPivotBoundValParams();
        calcParams.setTtlInfo(ttlInfo);
        calcParams.setPartKeyLevel(partLevel);
        calcParams.setPivotBoundValue(pivotPointResult.getPreBuildPivotPointStr());
        calcParams.setTtlTimeZone(ttlTimeZone);
        calcParams.setPreBuildCnt(preBuildPartCnt);
        calcParams.setPostBuildCnt(postBuildPartCnt);
        calcParams.setArcPartUnit(arcPartTimeUnit);
        calcParams.setArcPartInterval(Long.valueOf(arcPartInterval));
        calcParams.setPreBuildTargetBoundValue(pivotPointResult.getPreBuildTargetBoundValStr());
        calcParams.setIgnoreRoutingCheck(false);
        calcParams.setUseExpireOver(useExpireOver);
        calcParams.setEc(ec);
        calcParams.setNewAddPartSpecInfosOutput(newAddPartSpecInfosOutputResult);

        CalcNewPartBoundsByPivotBoundValWithTblMetaParams buildParams =
            new CalcNewPartBoundsByPivotBoundValWithTblMetaParams();
        buildParams.setCalcParams(calcParams);
        buildParams.setTarTblMeta(tarTblMeta);
        buildParams.setTarPartKeyLevel(partLevel);
        calcNewPartBoundsByPivotBoundValWithTblMeta(buildParams);

        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartBy(tarTblMeta, partLevel);

        /**
         * Find the last not one partition
         */
        String maxValPartName = null;
        List<PartitionSpec> tarPartSpecList = partBy.getPartitions();
        int partCnt = tarPartSpecList.size();
        PartitionSpec lastPart = null;
        PartitionSpec lastButOnePart = null;
        PartitionSpec lastNonMaxValPart = null;
        if (partCnt > 0) {
            lastPart = tarPartSpecList.get(partCnt - 1);
            if (partCnt > 1) {
                lastButOnePart = tarPartSpecList.get(partCnt - 2);
            }
        }

        /**
         * Find the last non-max-val partition
         */
        boolean foundMaxValPart = false;
        if (lastPart != null) {
            foundMaxValPart = lastPart.getBoundSpec().containMaxValues();
            if (foundMaxValPart) {
                maxValPartName = lastPart.getName();
                if (lastButOnePart != null) {
                    lastNonMaxValPart = lastButOnePart;
                }
            } else {
                lastNonMaxValPart = lastPart;
            }
        }

        /**
         * Mark sure that the bound values of
         * all new parts are more than the bound value of the all the non-max-val part
         */
        Map<String, TtlColBoundValue> newPartBndValMappings =
            buildParams.getCalcParams().getNewAddPartSpecMappingsOutput();
        List<Pair<String, String>> finalNewAddPartSpecInfos = new ArrayList<>();
        if (lastNonMaxValPart != null) {
            SearchDatumInfo bndValOfLastNonMaxValPart = lastNonMaxValPart.getBoundSpec().getSingleDatum();
            PartitionBoundValueKind bndValKind = bndValOfLastNonMaxValPart.getSingletonValue().getValueKind();
            if (bndValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                PartitionField fldOfBndValOfLastNonMaxValPart =
                    bndValOfLastNonMaxValPart.getSingletonValue().getValue();

//                DataType partColDt = partBy.getPartitionColumnTypeList().get(0);
                PartitionIntFunction partIntFunc = partBy.getPartIntFunc();
                ExecutionContext newEc = ec.copy();
                InternalTimeZone tz = TimeZoneUtils.convertFromMySqlTZ(ttlTimeZone);
                newEc.setTimeZone(tz);
                for (int i = 0; i < newAddPartSpecInfosOutputResult.size(); i++) {
                    Pair<String, String> newPart = newAddPartSpecInfosOutputResult.get(i);
                    String newPartName = newPart.getKey();
//                    String newPartBndStr = newPart.getValue();
                    TtlColBoundValue newPartBndVal = newPartBndValMappings.get(newPartName);

                    PartitionField fldOfNewPartBnd = newPartBndVal.getPartColValFld();
                    PartitionField partFuncFldOfNewPartBnd = null;
                    if (partIntFunc != null) {
                        partFuncFldOfNewPartBnd =
                            PartitionPrunerUtils.buildPartFieldByEvalPartFuncExpr(fldOfNewPartBnd, partBy, newEc);
                    } else {
                        partFuncFldOfNewPartBnd = fldOfNewPartBnd;
                    }

                    /**
                     * Mark sure that the bound values of
                     * all new parts are more than the bound value of the all the non-max-val part
                     */
                    if (partFuncFldOfNewPartBnd.compareTo(fldOfBndValOfLastNonMaxValPart) > 0) {
                        finalNewAddPartSpecInfos.add(newPart);
                    }
                }
            }
        }

        BuildPrePartCalcResult result = new BuildPrePartCalcResult();
        result.setParams(params);
        result.setMaxValPartName(maxValPartName);
        result.setFoundMaxValPart(!StringUtils.isEmpty(maxValPartName));
        result.setNewAddPartSpecInfos(finalNewAddPartSpecInfos);

        return result;
    }

    protected static @NotNull PivotPointResult calcAndBuildPivotPointResult(String pivotPointValStr,
                                                                            String specifiedPreBuildTargetBoundValStr,
                                                                            boolean calcPrePartSpecsForCci,
                                                                            TtlDefinitionInfo ttlInfo,
                                                                            ExecutionContext ec) {
        Boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
        Boolean archiveByPartition = ttlInfo.performArchiveByPartitionOrSubPartition();

        String preBuildTargetBoundValStr = null;
        /**
         * when using expireOver policy, the input of pivotPointValStr will be null
         */
        String preBuildPivotPointStr = pivotPointValStr;
        if (calcPrePartSpecsForCci && useExpireOver && archiveByPartition) {
            String ttlTblSchemaName = ttlInfo.getTtlInfoRecord().getTableSchema();
            String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
            TableMeta primTableMeta = ec.getSchemaManager(ttlTblSchemaName).getTable(ttlTblName);

            /**
             * Fetch the partInfo for primTbl
             */
            PartitionInfo primPartInfo = primTableMeta.getPartitionInfo();
            PartKeyLevel primTtlPartLevel = PartKeyLevel.PARTITION_KEY;
            if (ttlInfo.performArchiveBySubPartition()) {
                primTtlPartLevel = PartKeyLevel.SUBPARTITION_KEY;
            }

            /**
             * Fetch the minBndVal and maxBndVal from primTtlPartInfo (ignore max-val part)
             */
            Pair<String, SearchDatumInfo> minBndValDatumInfoIgnoreMaxValBndOfPrimTbl =
                TtlPartitionUtil.findMinPartBoundValAndPartNameFromNonMaxValParts(primPartInfo, primTtlPartLevel);
            Pair<String, SearchDatumInfo> maxBndValDatumInfoIgnoreMaxValBndOfPrimTbl =
                TtlPartitionUtil.findMaxPartBoundValAndPartNameFromNonMaxValParts(primPartInfo, primTtlPartLevel);

            /**
             * The addPartSql of cci on expireOver policy must include all the bound range
             * from both
             * minBndVal of primTtlPartInfo (preBuildPivotPointStr)
             * and
             * maxBndVal of primTtlPartInfo (preBuildTargetBoundValStr)
             */
            BuildPartFieldStringParams params = BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo);
            preBuildPivotPointStr = convertPartSpecBoundValToString(
                minBndValDatumInfoIgnoreMaxValBndOfPrimTbl.getValue().getSingletonValue(), params);
            preBuildTargetBoundValStr = convertPartSpecBoundValToString(
                maxBndValDatumInfoIgnoreMaxValBndOfPrimTbl.getValue().getSingletonValue(), params);
            if (specifiedPreBuildTargetBoundValStr != null) {
                preBuildTargetBoundValStr = specifiedPreBuildTargetBoundValStr;
            }
        }
        String formatedPreBuildPivotPointStr = null;
        if (useExpireOver) {
            /**
             * when using expireOver policy,
             * the preBuildPivotPointStr from the first part bound of ttlTblPart,
             * maybe a non-formated bound value,
             * so here do the formatting.
             */
            formatedPreBuildPivotPointStr =
                TtlJobUtil.formatPivotPointStringByTtlUnitIfNeed(ec, ttlInfo, preBuildPivotPointStr);
        } else {
            formatedPreBuildPivotPointStr = preBuildPivotPointStr;
        }

        PivotPointResult result = new PivotPointResult(formatedPreBuildPivotPointStr, preBuildTargetBoundValStr);
        return result;
    }

    private static String convertPartSpecBoundValToString(PartitionBoundVal boundVal,
                                                          BuildPartFieldStringParams params) {
        return convertPartFieldToString(boundVal.getValue(), params);
    }

    private static String convertPartFieldToString(PartitionField partBndFld,
                                                   BuildPartFieldStringParams params) {
        if (partBndFld.isNull()) {
            return null;
        }

        String partFldStr = partBndFld.stringValue().toStringUtf8();
        boolean isDateType = DataTypeUtil.isDateType(partBndFld.dataType());
        if (params != null) {
            if (params.isForceReturnPartFldString()) {
                if (isDateType) {
                    partFldStr = partBndFld.datetimeValue().toDatetimeString(0);
                }
                return partFldStr;
            }
            boolean needConvert = params.isNeedConvertNumberToTimestampOrDate();
            if (needConvert) {
                DataType dt = partBndFld.dataType();
                if (!DataTypeUtil.isNumberSqlType(dt)) {
                    return partFldStr;
                }
                boolean needConvertToDate = params.isNeedConvertNumberToDate();
                if (needConvertToDate) {
                    //...
                    throw new UnsupportedOperationException();
                }

                long uxtsNumLongVal = partBndFld.longValue();
                boolean treatNumAsTsMillSec = params.isTreatAsUnixTimestampMillsSec();
                if (treatNumAsTsMillSec) {
                    uxtsNumLongVal = uxtsNumLongVal / 1000;
                }
                String targetTz = params.getTargetTimeZone();
                String tsStr = convertUnixTimestampToZonedDateTimeString(uxtsNumLongVal, targetTz);
                partFldStr = tsStr;
            }
        }
        return partFldStr;
    }

    public static String convertUnixTimestampToZonedDateTimeString(long unixTimestampNum,
                                                                   String tarTimeZone) {
        long timestamp = unixTimestampNum;
        Instant instant = Instant.ofEpochSecond(timestamp);
        ZoneId targetTz = ZoneId.of(tarTimeZone);
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, targetTz);
        DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
        String formattedDateTime = dateTime.format(formatter);
        return formattedDateTime;
    }

    public static long convertZonedDateTimeStringToUnixTimestamp(
        String dateTimeString,
        String tarTimeZone
    ) {
        String timeZone = tarTimeZone; // e.g：'+08:00'
        DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
        ZonedDateTime zonedDateTime = ZonedDateTime.of(
            java.time.LocalDateTime.parse(dateTimeString, formatter),
            ZoneId.of(timeZone)
        );
        long unixTimestamp = zonedDateTime.toEpochSecond();
        return unixTimestamp;
    }

    private static class PivotPointResult {
        public final String preBuildPivotPointStr;
        public final String preBuildTargetBoundValStr;

        public PivotPointResult(String preBuildPivotPointStr, String preBuildTargetBoundValStr) {
            this.preBuildPivotPointStr = preBuildPivotPointStr;
            this.preBuildTargetBoundValStr = preBuildTargetBoundValStr;
        }

        public String getPreBuildPivotPointStr() {
            return preBuildPivotPointStr;
        }

        public String getPreBuildTargetBoundValStr() {
            return preBuildTargetBoundValStr;
        }
    }

    public static CreateArcCciPartByDefResult calcCreateArcCciPartByDef(CreateArcCciPartByDefCalcParams params) {

        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        PartKeyLevel cciPartLevel = PartKeyLevel.PARTITION_KEY;

        ExecutionContext ec = params.getEc();
        TableMeta ttlTblMeta = params.getTtlTblMeta();
        String pivotPointValStr = params.getPivotPointValStr();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();

        Integer ttlInterval = ttlInfo.getTtlInfoRecord().getTtlInterval();
        TtlTimeUnit ttlUnitVal = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getTtlUnit());
        Integer arcPartInterval = ttlInfo.getTtlInfoRecord().getArcPartInterval();
        TtlTimeUnit arcPartTimeUnit = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit());
        boolean useExpireOver = ttlInfo.useExpireOverPartitionsPolicy();
        boolean preBuildForCci = params.getBuildForCci();

        int preBuiltPartCntForFuture = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();
        int postBuiltPartCntForPast = ttlInfo.getTtlInfoRecord().getArcPostPartCnt();

        List<Pair<String, String>> newAddPartSpecInfosOutputResult = new ArrayList<>();
        int finalPreBuildPartCnt =
            TtlJobUtil.decidePreBuiltPartCnt(preBuiltPartCntForFuture, ttlInterval, ttlUnitVal, arcPartInterval,
                arcPartTimeUnit);

        PivotPointResult pivotPointResult =
            calcAndBuildPivotPointResult(pivotPointValStr, null, preBuildForCci, ttlInfo, ec);

        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        ColumnMeta ttlColMeta = ttlTblMeta.getColumn(ttlColName);

        /**
         * Here need check if partCol use part func
         */
        boolean usePartFunc = false;

        CalcNewPartBoundsByPivotBoundValParams buildParams = new CalcNewPartBoundsByPivotBoundValParams();

        buildParams.setTtlInfo(ttlInfo);
        buildParams.setPivotBoundValue(pivotPointResult.getPreBuildPivotPointStr());
        buildParams.setTtlTimeZone(ttlTimeZone);
        buildParams.setPreBuildCnt(finalPreBuildPartCnt);
        buildParams.setPostBuildCnt(postBuiltPartCntForPast);
        buildParams.setPreBuildTargetBoundValue(pivotPointResult.getPreBuildTargetBoundValStr());
        buildParams.setArcPartUnit(arcPartTimeUnit);
        buildParams.setArcPartInterval(Long.valueOf(arcPartInterval));
        buildParams.setUseExpireOver(useExpireOver);
        buildParams.setIgnoreRoutingCheck(true);
        buildParams.setTtlPartitionRouter(null);
        buildParams.setPartColMeta(ttlColMeta);
        buildParams.setPartKeyLevel(cciPartLevel);
        buildParams.setUsePartFunc(usePartFunc);
        buildParams.setEc(ec);
        buildParams.setNewAddPartSpecInfosOutput(newAddPartSpecInfosOutputResult);
        calcNewPartBoundsByPivotBoundVal(buildParams);

        if (ec != null) {
            Boolean needAutoAddMaxValPart =
                ec.getParamManager().getBoolean(ConnectionParams.TTL_ADD_MAXVAL_PART_ON_CCI_CREATING);
            if (needAutoAddMaxValPart) {
                Pair<String, String> maxValPartNameAndBndVal =
                    new Pair<>(TtlTaskSqlBuilder.ARC_TBL_MAXVAL_PART_NAME, TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
                newAddPartSpecInfosOutputResult.add(maxValPartNameAndBndVal);
            }
        }

        CreateArcCciPartByDefResult result = new CreateArcCciPartByDefResult();
        result.setParams(params);
        result.setNewCreatedPartSpecInfos(newAddPartSpecInfosOutputResult);

        return result;
    }

    public static CleanupPostPartsCalcResult calcCleanupPostParts(CleanupPostPartsCalcParams params) {
        CleanupPostPartsCalcResult result = new CleanupPostPartsCalcResult();
        result.setParams(params);

        boolean cleanupForCci = params.isCleanupPostPastForCci();
        String pivotDatetimeStr = params.getCleanupUpperBoundDatetimeStr();
        Integer newAddPartsCount = params.getNewAddPartsCount();
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        PartKeyLevel partKeyLevel = params.getTargetPartLevel();
        ExecutionContext ec = params.getEc();

        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ttlTimeZone = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        boolean expireByOver = ttlInfo.useExpireOverPartitionsPolicy();

        TableMeta tarTblMeta = null;
        String tarTblSchema = null;
        String tarTblName = null;
        if (cleanupForCci) {
            tarTblSchema = ttlInfo.getArchiveTableSchema();
            tarTblName = ttlInfo.getTmpTableName();
            tarTblMeta = TtlJobUtil.getArchiveCciTableMeta(ttlTblSchema, ttlTblName, ec);
        } else {
            tarTblSchema = ttlTblSchema;
            tarTblName = ttlTblName;
            tarTblMeta = ec.getSchemaManager(ttlTblSchema).getTableWithNull(ttlTblName);
        }

        if (tarTblMeta == null) {
            // throw ex
            throw new TtlJobRuntimeException(
                String.format("No found any tableMeta for `%s`.`%s`", tarTblSchema, tarTblName));
        }

        List<String> partListToBeDropped = null;
        if (expireByOver) {
            partListToBeDropped = calcExpiredPartsByExpiredOverPolicy(ec, tarTblMeta, partKeyLevel, newAddPartsCount);
        } else {
            partListToBeDropped =
                calcExpiredPartsByExpiredAfterPolicy(ec, tarTblMeta, partKeyLevel, ttlTimeZone, pivotDatetimeStr);
        }

        result.setPartNamesToBeDropped(partListToBeDropped);
        return result;
    }

    private static @NotNull List<String> calcExpiredPartsByExpiredAfterPolicy(ExecutionContext ec,
                                                                              TableMeta tarTblMeta,
                                                                              PartKeyLevel partKeyLevel,
                                                                              String ttlTimeZone,
                                                                              String cleanupUpperBoundDatetimeStr) {
        /**
         * Route the pivotDatetimeStr to target partitions
         */
        String tarPartName = TtlPartitionUtil.findTargetNonMaxValPartByRoutingTtlColVal(ec, tarTblMeta,
            partKeyLevel,
            ttlTimeZone,
            cleanupUpperBoundDatetimeStr);

        List<String> partListToBeDropped = new ArrayList<>();
        if (tarPartName != null) {
            PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partKeyLevel);

            List<PartitionSpec> allPartSpecs = tarPartBy.getPartitions();
            boolean findTarPartName = false;
            int tarPartIndex = -1;
            for (int i = 0; i < allPartSpecs.size(); i++) {
                PartitionSpec part = allPartSpecs.get(i);
                String partName = part.getName();
                if (partName.equalsIgnoreCase(tarPartName)) {
                    findTarPartName = true;
                    tarPartIndex = i;
                    break;
                }
            }

            /**
             * All the parts from first part to the part before tarPartName,
             * can be dropped
             */

            if (findTarPartName) {
                for (int i = 0; i < tarPartIndex; i++) {
                    partListToBeDropped.add(allPartSpecs.get(i).getName());
                }
            }
        }
        return partListToBeDropped;
    }

    private static @NotNull List<String> calcExpiredPartsByExpiredOverPolicy(ExecutionContext ec,
                                                                             TableMeta tarTblMeta,
                                                                             PartKeyLevel partKeyLevel,
                                                                             Integer newAddPartsCount) {
        List<String> partListToBeDropped = new ArrayList<>();
        PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partKeyLevel);
        int partCnt = tarPartBy.getPartitions().size();
        TtlDefinitionInfo ttlInfo = tarTblMeta.getTtlDefinitionInfo();
        int expireOverCnt = ttlInfo.getTtlInfoRecord().getArcPrePartCnt();

        int partCntToBeDropped =
            TtlPartitionUtil.decideDropPartsCountForTtlTblWithExpiredOverPolicy(partCnt, newAddPartsCount,
                expireOverCnt);

        List<PartitionSpec> partitionSpecs = tarPartBy.getPartitions();
        for (int i = 0; i < partCntToBeDropped; i++) {
            PartitionSpec p = partitionSpecs.get(i);
            partListToBeDropped.add(p.getName());
        }
        return partListToBeDropped;
    }

    /**
     * calculate the new partitions by Pivot TimePoint
     *
     * <pre>
     *  case1: there some Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|-----...-----|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *                                  [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time -----------------------------|---------|---------|------...------|--------------->
     *
     *  so, add the following new parts (because newP1Bnd < pnBnd < newP2Bnd ):
     *          newP2Bnd, newP3Bnd, ... , newPmBnd
     *
     *  case2: there no Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|-----...-----|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *                                                  [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time --------------------------------------------|---------|---------|-------...-----|--------------->
     *
     *  so, add the following new parts (because pnBnd < newP1Bnd ):
     *          newP1Bnd, newP3Bnd, ... , newPmBnd
     *
     *  case3: there all Covered partitions between old parts and new parts
     *
     *     Time --------|------|------|----...------|---------------->
     *              [ p1Bnd, p2Bnd, p3Bnd, ... , pnBnd ( exclude pmax ) )
     *
     *         [ newP1Bnd, newP2Bnd, newP3Bnd, ... , newPmBnd )
     *     Time ----|---------|---------|------...-----|--------------->
     *
     *  so, add the following new parts (because pnBnd < newPmBnd ):
     *          PmBnd
     *
     * </pre>
     */
//    protected static void calcNewPartBoundsByPivotTimePoint(String pivotDatetime,
//                                                            String ttlTimeZone,
//                                                            Integer preBuildCnt,
//                                                            Integer postBuildCnt,
//                                                            TtlTimeUnit arcPartTimeUnit,
//                                                            Integer arcPartInterval,
//                                                            boolean ignoreRoutingCheck,
//                                                            TableMeta targetTblMeta,
//                                                            PartKeyLevel partKeyLevel,
//                                                            ExecutionContext ec,
//                                                            List<Pair<String, String>> newAddPartSpecInfosOutput) {
//
//        try {
//            DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
//            LocalDateTime pivotTimePoint = LocalDateTime.parse(pivotDatetime, formatter);
//
//            List<LocalDateTime> newPostPartBoundValList = new ArrayList<>();
//            if (postBuildCnt != null && postBuildCnt > 0) {
//                LocalDateTime newPostBound = pivotTimePoint;
//                for (int i = 0; i < postBuildCnt; i++) {
//                    newPostBound = TtlJobUtil.plusDeltaIntervals(newPostBound, arcPartTimeUnit, -1 * arcPartInterval);
//                    String newPostPartBndValStr = newPostBound.format(formatter);
//
//                    String tarPartName = null;
//                    if (!ignoreRoutingCheck) {
//                        tarPartName = findTargetNonMaxValPartByRoutingTtlColVal(ec, targetTblMeta,
//                            partKeyLevel,
//                            ttlTimeZone,
//                            newPostPartBndValStr);
//                        if (tarPartName == null) {
//                            /**
//                             * No found any non-max-value parts
//                             */
//                            newPostPartBoundValList.add(newPostBound);
//                        }
//                    } else {
//                        newPostPartBoundValList.add(newPostBound);
//                    }
//                }
//            }
//
//            List<LocalDateTime> newPrePartBoundValList = new ArrayList<>();
//            newPrePartBoundValList.add(pivotTimePoint);
//            LocalDateTime newBound = pivotTimePoint;
//            for (int i = 0; i < preBuildCnt; i++) {
//                newBound = TtlJobUtil.plusDeltaIntervals(newBound, arcPartTimeUnit, arcPartInterval);
//                String newPartBndValStr = newBound.format(formatter);
//                if (!ignoreRoutingCheck) {
//                    String tarPartName = findTargetNonMaxValPartByRoutingTtlColVal(ec, targetTblMeta,
//                        partKeyLevel,
//                        ttlTimeZone,
//                        newPartBndValStr);
//                    if (tarPartName == null) {
//                        /**
//                         * No found any non-max-value parts
//                         */
//                        newPrePartBoundValList.add(newBound);
//                    }
//                } else {
//                    newPrePartBoundValList.add(newBound);
//                }
//            }
//
//            List<LocalDateTime> finalNewPartInfos = new ArrayList<>();
//
//            for (int i = newPostPartBoundValList.size() - 1; i > -1; --i) {
//                finalNewPartInfos.add(newPostPartBoundValList.get(i));
//            }
//            finalNewPartInfos.addAll(newPrePartBoundValList);
//
//            if (newAddPartSpecInfosOutput != null) {
//                if (!finalNewPartInfos.isEmpty()) {
//                    boolean buildForSubPartTempName = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY;
//                    for (int i = 0; i < finalNewPartInfos.size(); i++) {
//                        LocalDateTime newBndVal = finalNewPartInfos.get(i);
//                        String newBndValStr = newBndVal.format(formatter);
////                        LocalDateTime bndValForPartName =
////                            TtlJobUtil.plusDeltaIntervals(newBndVal, arcPartTimeUnit, -1 * rngInterval);
//                        String bndValForPartNameStr =
//                            buildPartNameByBoundValue(newBndVal, arcPartTimeUnit, buildForSubPartTempName);
//                        Pair<String, String> newPartNameAndBndVal = new Pair<>(bndValForPartNameStr, newBndValStr);
//                        newAddPartSpecInfosOutput.add(newPartNameAndBndVal);
//                    }
//                }
//            }
//        } catch (Throwable ex) {
//            throw new TtlJobRuntimeException(ex);
//        }
//    }

    public static class TtlColBoundValue {

        protected boolean isNumberType = false;
        protected boolean isMySqlDateType = false;
        protected ColumnMeta partColMeta = null;

        protected PartKeyLevel partKeyLevel;
        protected boolean usePartFunc = false;
        protected boolean isForSubPart = false;
        protected TtlDefinitionInfo ttlInfo;
        protected boolean useTtlColFuncExpr = false;

        protected DataType partColValDataType = null;
        protected PartitionField partColValFld;

        protected PartitionField ttlColValFldForCalcNewBounds;
        protected RelDataType ttlColRelDataTypeForCalcNewBounds;
        protected DataType ttlColDataTypeForCalcNewBounds;

        protected TtlColBoundValue(String bndValStr,
                                   ColumnMeta partColMeta,
                                   PartKeyLevel partKeyLevel,
                                   boolean usePartFunc,
                                   TtlDefinitionInfo ttlInfo) {
            init(bndValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo);
        }

        protected void init(String bndValStr,
                            ColumnMeta partColMeta,
                            PartKeyLevel partKeyLevel,
                            boolean usePartFunc,
                            TtlDefinitionInfo ttlInfo) {
            this.ttlInfo = ttlInfo;

            if (this.ttlInfo != null) {
                this.useTtlColFuncExpr = this.ttlInfo.isTtlColUseFuncExpr();
            }

            this.usePartFunc = usePartFunc;
            this.partColMeta = partColMeta;
            this.partColValDataType = partColMeta.getDataType();

            this.ttlColRelDataTypeForCalcNewBounds = getTtlColRelDataTypeForCalcNewBounds(ttlInfo, partColMeta);
            this.ttlColDataTypeForCalcNewBounds =
                getTtlColDataTypeForCalcNewBounds(ttlInfo, partColMeta, ttlColRelDataTypeForCalcNewBounds);

            this.isNumberType =
                DataTypeUtil.isUnderBigintUnsignedType(ttlColDataTypeForCalcNewBounds) || DataTypeUtil.isDecimalType(
                    ttlColDataTypeForCalcNewBounds);
            this.partKeyLevel = partKeyLevel;
            if (this.isNumberType) {
                /**
                 *  if part col of range is a number type,
                 *  that means the datatype of bound value is the same as the datetype of part col
                 */
            } else {
                /**
                 *  if part col of range is a datetime type,
                 *  then
                 *  the datatype of bound value is different of the datetype of part col
                 *  if using partFuc, such partition by range(unix_timestamp(ts_col))
                 */
                if (this.partColValDataType.getSqlType() == DataTypes.DateType.getSqlType()) {
                    isMySqlDateType = true;
                }
            }
            this.isForSubPart = partKeyLevel == PartKeyLevel.SUBPARTITION_KEY;

            if (!useTtlColFuncExpr) {
                this.partColValFld = PartitionFieldBuilder.createField(partColValDataType);
                this.partColValFld.store(bndValStr, DataTypes.StringType);
                this.ttlColValFldForCalcNewBounds = this.partColValFld;
            } else {
                this.ttlColValFldForCalcNewBounds = PartitionFieldBuilder.createField(ttlColDataTypeForCalcNewBounds);
                this.ttlColValFldForCalcNewBounds.store(bndValStr, DataTypes.StringType);

                String datetimeStr = this.ttlColValFldForCalcNewBounds.stringValue().toStringUtf8();
                Long unixTimestampLongVal =
                    convertZonedDateTimeStringToUnixTimestamp(datetimeStr, ttlInfo.getTtlInfoRecord().getTtlTimezone());
                this.partColValFld = PartitionFieldBuilder.createField(partColValDataType);
                if (ttlInfo.getTtlColFuncExprInfo().isTreatTtlColAsUnixTimestampMillSeconds()) {
                    unixTimestampLongVal *= 1000;
                }
                this.partColValFld.store(unixTimestampLongVal, DataTypes.LongType);
            }

        }

        public static TtlColBoundValue buildPartBoundValue(String bndValStr,
                                                           ColumnMeta partColMeta,
                                                           PartKeyLevel partKeyLevel,
                                                           boolean usePartFunc,
                                                           TtlDefinitionInfo ttlInfo) {
            TtlColBoundValue
                boundValue = new TtlColBoundValue(bndValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo);
            return boundValue;
        }

        public String getPartBoundValueStringForCalcNewBounds() {
            BuildPartFieldStringParams params = BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo);
            params.setForceReturnPartFldString(true);
            return convertPartFieldToString(this.ttlColValFldForCalcNewBounds, params);
        }

        public String getPartBoundValueStringByOriginalPartColDataType() {
            BuildPartFieldStringParams params = BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo);
            params.setForceReturnPartFldString(true);
            return convertPartFieldToString(this.partColValFld, params);
        }

        private String getDateTimePartBoundValueFormatOnPartName(TtlTimeUnit ttlUnit) {
            DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
            String bndValStr = getPartBoundValueStringForCalcNewBounds();
            LocalDateTime localDateTimeObj = LocalDateTime.parse(bndValStr, formatter);
            String partNameFormatterPattern = TtlTaskSqlBuilder.getPartNameFormatterPatternByTtlUnit(TtlTimeUnit.DAY);
            DateTimeFormatter partNameDatetimeFormatter = DateTimeFormatter.ofPattern(partNameFormatterPattern);
            String formatedResult = localDateTimeObj.format(partNameDatetimeFormatter);
            return formatedResult;
        }

        private String getNumberPartBoundValueFormatOnPartName(TtlTimeUnit ttlUnit) {
            String bndValStr = getPartBoundValueStringForCalcNewBounds();
            return bndValStr;
        }

        public String buildPartNameByPartUnit(TtlTimeUnit ttlUnit) {
            String partNamePrefix = "p";
            if (isForSubPart) {
                partNamePrefix = "sp";
            }
            String partName = null;
            if (!isNumberType) {
                partName = partNamePrefix + getDateTimePartBoundValueFormatOnPartName(ttlUnit);
            } else {
                partName = partNamePrefix + getNumberPartBoundValueFormatOnPartName(ttlUnit);
            }
            return partName;

        }

//        public PartitionByDefinition getTartPartBy() {
//            return tartPartBy;
//        }

        public RelDataType getTtlColRelDataTypeForCalcNewBounds(TtlDefinitionInfo ttlInfo,
                                                                ColumnMeta partColMeta) {
            boolean useTtlFuncExpr = ttlInfo.isTtlColUseFuncExpr();
            if (!useTtlFuncExpr) {
                return partColMeta.getField().getRelType();
            }
            /**
             * All the value of date/datetime/timestamp should treat as datetime with scale=0 relDatetype
             */
            RelDataType calciteDataType = DataTypeUtil.jdbcTypeToRelDataType(0,
                "DATETIME", 0, 0, 0, true);
            return calciteDataType;
        }

        public DataType getTtlColDataTypeForCalcNewBounds(TtlDefinitionInfo ttlInfo,
                                                          ColumnMeta partColMeta,
                                                          RelDataType ttlColRelDataTypeForCalcNewBounds) {
            boolean useTtlFuncExpr = ttlInfo.isTtlColUseFuncExpr();
            if (!useTtlFuncExpr) {
                return partColMeta.getDataType();
            }
            /**
             * All the value of date/datetime/timestamp should treat as datetime string
             */
            DataType dataTypeResult = DataTypeUtil.calciteToDrdsType(ttlColRelDataTypeForCalcNewBounds);
            return dataTypeResult;
        }

        public TtlColBoundValue plusInterval(Long partInterval,
                                             TtlTimeUnit partIntervalUnit) {

            String newBndValStr = null;
//            PartitionByDefinition tarPartBy = this.getTartPartBy();
            if (!isNumberType) {
                String pivotPointStr = getPartBoundValueStringForCalcNewBounds();
                DateTimeFormatter dateTimeIsoFormatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
                LocalDateTime pivotTimePointObj = LocalDateTime.parse(pivotPointStr, dateTimeIsoFormatter);
                LocalDateTime nextBoundObj =
                    TtlJobUtil.plusDeltaIntervals(pivotTimePointObj, partIntervalUnit, partInterval.intValue());
                newBndValStr = nextBoundObj.format(dateTimeIsoFormatter);
            } else {
                String utf8StrVal = getPartBoundValueStringForCalcNewBounds();
                if (DataTypeUtil.isUnderBigintUnsignedType(ttlColDataTypeForCalcNewBounds)) {
                    BigInteger tmpBigIntVal = new BigInteger(utf8StrVal);
                    BigInteger tmpInterval = new BigInteger(String.valueOf(partInterval));
                    tmpBigIntVal = tmpBigIntVal.add(tmpInterval);
                    newBndValStr = String.valueOf(tmpBigIntVal);
                } else if (DataTypeUtil.isDecimalType(ttlColDataTypeForCalcNewBounds)) {
                    BigDecimal tmpBigDeciVal = new BigDecimal(utf8StrVal);
                    BigDecimal tmpInterval = new BigDecimal(String.valueOf(partInterval));
                    tmpBigDeciVal = tmpBigDeciVal.add(tmpInterval);
                    newBndValStr = String.valueOf(tmpBigDeciVal);
                }
            }
            TtlColBoundValue newBondVal =
                new TtlColBoundValue(newBndValStr, this.partColMeta, this.partKeyLevel, this.usePartFunc, this.ttlInfo);
            return newBondVal;
        }

        /**
         * currBndVal > otherPartBndVal , return 1;
         * currBndVal = otherPartBndVal , return 0;
         * currBndVal < otherPartBndVal , return -1
         */
        public int compare(TtlColBoundValue otherPartBndVal) {

            SearchDatumComparator partColSpaceComp = null;
            if (!useTtlColFuncExpr) {
                List<ColumnMeta> partColMetas = new ArrayList<>();
                partColMetas.add(partColMeta);
                partColSpaceComp = PartitionInfoUtil.buildPartColumnComparator(partColMetas);
            } else {
                List<RelDataType> ttlColRelDataTypes = new ArrayList<>();
                List<DataType> ttlColDataTypes = new ArrayList<>();
                ttlColRelDataTypes.add(ttlColRelDataTypeForCalcNewBounds);
                ttlColDataTypes.add(ttlColDataTypeForCalcNewBounds);
                partColSpaceComp =
                    PartitionInfoUtil.buildQuerySpaceComparatorBySpecifyDataTypes(ttlColRelDataTypes, ttlColDataTypes);
            }

            PartitionField otherPartBndFld = otherPartBndVal.getPartColValFld();
            SearchDatumInfo otherPartBndDatum = SearchDatumInfo.createFromField(otherPartBndFld);
            SearchDatumInfo currBndValDatum = SearchDatumInfo.createFromField(partColValFld);
            int comRs = partColSpaceComp.compare(currBndValDatum, otherPartBndDatum);
            return comRs;
        }

        @Override
        public String toString() {
            if (partColValFld != null) {
                if (partColValFld.isNull()) {
                    return null;
                }
                return getPartBoundValueStringForCalcNewBounds();
            }
            return null;
        }

        public boolean isNumberType() {
            return isNumberType;
        }

        public DataType getPartColValDataType() {
            return partColValDataType;
        }

        public PartitionField getPartColValFld() {
            return partColValFld;
        }

        public PartKeyLevel getPartKeyLevel() {
            return partKeyLevel;
        }

        public boolean isUsePartFunc() {
            return usePartFunc;
        }

        public boolean isForSubPart() {
            return isForSubPart;
        }
    }

    public static PartitionByDefinition getTargetPartBy(TableMeta tarTblMeta, PartKeyLevel partKeyLevel) {
        PartitionInfo partInfo = tarTblMeta.getPartitionInfo();
        return getTargetPartByFromPartInfo(partInfo, partKeyLevel);
    }

    public static PartitionByDefinition getTargetPartByFromPartInfo(PartitionInfo partInfo, PartKeyLevel partKeyLevel) {
        PartitionByDefinition partBy = partInfo.getPartitionBy();
        if (partKeyLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partBy = partBy.getSubPartitionBy();
        }
        return partBy;
    }

    protected static class TtlPartitionRouter {

        protected TableMeta tarTblMeta;
        protected PartKeyLevel partLevel;
        protected String ttlTimeZone;

        public TtlPartitionRouter(TableMeta tarTblMeta,
                                  PartKeyLevel partLevel,
                                  String ttlTimeZone
        ) {
            this.tarTblMeta = tarTblMeta;
            this.partLevel = partLevel;
            this.ttlTimeZone = ttlTimeZone;
        }

        protected String findTargetNonMaxValPartByRoutingTtlColVal(ExecutionContext ec, TtlColBoundValue ttlColVal) {
            List<String> phyParts = fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ec,
                tarTblMeta,
                partLevel,
                ttlTimeZone,
                ttlColVal);
            if (phyParts.isEmpty()) {
                return null;
            }
            String partNameRs = phyParts.get(0);
            PartitionInfo tarTblPartInfo = tarTblMeta.getPartitionInfo();
            PartitionSpec partSpecRs = null;
            if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
                partSpecRs = tarTblPartInfo.getPartSpecSearcher().getSubPartTempSpecNameBySubPartTempName(partNameRs);
            } else {
                partSpecRs = tarTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameRs);
            }

            if (partSpecRs != null && partSpecRs.getBoundSpec().containMaxValues()) {
                return null;
            }
            return partNameRs;
        }

        /**
         * Auto fix the invalid partitions which is to be added newly
         * <pre>
         *    Some auto-gen bound value will be out of range of date type
         *    (1) auto remove the duplicated partitions (partition names are the same or bound value are the same);
         *    (2) auto generate new part name the the new add part whose name are already exists in curr partitions but
         *        bound value is different.
         *
         * </pre>
         */
        public boolean fixInvalidPartitionsWithPartInfo(List<Pair<String, String>> newAddPartSpecInfosOutput,
                                                        List<Pair<String, String>> fixedPartSpecInfosOutput,
                                                        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {
            TtlPartitionUtil.fixInvalidPartitions(newAddPartSpecInfosOutput, tarTblMeta, partLevel,
                fixedPartSpecInfosOutput, fixedPartSpecIndexMappingsOutput);
            return true;
        }
    }

    protected static void calcNewPartBoundsByPivotBoundValWithTblMeta(
        CalcNewPartBoundsByPivotBoundValWithTblMetaParams params) {

        TableMeta targetTblMeta = params.getTarTblMeta();
        PartKeyLevel partKeyLevel = params.getTarPartKeyLevel();
        CalcNewPartBoundsByPivotBoundValParams inputParams = params.getCalcParams();

        String ttlTimeZone = inputParams.getTtlTimeZone();
        PartitionByDefinition tarPartBy = TtlPartitionUtil.getTargetPartBy(targetTblMeta, partKeyLevel);
        TtlPartitionRouter ttlPartitionRouter = new TtlPartitionRouter(targetTblMeta, partKeyLevel, ttlTimeZone);

        CalcNewPartBoundsByPivotBoundValParams buildParams = new CalcNewPartBoundsByPivotBoundValParams();
        buildParams.setTtlInfo(inputParams.getTtlInfo());
        buildParams.setPivotBoundValue(inputParams.getPivotBoundValue());
        buildParams.setTtlTimeZone(inputParams.getTtlTimeZone());
        buildParams.setPreBuildCnt(inputParams.getPreBuildCnt());
        buildParams.setPostBuildCnt(inputParams.getPostBuildCnt());
        buildParams.setArcPartUnit(inputParams.getArcPartUnit());
        buildParams.setArcPartInterval(Long.valueOf(inputParams.getArcPartInterval()));
        buildParams.setPreBuildTargetBoundValue(inputParams.getPreBuildTargetBoundValue());
        buildParams.setIgnoreRoutingCheck(inputParams.isIgnoreRoutingCheck());
        buildParams.setUseExpireOver(inputParams.isUseExpireOver());
        buildParams.setEc(inputParams.getEc());
        buildParams.setNewAddPartSpecInfosOutput(inputParams.getNewAddPartSpecInfosOutput());
        buildParams.setNewAddPartSpecMappingsOutput(inputParams.getNewAddPartSpecMappingsOutput());

        buildParams.setTtlPartitionRouter(ttlPartitionRouter);
        buildParams.setPartColMeta(tarPartBy.getPartitionFieldList().get(0));
        buildParams.setPartKeyLevel(tarPartBy.getPartLevel());
        buildParams.setUsePartFunc(tarPartBy.getPartIntFunc() != null);

        calcNewPartBoundsByPivotBoundVal(buildParams);
    }

    public static void calcNewPartBoundsByPivotBoundVal(CalcNewPartBoundsByPivotBoundValParams params) {

        String pivotBoundValue = params.getPivotBoundValue();
        Integer preBuildCnt = params.getPreBuildCnt();
        Integer postBuildCnt = params.getPostBuildCnt();
        String preBuildTargetBoundValue = params.getPreBuildTargetBoundValue();
        TtlTimeUnit arcPartUnit = params.getArcPartUnit();
        Long arcPartInterval = params.getArcPartInterval();
        boolean ignoreRoutingCheck = params.isIgnoreRoutingCheck();
        TtlPartitionUtil.TtlPartitionRouter ttlPartitionRouter = params.getTtlPartitionRouter();
        ColumnMeta partColMeta = params.getPartColMeta();
        PartKeyLevel partKeyLevel = params.getPartKeyLevel();
        boolean usePartFunc = params.isUsePartFunc();
        ExecutionContext ec = params.getEc();
        TtlDefinitionInfo ttlInfo = params.getTtlInfo();
        List<Pair<String, String>> newAddPartSpecInfosOutput = params.getNewAddPartSpecInfosOutput();
        Map<String, TtlColBoundValue> newAddPartSpecMappingsOutput = params.getNewAddPartSpecMappingsOutput();
        try {
            String pivotValStr = pivotBoundValue;
            TtlColBoundValue pivotValuePoint =
                TtlColBoundValue.buildPartBoundValue(pivotValStr, partColMeta, partKeyLevel, usePartFunc, ttlInfo);

            List<TtlColBoundValue> newPostPartBoundValList = new ArrayList<>();
            if (postBuildCnt != null && postBuildCnt > 0) {

                TtlColBoundValue newPostBound = pivotValuePoint;
                for (int i = 0; i < postBuildCnt; i++) {
                    newPostBound = newPostBound.plusInterval(-1L * arcPartInterval, arcPartUnit);
                    String tarPartName = null;
                    if (!ignoreRoutingCheck) {
                        if (ttlPartitionRouter != null) {
                            tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newPostBound);
                            if (tarPartName == null) {
                                /**
                                 * No found any non-max-value parts
                                 */
                                newPostPartBoundValList.add(newPostBound);
                            }
                        }
                    } else {
                        newPostPartBoundValList.add(newPostBound);
                    }
                }
            }

            List<TtlColBoundValue> newPrePartBoundValList = new ArrayList<>();
            newPrePartBoundValList.add(pivotValuePoint);

            String preBuildTarBoundValueStr = preBuildTargetBoundValue;
            TtlColBoundValue preBuildTargetBndVal = null;
            boolean needPreBuildToTargetBndVal = false;
            if (preBuildTarBoundValueStr != null) {
                preBuildTargetBndVal =
                    TtlColBoundValue.buildPartBoundValue(preBuildTarBoundValueStr, partColMeta, partKeyLevel,
                        usePartFunc, ttlInfo);
                needPreBuildToTargetBndVal = true;
            }

            TtlColBoundValue newBound = pivotValuePoint;
            if (needPreBuildToTargetBndVal) {
                while (true) {
                    newBound = newBound.plusInterval(arcPartInterval, arcPartUnit);
                    if (!ignoreRoutingCheck) {
                        if (ttlPartitionRouter != null) {
                            String tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newBound);
                            if (tarPartName == null) {
                                /**
                                 * No found any non-max-value parts
                                 */
                                newPrePartBoundValList.add(newBound);
                            }
                        }
                    } else {
                        newPrePartBoundValList.add(newBound);
                    }
                    if (newBound.compare(preBuildTargetBndVal) >= 0) {
                        break;
                    }
                }
            } else {
                for (int i = 0; i < preBuildCnt; i++) {
                    newBound = newBound.plusInterval(arcPartInterval, arcPartUnit);
                    if (!ignoreRoutingCheck) {
                        if (ttlPartitionRouter != null) {
                            String tarPartName =
                                ttlPartitionRouter.findTargetNonMaxValPartByRoutingTtlColVal(ec, newBound);
                            if (tarPartName == null) {
                                /**
                                 * No found any non-max-value parts
                                 */
                                newPrePartBoundValList.add(newBound);
                            }
                        }
                    } else {
                        newPrePartBoundValList.add(newBound);
                    }
                }
            }

            List<TtlColBoundValue> finalNewPartInfos = new ArrayList<>();

            for (int i = newPostPartBoundValList.size() - 1; i > -1; --i) {
                finalNewPartInfos.add(newPostPartBoundValList.get(i));
            }
            finalNewPartInfos.addAll(newPrePartBoundValList);

            List<Pair<String, String>> newAddPartSpecInfosBeforeFixingOutput = new ArrayList<>();
            if (newAddPartSpecInfosOutput != null) {
                if (!finalNewPartInfos.isEmpty()) {
                    for (int i = 0; i < finalNewPartInfos.size(); i++) {
                        TtlColBoundValue newBndVal = finalNewPartInfos.get(i);
                        String newBndValStr = newBndVal.getPartBoundValueStringByOriginalPartColDataType();
                        String bndValForPartNameStr = newBndVal.buildPartNameByPartUnit(arcPartUnit);
                        Pair<String, String> newPartNameAndBndVal = new Pair<>(bndValForPartNameStr, newBndValStr);
                        newAddPartSpecInfosBeforeFixingOutput.add(newPartNameAndBndVal);
                    }
                }

                List<Pair<String, String>> newAddPartSpecInfosAfterFixingOutput = new ArrayList<>();
                List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput = new ArrayList<>();
                if (ttlPartitionRouter != null) {
                    ttlPartitionRouter.fixInvalidPartitionsWithPartInfo(newAddPartSpecInfosBeforeFixingOutput,
                        newAddPartSpecInfosAfterFixingOutput, fixedPartSpecIndexMappingsOutput);
                    newAddPartSpecInfosOutput.clear();
                    newAddPartSpecInfosOutput.addAll(newAddPartSpecInfosAfterFixingOutput);
                } else {
                    TtlPartitionUtil.fixInvalidPartitionsWithoutPartInfo(newAddPartSpecInfosBeforeFixingOutput,
                        newAddPartSpecInfosAfterFixingOutput, fixedPartSpecIndexMappingsOutput);
                    newAddPartSpecInfosOutput.clear();
                    newAddPartSpecInfosOutput.addAll(newAddPartSpecInfosAfterFixingOutput);
                }

                if (newAddPartSpecMappingsOutput != null) {
                    for (int i = 0; i < fixedPartSpecIndexMappingsOutput.size(); i++) {
                        Pair<Integer, Integer> specIndexMapping = fixedPartSpecIndexMappingsOutput.get(i);
                        String newPartName = newAddPartSpecInfosAfterFixingOutput.get(i).getKey();

                        Integer originIdx = specIndexMapping.getKey();
                        TtlColBoundValue partBndVal = finalNewPartInfos.get(originIdx);
                        newAddPartSpecMappingsOutput.put(newPartName, partBndVal);
                    }
                }
            }
        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }
    }

    protected static String findTargetNonMaxValPartByRoutingTtlColVal(ExecutionContext ec,
                                                                      TableMeta tarTblMeta,
                                                                      PartKeyLevel partLevel,
                                                                      String ttlTimeZone,
                                                                      String ttlColVal) {

        TtlDefinitionInfo ttlInfo = tarTblMeta.getTtlDefinitionInfo();
        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(ec);
        PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, partLevel);
        TtlColBoundValue ttlColBoundVal = new TtlColBoundValue(ttlColVal,
            ttlColMeta, partLevel, tarPartBy.getPartIntFunc() != null, ttlInfo);
        List<String> phyParts = fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ec,
            tarTblMeta,
            partLevel,
            ttlTimeZone,
            ttlColBoundVal);
        if (phyParts.isEmpty()) {
            return null;
        }
        String partNameRs = phyParts.get(0);
        PartitionInfo tarTblPartInfo = tarTblMeta.getPartitionInfo();
        PartitionSpec partSpecRs = null;
        if (partLevel == PartKeyLevel.SUBPARTITION_KEY) {
            partSpecRs = tarTblPartInfo.getPartSpecSearcher().getSubPartTempSpecNameBySubPartTempName(partNameRs);
        } else {
            partSpecRs = tarTblPartInfo.getPartSpecSearcher().getPartSpecByPartName(partNameRs);
        }

        if (partSpecRs != null && partSpecRs.getBoundSpec().containMaxValues()) {
            return null;
        }
        return partNameRs;
    }

    protected static List<String> fetchTargetPartNamesByRoutingTtlColValAndPartLevel(ExecutionContext ec,
                                                                                     TableMeta tarTblMeta,
                                                                                     PartKeyLevel targetPartLevel,
                                                                                     String ttlTimezone,
                                                                                     TtlColBoundValue tarTtlColValToRoute) {
        PartitionInfo arcTmpTblPartInfo = tarTblMeta.getPartitionInfo();
        InternalTimeZone internalTimeZone = TimeZoneUtils.convertFromMySqlTZ(ttlTimezone);
        ec.setTimeZone(internalTimeZone);
        List<Object> pointValue = new ArrayList<>();
        String ttlColValStr = tarTtlColValToRoute.getPartBoundValueStringByOriginalPartColDataType();
        pointValue.add(ttlColValStr);

        List<DataType> pointValueOpTypes = new ArrayList<>();
        pointValueOpTypes.add(DataTypes.StringType);
        ExecutionContext[] newEcOutput = new ExecutionContext[1];
        RelDataTypeFactory type = PartitionPrunerUtils.getTypeFactory();
        RelDataType tbRelRowType = tarTblMeta.getPhysicalRowType(type);

        PartitionPruneStep pruneStep = PartitionPruneStepBuilder.genPointSelectPruneStepInfoForTtlRouting(pointValue,
            pointValueOpTypes, ec, newEcOutput, arcTmpTblPartInfo, targetPartLevel, tbRelRowType);

        PartPrunedResult prunedResult = PartitionPruner.doPruningByStepInfo(pruneStep, newEcOutput[0]);
        List<String> phyPartNameList = prunedResult.getPrunedPartitionNamesOfPartLevel(targetPartLevel, true);

        return phyPartNameList;
    }

//    protected static String buildPartNameByBoundValue(LocalDateTime partBoundVal,
//                                                      TtlTimeUnit ttlUnit,
//                                                      Boolean buildForSubPartTempName) {
//        String partNameFormatterPattern = TtlTaskSqlBuilder.getPartNameFormatterPatternByTtlUnit(ttlUnit);
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(partNameFormatterPattern);
//        String partNamePrefix = "p";
//        if (buildForSubPartTempName) {
//            partNamePrefix = "sp";
//        }
//        String partName = partNamePrefix + partBoundVal.format(formatter);
//        return partName;
//    }
//    protected static String buildAddPartitionsSqlTemplate(TtlDefinitionInfo ttlInfo,
//                                                       List<String> newAddRangeBounds,
//                                                       List<String> newAddRangePartNames,
//                                                       String maxValPartName,
//                                                       String queryHint,
//                                                       ExecutionContext executionContext) {
//
//        String partBoundDefs = "";
//        List<String> normalizedNewAddPartBoundList =
//            normalizedRangePartBoundValueList(ttlInfo, newAddRangeBounds, executionContext);
//        for (int i = 0; i < normalizedNewAddPartBoundList.size(); i++) {
//            String bndStr = normalizedNewAddPartBoundList.get(i);
//            String partNameStr = newAddRangePartNames.get(i);
//            String escapedPartName = SqlIdentifierUtil.escapeIdentifierString(partNameStr);
//            String part = String.format("PARTITION %s VALUES LESS THAN (%s)", escapedPartName, bndStr);
//            if (!partBoundDefs.isEmpty()) {
//                partBoundDefs += ",\n";
//            }
//            partBoundDefs += part;
//        }
//
//        String addPartSpecSql = "";
//        if (StringUtils.isEmpty(maxValPartName)) {
//            addPartSpecSql = String.format("ADD PARTITION ( \n%s );",
//                partBoundDefs);
//        } else {
//            addPartSpecSql = String.format("SPLIT PARTITION `%s` INTO ( \n%s, PARTITION `%s` VALUES LESS THAN (MAXVALUE) );",
//                maxValPartName, partBoundDefs, maxValPartName);
//        }
//
//        String skipDdlTasks =
//            executionContext.getParamManager().getString(ConnectionParams.TTL_DEBUG_CCI_SKIP_DDL_TASKS);
//        if (!StringUtils.isEmpty(skipDdlTasks)) {
//            queryHint = TtlTaskSqlBuilder.addCciHint(queryHint, String.format("SKIP_DDL_TASKS=\"%s\"", skipDdlTasks));
//        }
//
//        String addPartSqlTemp = queryHint + " ALTER INDEX %s ON TABLE %s " + addPartSpecSql;
//
//        return addPartSqlTemp;
//    }

    protected static List<String> normalizedRangePartBoundValueList(TtlDefinitionInfo ttlInfo,
                                                                    List<String> rangeBounds,
                                                                    boolean buildPartForCci,
                                                                    PartKeyLevel tarPartLevel,
                                                                    ExecutionContext executionContext) {
        String primTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String primTblName = ttlInfo.getTtlInfoRecord().getTableName();
        TableMeta primTblMeta = executionContext.getSchemaManager(primTblSchema).getTable(primTblName);

        TableMeta tarTblMeta = null;
        PartitionInfo tarTblPartInfo = null;
        PartitionByDefinition tarPartBy = null;
        String tarTblSchema = null;
        String tarTblName = null;
        if (buildPartForCci) {
            tarTblSchema = ttlInfo.getTmpTableSchema();
            tarTblName = ttlInfo.getTmpTableName();
            tarTblMeta = TtlJobUtil.getArchiveCciTableMeta(primTblSchema, primTblName, executionContext);
            tarTblPartInfo = tarTblMeta.getPartitionInfo();
        } else {
            tarTblSchema = primTblSchema;
            tarTblName = primTblName;
            tarTblMeta = primTblMeta;
            tarTblPartInfo = tarTblMeta.getPartitionInfo();
        }

        PartitionTableType tblType = tarTblPartInfo.getTableType();
        if (PartitionTableType.PARTITIONED_TABLE.contains(tblType)) {
            tarPartBy = tarTblPartInfo.getPartitionBy();
            if (tarPartLevel == PartKeyLevel.SUBPARTITION_KEY) {
                tarPartBy = tarPartBy.getSubPartitionBy();
            }
        }

        String tarPartFuncName = "";
        PartitionIntFunction partFunc = tarPartBy.getPartIntFunc();
        DataType partColType = tarPartBy.getPartitionColumnTypeList().get(0);
        boolean isNumberType = DataTypeUtil.isNumberSqlType(partColType);
        if (partFunc != null) {
            String partFuncName = partFunc.getSqlOperator().getName();
            if (partFunc.getMonotonicity(partColType) != Monotonicity.NON_MONOTONIC
                && PartitionFunctionBuilder.isTimeBasedFamilyPartitionFunction(partFuncName)) {
                tarPartFuncName = partFunc.getSqlOperator().getName();
            } else {
                throw new TtlJobRuntimeException(String.format(
                    "Found invalid partition function `%s` on the partition columns of %s level  of `%s`.`%s`",
                    partFuncName, tarPartLevel.name(), tarTblSchema, tarTblName));
            }
        }

        List<String> result = new ArrayList<>();
        for (int i = 0; i < rangeBounds.size(); i++) {
            String bndStr = rangeBounds.get(i);
            boolean isMaxValBnd = bndStr.toUpperCase().contains(TtlTaskSqlBuilder.ARC_TBL_MAXVALUE_BOUND);
            boolean partBndNeedUseFuncExpr = !StringUtils.isEmpty(tarPartFuncName);
            String normalizedPartBndVal = "";
            if (!isMaxValBnd) {
//                if (!partBndNeedUseFuncExpr) {
//                    normalizedPartBndVal = String.format("'%s'", bndStr);
//                } else {
//                    normalizedPartBndVal = convertToUnixTimestampLongStr(bndStr, ttlInfo);
//                }
                if (!isNumberType) {
                    if (partBndNeedUseFuncExpr) {
                        normalizedPartBndVal = String.format("%s('%s')", tarPartFuncName, bndStr);
                    } else {
                        normalizedPartBndVal = String.format("'%s'", bndStr);
                    }
                } else {
                    normalizedPartBndVal = String.format("%s", bndStr);
                }

            } else {
                normalizedPartBndVal = bndStr;
            }
            result.add(normalizedPartBndVal);
        }
        return result;
    }

    public static String convertToUnixTimestampLongStr(String bndStr,
                                                       TtlDefinitionInfo ttlInfo) {
        String timezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        ZoneId zoneId = ZoneId.of(timezoneStr);
        LocalDateTime localDateTime = LocalDateTime.parse(bndStr, TtlTaskSqlBuilder.ISO_DATETIME_FORMATTER);
        ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, zoneId);
        Long unixTs = zonedDateTime.toEpochSecond();
        String unixTsStr = String.valueOf(unixTs);
        return unixTsStr;
    }

    public static Pair<String, SearchDatumInfo> findMinPartBoundValAndPartNameFromNonMaxValParts(
        PartitionInfo tarPartInfo,
        PartKeyLevel partLevel) {
        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartByFromPartInfo(tarPartInfo, partLevel);
        List<PartitionSpec> parts = partBy.getPartitions();

        PartitionSpec firstPart = parts.get(0);
        if (firstPart.getBoundSpec().containMaxValues()) {
            return null;
        }

        Pair<String, SearchDatumInfo> result =
            new Pair<String, SearchDatumInfo>(firstPart.getName(), firstPart.getBoundSpec().getSingleDatum());
        return result;
    }

    public static Pair<String, SearchDatumInfo> findMaxPartBoundValAndPartNameFromNonMaxValParts(
        PartitionInfo tarPartInfo,
        PartKeyLevel partLevel) {
        PartitionByDefinition partBy = TtlPartitionUtil.getTargetPartByFromPartInfo(tarPartInfo, partLevel);
        List<PartitionSpec> parts = partBy.getPartitions();
        int partCnt = parts.size();
        PartitionSpec lastNonMaxValPartSpec = parts.get(partCnt - 1);
        if (lastNonMaxValPartSpec.getBoundSpec().containMaxValues()) {
            if (partCnt - 2 >= 0) {
                lastNonMaxValPartSpec = parts.get(partCnt - 2);
            } else {
                lastNonMaxValPartSpec = null;
            }
        }
        if (lastNonMaxValPartSpec == null) {
            return null;
        }

        SearchDatumInfo bndVal = lastNonMaxValPartSpec.getBoundSpec().getSingleDatum();
        Pair<String, SearchDatumInfo> result =
            new Pair<String, SearchDatumInfo>(lastNonMaxValPartSpec.getName(), bndVal);
        return result;
    }

    public static String findMaxPartBoundValStrFromNonMaxValParts(PartitionInfo tarPartInfo,
                                                                  PartKeyLevel partLevel,
                                                                  TtlDefinitionInfo ttlInfo) {
        Pair<String, SearchDatumInfo> partNameAndDatum =
            findMaxPartBoundValAndPartNameFromNonMaxValParts(tarPartInfo, partLevel);
        SearchDatumInfo bndValDatum = partNameAndDatum.getValue();
        BuildPartFieldStringParams params = BuildPartFieldStringParams.constructPartFieldStringParams(ttlInfo);
        return convertPartSpecBoundValToString(bndValDatum.getDatumInfo()[0], params);
    }

    public static boolean fixInvalidPartitionsWithoutPartInfo(
        List<Pair<String, String>> newAddPartSpecInfosOutput,
        List<Pair<String, String>> fixedPartSpecInfosOutput,
        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {
        TtlPartitionUtil.fixInvalidPartitions(newAddPartSpecInfosOutput, null, null, fixedPartSpecInfosOutput,
            fixedPartSpecIndexMappingsOutput);
        return true;
    }

    /**
     * Auto fix the invalid partitions which is to be added newly
     * <pre>
     *    Some auto-gen bound value will be out of range of date type
     *    (1) auto remove the duplicated partitions (partition names are the same or bound value are the same);
     *    (2) auto generate new part name the the new add part whose name are already exists in curr partitions but
     *        bound value is different.
     *
     * </pre>
     */
    protected static boolean fixInvalidPartitions(
        List<Pair<String, String>> newAddPartSpecInfosOutput,
        TableMeta tarTblMeta,
        PartKeyLevel tarTblPartLevel,
        List<Pair<String, String>> fixedPartSpecInfosOutput,
        List<Pair<Integer, Integer>> fixedPartSpecIndexMappingsOutput) {

        List<Pair<String, String>> newAddPartSpecInfosOutputAfterFixing = new ArrayList<>();
        List<Pair<Integer, Integer>> newAddPartSpecIndexMappingsOutputAfterFixing = new ArrayList<>();

        boolean specifyTblMeta = tarTblMeta != null;
        Map<String, String> allPartBndInfoMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        PartitionInfo tartPartInfo = null;
        PartSpecSearcher partSpecSearcher = null;
        if (specifyTblMeta) {
            PartitionByDefinition tarPartBy = getTargetPartBy(tarTblMeta, tarTblPartLevel);
            tartPartInfo = tarTblMeta.getPartitionInfo();
            partSpecSearcher = tartPartInfo.getPartSpecSearcher();
            List<PartitionSpec> currPartList = tarPartBy.getPartitions();
            for (int i = 0; i < currPartList.size(); i++) {
                PartitionSpec pspec = currPartList.get(i);
                String partName = pspec.getName();
                String bndValStr = pspec.getBoundSpec().getSingleDatum().toStringWithNoBracket();
                allPartBndInfoMapping.put(bndValStr, partName);
            }
        }

        int beginIdx = -1;
        int endIdx = -1;

        String lastBndValStr = null;
        for (int i = 0; i < newAddPartSpecInfosOutput.size(); i++) {
            Pair<String, String> partVal = newAddPartSpecInfosOutput.get(i);
            String bndStr = partVal.getValue();
            if (lastBndValStr == null) {
                lastBndValStr = bndStr;
                beginIdx = i;
                continue;
            }
            if (bndStr.equalsIgnoreCase(lastBndValStr)) {
                beginIdx = i;
            } else {
                break;
            }
        }

        lastBndValStr = null;
        for (int i = newAddPartSpecInfosOutput.size() - 1; i > -1; --i) {
            Pair<String, String> partVal = newAddPartSpecInfosOutput.get(i);
            String bndStr = partVal.getValue();
            if (lastBndValStr == null) {
                lastBndValStr = bndStr;
                endIdx = i;
                continue;
            }
            if (bndStr.equalsIgnoreCase(lastBndValStr)) {
                endIdx = i;
            } else {
                break;
            }
        }

        for (int i = beginIdx; i <= endIdx; i++) {
            Pair<String, String> partSpec = newAddPartSpecInfosOutput.get(i);
            String partNameOfNewPart = partSpec.getKey();
            String bndValStrOfNewPart = partSpec.getValue();

            if (specifyTblMeta && allPartBndInfoMapping.containsKey(bndValStrOfNewPart)) {
                // ignore
                continue;
            }

            String fixedPartNameOfNewPart = null;
            String fixedBndValOfNewPart = null;

            if (specifyTblMeta) {
                PartitionSpec tartPart = partSpecSearcher.getPartSpecByPartName(partNameOfNewPart);
                if (tartPart == null && tarTblPartLevel == PartKeyLevel.SUBPARTITION_KEY) {
                    tartPart = partSpecSearcher.getSubPartTempSpecNameBySubPartTempName(partNameOfNewPart);
                }
                if (tartPart != null) {
                    /**
                     * Found already-exist part by using partNameOfNewPart
                     */
                    fixedPartNameOfNewPart = PartitionNameUtil.buildNewPartNameForDuplicatedPartName(partNameOfNewPart);
                } else {
                    fixedPartNameOfNewPart = partNameOfNewPart;
                }
            } else {
                fixedPartNameOfNewPart = partNameOfNewPart;
            }

            fixedBndValOfNewPart = bndValStrOfNewPart;

            Pair<String, String> fixedNewPartInfo = new Pair<>(fixedPartNameOfNewPart, fixedBndValOfNewPart);
            newAddPartSpecInfosOutputAfterFixing.add(fixedNewPartInfo);

            Pair<Integer, Integer> fixedNewPartIndexMapping =
                new Pair<>(i, newAddPartSpecInfosOutputAfterFixing.size() - 1);
            newAddPartSpecIndexMappingsOutputAfterFixing.add(fixedNewPartIndexMapping);
        }

        if (fixedPartSpecInfosOutput != null) {
            fixedPartSpecInfosOutput.addAll(newAddPartSpecInfosOutputAfterFixing);
        }

        if (fixedPartSpecIndexMappingsOutput != null) {
            fixedPartSpecIndexMappingsOutput.addAll(newAddPartSpecIndexMappingsOutputAfterFixing);
        }

        return true;

    }

    protected static int decideAddPartsCountForTtlTblWithExpiredOverPolicy(int curPartCnt,
                                                                           int expireOverCnt) {
        int finalPreBuildPartCnt = 0;
        if (curPartCnt < expireOverCnt) {
            /**
             * When currPartCount is less than expireOverCount,
             * just add parts and no parts to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = expireOverCnt - curPartCnt;
        } else if (curPartCnt == expireOverCnt) {
            /**
             * When currPartCount is equal to expireOverCount,
             * just add one part, and then the first part will to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = 1;
        } else {
            /**
             * When currPartCount is more than the expireOverCount,
             * then no parts will be added, and then top parts will to be dropped (if ttl_cleanup=on)
             */
            finalPreBuildPartCnt = 0;
        }
        return finalPreBuildPartCnt;
    }

    protected static int decideDropPartsCountForTtlTblWithExpiredOverPolicy(int currPartCnt,
                                                                            int newAddPartsCnt,
                                                                            int expireOverCnt) {
        int partCntToBeDropped = 0;
        int currAllPartCnt = currPartCnt + newAddPartsCnt;
        if (currAllPartCnt <= expireOverCnt) {
            return partCntToBeDropped;
        }
        partCntToBeDropped = currAllPartCnt - expireOverCnt;
        return partCntToBeDropped;
    }
}
