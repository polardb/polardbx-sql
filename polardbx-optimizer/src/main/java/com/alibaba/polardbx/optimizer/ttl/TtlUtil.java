package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTablePreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTimeToLiveDefinitionExpr;
import org.apache.calcite.sql.SqlTimeToLiveExpr;
import org.apache.calcite.sql.SqlTimeToLiveJobExpr;
import org.apache.calcite.util.Pair;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class TtlUtil {

    public static final int MAX_TTL_TMP_NAME_PREFIX_LENGTH = 38;

    /**
     * arc_tmp name format (maxlen 60)：arctmp_(7) + %s(arc_tbl_prefix,max 38) + _%s(hashvalstr, 9)
     */
    public static final String ARC_TMP_NAME_TEMPLATE_WITH_HASHCODE = "arctmp_%s_%s";
    public static final String ARC_TMP_NAME_TEMPLATE_WITHOUT_HASHCODE = "arctmp_%s";

    public static boolean checkIfCreateArcTblLikeRowLevelTtl(SqlCreateTable sqlCreateTable,
                                                             ExecutionContext ec) {
        SqlIdentifier sourceTableNameAst = (SqlIdentifier) sqlCreateTable.getLikeTableName();
        String sourceTableSchema =
            SQLUtils.normalizeNoTrim(
                sourceTableNameAst.names.size() > 1 ? sourceTableNameAst.names.get(0) : ec.getSchemaName());
        String sourceTableName = SQLUtils.normalizeNoTrim(sourceTableNameAst.getLastName());
        if (checkIfUsingRowLevelTtl(sourceTableSchema, sourceTableName, ec)) {
            return true;
        }
        return false;
    }

    public static TtlDefinitionInfo fetchTtlInfoFromCreateTableLikeAst(SqlCreateTable sqlCreateTable,
                                                                       ExecutionContext ec) {
        if (!checkIfCreateArcTblLikeRowLevelTtl(sqlCreateTable, ec)) {
            return null;
        }
        SqlIdentifier sourceTableNameAst = (SqlIdentifier) sqlCreateTable.getLikeTableName();
        String sourceTableSchema =
            SQLUtils.normalizeNoTrim(
                sourceTableNameAst.names.size() > 1 ? sourceTableNameAst.names.get(0) : ec.getSchemaName());
        String sourceTableName = SQLUtils.normalizeNoTrim(sourceTableNameAst.getLastName());
        TtlDefinitionInfo ttlDefinitionInfo = getTtlDefInfoBySchemaAndTable(sourceTableSchema, sourceTableName, ec);
        return ttlDefinitionInfo;
    }

    /**
     * Check if using row-level ttl
     */
    public static boolean checkIfUsingRowLevelTtl(String tableSchema,
                                                  String tableName,
                                                  ExecutionContext ec) {
        if (StringUtils.isEmpty(tableSchema) || StringUtils.isEmpty(tableName)) {
            return false;
        }
        TtlDefinitionInfo ttlDefinitionInfo = getTtlDefInfoBySchemaAndTable(tableSchema, tableName, ec);
        return ttlDefinitionInfo != null;
    }

    /**
     * Check the
     */
    public static boolean checkIfArchiveCciOfTtlTable(String ttlSchema,
                                                      String ttlTblName,
                                                      String fullCciName,
                                                      ExecutionContext ec) {
        if (StringUtils.isEmpty(ttlSchema) || StringUtils.isEmpty(fullCciName)) {
            return false;
        }

        TableMeta ttlTblMeta = ec.getSchemaManager(ttlSchema).getTableWithNull(ttlTblName);
        if (ttlTblMeta == null) {
            return false;
        }
        TtlDefinitionInfo ttlInfo = ttlTblMeta.getTtlDefinitionInfo();
        if (ttlInfo == null) {
            return false;
        }
        if (ttlInfo.needPerformExpiredDataArchivingByCci()) {
            String arcCciName = ttlInfo.getTtlInfoRecord().getArcTmpTblName();
            Map<String, GsiMetaManager.GsiIndexMetaBean> tmpGsiName2Bean =
                new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            tmpGsiName2Bean.putAll(ttlTblMeta.getColumnarIndexPublished());

            if (TtlConfigUtil.isUseGsiInsteadOfCciForCreateColumnarArcTbl(ec)) {
                tmpGsiName2Bean.putAll(ttlTblMeta.getGsiPublished());
            }
            boolean foundArcCci = false;
            if (tmpGsiName2Bean.containsKey(fullCciName) && fullCciName.toLowerCase()
                .startsWith(arcCciName.toLowerCase())) {
                foundArcCci = true;
            }
            return foundArcCci;
        }

        return false;
    }

    public static TtlDefinitionInfo getTtlDefInfoBySchemaAndTable(
        String tableSchema, String tableName, ExecutionContext ec) {

        SchemaManager sm = ec.getSchemaManager(tableSchema);
        if (sm == null) {
            return null;
        }

        TableMeta tableMeta = sm.getTableWithNull(tableName);
        if (tableMeta == null) {
            return null;
        }
        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        return ttlDefinitionInfo;
    }

    public static TtlInfoRecord fetchTtlDefinitionInfoByArcDbAndArcTb(String arcTableSchema,
                                                                      String arcTableName) {
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager mgr = new TableInfoManager();
            mgr.setConnection(metaDbConn);
            TtlInfoRecord record = mgr.getTtlInfoRecordByArchiveTable(arcTableSchema, arcTableName);
            return record;
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TTL, ex);
        }
    }

    public static TtlDefinitionInfo fetchTtlDefinitionInfoByDbAndTb(String tableSchema,
                                                                    String tableName,
                                                                    ExecutionContext ec) {
        TableMeta tableMeta = ec.getSchemaManager(tableSchema).getTable(tableName);
        TtlDefinitionInfo ttlDefinitionInfo = tableMeta.getTtlDefinitionInfo();
        return ttlDefinitionInfo;
    }

    public static SQLPartitionBy buildNewSqlPartByAstForOssTable(ColumnMeta ttlTblTimeColMeta,
                                                                 List<ColumnMeta> ttlTblPkColMetas,
                                                                 int defaultSubHashPartCnt,
                                                                 String minRangeValue,
                                                                 ExecutionContext ec) {

        /**
         * Build 2nd-level part by key
         */
        // Generate the partitioning clause.
        final SQLSubPartitionByHash sqlSubPartitionByHash = new SQLSubPartitionByHash();
        SQLIntegerExpr intExpr = new SQLIntegerExpr(defaultSubHashPartCnt);
        int maxPartColCnt = ec.getParamManager().getInt(ConnectionParams.MAX_PARTITION_COLUMN_COUNT);
        List<SQLIdentifierExpr> subPartKeys = new ArrayList<>();
        for (int i = 0; i < ttlTblPkColMetas.size(); i++) {
            ColumnMeta pkCol = ttlTblPkColMetas.get(i);
            SQLIdentifierExpr idAst = new SQLIdentifierExpr(pkCol.getName());
            subPartKeys.add(idAst);
            if (i >= maxPartColCnt) {
                break;
            }
        }
        sqlSubPartitionByHash.getColumns().addAll(subPartKeys);
        sqlSubPartitionByHash.setKey(true);
        sqlSubPartitionByHash.setSubPartitionsCount(intExpr);

        /**
         * Build 1st-level part by range
         */
        List<SQLIdentifierExpr> partKeys = new ArrayList<>();
        SQLIdentifierExpr ttlColAst = new SQLIdentifierExpr(ttlTblTimeColMeta.getName());
        partKeys.add(ttlColAst);
        List<SQLIdentifierExpr> rangePartKeys = partKeys;

        SQLCharExpr valExpr = new SQLCharExpr(minRangeValue);
        SQLPartitionValue p1SpecVal = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
        p1SpecVal.addItem(valExpr);
        SQLPartition p1PartSpec = new SQLPartition();
        p1PartSpec.setName(new SQLIdentifierExpr("pstart"));
        p1PartSpec.setValues(p1SpecVal);

        SQLPartitionByRange sqlPartitionByRange = new SQLPartitionByRange();
        sqlPartitionByRange.setColumns(true);
        sqlPartitionByRange.getColumns().addAll(rangePartKeys);
        sqlPartitionByRange.getPartitions().add(p1PartSpec);

        /**
         * Set the subpartBy for partBy
         */
        sqlPartitionByRange.setSubPartitionBy(sqlSubPartitionByHash);

        return sqlPartitionByRange;
    }

    public static String buildArcTmpNameByArcTblName(String arcTblName) {
        String finalArcTmpName = "";
        if (StringUtils.isEmpty(arcTblName)) {
            return finalArcTmpName;
        }
        int length = arcTblName.length();
        String ttlTmpNamePrefixStr = arcTblName;
        String ttlTblNameHashCodeHexStr = "";
        if (length > TtlUtil.MAX_TTL_TMP_NAME_PREFIX_LENGTH) {
            ttlTmpNamePrefixStr = arcTblName.substring(0, TtlUtil.MAX_TTL_TMP_NAME_PREFIX_LENGTH);
            ttlTblNameHashCodeHexStr = GroupInfoUtil.doMurmur3Hash32(arcTblName);
            finalArcTmpName =
                String.format(ARC_TMP_NAME_TEMPLATE_WITH_HASHCODE, ttlTmpNamePrefixStr, ttlTblNameHashCodeHexStr);
        } else {
            finalArcTmpName = String.format(ARC_TMP_NAME_TEMPLATE_WITHOUT_HASHCODE, ttlTmpNamePrefixStr);
        }
        finalArcTmpName = finalArcTmpName.toLowerCase();
        return finalArcTmpName;
    }

    public static boolean checkIfDropTtlTableWithCciArcTblView(String ttlTableSchema,
                                                               String ttlTableName,
                                                               ExecutionContext executionContext) {

        TableMeta ttlTblMeta = executionContext.getSchemaManager(ttlTableSchema).getTableWithNull(ttlTableName);
        if (ttlTblMeta == null) {
            return false;
        }
        TtlDefinitionInfo ttlInfo = ttlTblMeta.getTtlDefinitionInfo();
        if (ttlInfo == null) {
            return false;
        }
        if (!ttlInfo.needPerformExpiredDataArchivingByCci()) {
            return false;
        }
        return true;
    }

    public static boolean checkIfDropCciOfArcTblView(String ttlTableSchema,
                                                     String ttlTableName,
                                                     String cciIndexName,
                                                     ExecutionContext executionContext) {

        TableMeta ttlTblMeta = executionContext.getSchemaManager(ttlTableSchema).getTableWithNull(ttlTableName);
        if (ttlTblMeta == null) {
            return false;
        }
        TtlDefinitionInfo ttlInfo = ttlTblMeta.getTtlDefinitionInfo();
        if (ttlInfo == null) {
            return false;
        }
        if (!ttlInfo.needPerformExpiredDataArchivingByCci()) {
            return false;
        }

        if (!cciIndexName.toLowerCase().startsWith(ttlInfo.getTmpTableName().toLowerCase())) {
            return false;
        }
        return true;
    }

    public static boolean checkIfAllowedDropTableOperation(String targetTableSchema,
                                                           String targetTableName,
                                                           ExecutionContext executionContext) {

        /**
         * Check if tarDb is autodb,
         * if false, ignore check and retrun false;
         */
        if (!DbInfoManager.getInstance().isNewPartitionDb(targetTableSchema)) {
            return true;
        }

        boolean enableUseGsiInsteadOfCci =
            executionContext.getParamManager().getBoolean(ConnectionParams.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL);
        boolean forbidDropTableWithArcCci =
            executionContext.getParamManager().getBoolean(ConnectionParams.TTL_FORBID_DROP_TTL_TBL_WITH_ARC_CCI);

        /**
         * Check if tarDb.tarTb a view
         */
        ViewManager viewManager = OptimizerContext.getContext(targetTableSchema).getViewManager();
        if (viewManager != null) {
            SystemTableView.Row view = viewManager.select(targetTableName);

            /**
             * Check if view is a archive table view of ttl-table forcing cci
             */

            if (view != null) {
                boolean dropViewForArcCci =
                    checkIfDropArcTblViewOfTtlTableWithCci(targetTableSchema, targetTableName, executionContext);
                if (dropViewForArcCci) {
                    if (forbidDropTableWithArcCci) {
                        return false;
                    } else {
                        /**
                         * the tarDb.tarTb is view, and allowed drop arc_view of cci, so can return direclty
                         */
                        return true;
                    }
                }
            }
        }

        TableMeta targetTableMeta =
            executionContext.getSchemaManager(targetTableSchema).getTableWithNull(targetTableName);
        if (targetTableMeta != null) {

            PartitionInfo targetPartInfo = targetTableMeta.getPartitionInfo();

            /**
             * Check if tarDb.tarTb has ttl-definition
             */
            TtlDefinitionInfo ttlInfo = targetTableMeta.getTtlDefinitionInfo();
            if (ttlInfo == null) {
                /**
                 * tarDb.tarTb is NOT ttl-definition table
                 */

                PartitionTableType tableType = targetPartInfo.getTableType();
                boolean isCciTbl = tableType == PartitionTableType.COLUMNAR_TABLE;
                boolean isGsiTbl = tableType.isGsiTableType();
                boolean needCheckIfForbidDropOperation = isCciTbl;
                if (enableUseGsiInsteadOfCci) {
                    needCheckIfForbidDropOperation |= isGsiTbl;
                }
                if (needCheckIfForbidDropOperation && forbidDropTableWithArcCci) {
                    /**
                     * IF the tarDb.tarTb is the cci/gsi of a ttl-definition table,
                     * now allow droping
                     */
                    return false;
                }
            } else {
                /**
                 * Now the tarDb.tarTb is ttl-definition table
                 */

                /**
                 * Check if the ttl-definition table contain the archive table cci
                 */
                if (ttlInfo.performArchiveByColumnarIndex() && forbidDropTableWithArcCci) {
                    return false;
                }
            }
            return true;
        }
        return true;
    }

    public static boolean checkIfDropArcTblViewOfTtlTableWithCci(String arcTableSchema,
                                                                 String arcTableName,
                                                                 ExecutionContext executionContext) {

        final String schemaName = arcTableSchema;
        final String logicalTableName = arcTableName;

        boolean dropArcTblViewAndCiForTtlTbl = false;
        ViewManager viewManager = OptimizerContext.getContext(schemaName).getViewManager();
        if (viewManager == null) {
            /**
             * "viewManager == null" should NOT come here
             */
            return dropArcTblViewAndCiForTtlTbl;
        }
        SystemTableView.Row viewInfo = viewManager.select(logicalTableName);
        if (viewInfo != null) {

            /**
             * The logicalTableName is a View !!!
             * Check if it is a archive table view of ttl table with columnar index
             */

            TtlInfoRecord ttlInfoRec = TtlUtil.fetchTtlDefinitionInfoByArcDbAndArcTb(schemaName, logicalTableName);
            if (ttlInfoRec != null) {
                /**
                 * Current db.tb is actually a arc table view of ttl-table
                 */
                String ttlTblSchema = ttlInfoRec.getTableSchema();
                String ttlTblName = ttlInfoRec.getTableName();
                TableMeta ttlTblTm = executionContext.getSchemaManager(ttlTblSchema).getTableWithNull(ttlTblName);
                if (ttlTblTm != null) {
                    TtlDefinitionInfo ttlInfo = ttlTblTm.getTtlDefinitionInfo();
                    if (ttlInfo.performArchiveByColumnarIndex()) {
                        String ciNameOfArcTbl = ttlInfo.getTmpTableName();
                        boolean useGsiForCci =
                            TtlConfigUtil.isUseGsiInsteadOfCciForCreateColumnarArcTbl(executionContext);
                        if (useGsiForCci) {
                            boolean withGsi = ttlTblTm.withGsi();
                            if (withGsi) {
                                dropArcTblViewAndCiForTtlTbl = ttlTblTm.withGsi(ciNameOfArcTbl);
                            }
                        } else {
                            boolean withCci = ttlTblTm.withCci();
                            if (withCci) {
                                dropArcTblViewAndCiForTtlTbl = ttlTblTm.withCci(ciNameOfArcTbl);
                            }
                        }
                    }
                }

            }
        }
        return dropArcTblViewAndCiForTtlTbl;
    }

    public static boolean useTimestampOnTtlCol(TtlDefinitionInfo ttlInfo, ExecutionContext ec) {
        boolean result = false;
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
        TableMeta tblMeta = ec.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        ColumnMeta ttlCm = tblMeta.getColumn(ttlColName);
        DataType ds = ttlCm.getDataType();
        result = DataTypeUtil.isTimezoneDependentType(ds);
        return result;
    }

    public static boolean checkIfNeedRefreshViewForArcTbl(AlterTablePreparedData preparedData,
                                                          ExecutionContext ec) {
        String tableSchema = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();

        TtlDefinitionInfo ttlDefinitionInfo = fetchTtlDefinitionInfoByDbAndTb(tableSchema, tableName, ec);
        if (ttlDefinitionInfo == null) {
            return false;
        }

        if (!ttlDefinitionInfo.needPerformExpiredDataArchivingByCci()) {
            return false;
        }

        boolean needFreshView = preparedData.hasColumnModify();
        return needFreshView;
    }

    private static String getValueFromLiteral(SqlNode charExprAst) {
        String val = SQLUtils.normalize(((SqlCharStringLiteral) charExprAst).toValue());
        return val;
    }

    protected static String getFirstColumnarIndexName(SqlCreateTable sqlCreateTable) {

        List<Pair<SqlIdentifier, SqlIndexDefinition>> cciIdxInfos = sqlCreateTable.getColumnarKeys();
        if (cciIdxInfos == null || cciIdxInfos.isEmpty()) {
            return null;
        }
        SqlIndexDefinition cciDef = cciIdxInfos.get(0).getValue();
        SqlIdentifier cciRawName = cciDef.getOriginIndexName();
        String cciRawNameStr = cciRawName.getLastName();
        return cciRawNameStr;
    }

    public static TtlDefinitionInfo createTtlDefinitionInfoBySqlCreateTable(SqlCreateTable sqlCreateTable,
                                                                            TableMeta tableMeta,
                                                                            PartitionInfo newCreateTblPartInfo,
                                                                            ExecutionContext executionContext
    ) {
        boolean foundTtlDefinition = false;

        SqlNode ttlDefinitionExprNode = sqlCreateTable.getTtlDefinition();
        if (ttlDefinitionExprNode != null && ttlDefinitionExprNode instanceof SqlTimeToLiveDefinitionExpr) {
            foundTtlDefinition = true;
        }

        SqlTimeToLiveDefinitionExpr ttlDefinitionExprAst = (SqlTimeToLiveDefinitionExpr) ttlDefinitionExprNode;

        if (foundTtlDefinition) {
            if (sqlCreateTable.getLocalPartition() != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_CREATE_TABLE_WITH_TTL,
                    "create table with ttl definition is not allowed using local partition");
            }
        }

        if (!foundTtlDefinition) {
            return null;
        }

        String schemaName = tableMeta.getSchemaName();
        String tableName = tableMeta.getTableName();
        TtlDefinitionInfo ttlDefinitionInfo = null;
        SqlNode ttlEnableExpr = ttlDefinitionExprAst.getTtlEnableExpr();
        SqlNode ttlExprNode = ttlDefinitionExprAst.getTtlExpr();
        SqlNode ttlJobNode = ttlDefinitionExprAst.getTtlJobExpr();
//        SqlNode ttlFilterExpr = ttlDefinitionExprAst.getTtlFilterExpr();
        SqlNode archiveTypeExpr = ttlDefinitionExprAst.getArchiveTypeExpr();
        SqlNode archiveTableSchemaExpr = ttlDefinitionExprAst.getArchiveTableSchemaExpr();
        SqlNode archiveTableNameExpr = ttlDefinitionExprAst.getArchiveTableNameExpr();
        SqlNode archiveTablePreAllocateExpr = ttlDefinitionExprAst.getArchiveTablePreAllocateExpr();
        SqlNode archiveTablePostAllocateExpr = ttlDefinitionExprAst.getArchiveTablePostAllocateExpr();

        String ttlEnable = TtlInfoRecord.TTL_STATUS_DISABLE_SCHEDULE_STR_VAL;
        if (ttlEnableExpr != null) {
            ttlEnable = ttlEnableExpr.toString();
        }

        SqlTimeToLiveExpr ttlExpr = (SqlTimeToLiveExpr) ttlExprNode;
        SqlTimeToLiveJobExpr ttlJob = null;
        if (ttlJobNode != null) {
            ttlJob = (SqlTimeToLiveJobExpr) ttlJobNode;
        }

        List<ColumnMeta> pkColMetas = tableMeta.getPrimaryKey().stream().collect(Collectors.toList());
        List<String> pkColNames = new ArrayList<>();
        for (int i = 0; i < pkColMetas.size(); i++) {
            ColumnMeta pkCm = pkColMetas.get(i);
            pkColNames.add(pkCm.getName());
        }

        String archiveTypeVal = null;
        if (archiveTypeExpr != null) {
            archiveTypeVal = getValueFromLiteral(archiveTypeExpr);
        }

        String archiveTableSchemaVal = null;
        if (archiveTableSchemaExpr != null) {
            archiveTableSchemaVal = getValueFromLiteral(archiveTableSchemaExpr);
        }

        String archiveTableNameVal = null;
        if (archiveTableNameExpr != null) {
            archiveTableNameVal = getValueFromLiteral(archiveTableNameExpr);
        }

        Integer arcPreAllocateVal = TtlConfigUtil.getPreBuiltPartCntForCreatColumnarIndex();
        if (archiveTablePreAllocateExpr != null) {
            arcPreAllocateVal = Integer.valueOf(archiveTablePostAllocateExpr.toString());
        }

        Integer arcPostAllocateVal = TtlConfigUtil.getPostBuiltPartCntForCreateColumnarIndex();
        if (archiveTablePostAllocateExpr != null) {
            arcPostAllocateVal = Integer.valueOf(archiveTablePostAllocateExpr.toString());
        }

        ttlDefinitionInfo = TtlDefinitionInfo.createNewTtlInfo(schemaName, tableName,
            ttlEnable, ttlExpr, ttlJob, archiveTypeVal, archiveTableSchemaVal, archiveTableNameVal, arcPreAllocateVal,
            arcPostAllocateVal, pkColNames, tableMeta,
            newCreateTblPartInfo,
            sqlCreateTable,
            executionContext);

        return ttlDefinitionInfo;
    }

}
