package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.charset.MySQLCharsetDDLValidator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.ImportTableTaskManager;
import com.alibaba.polardbx.executor.utils.StandardToEnterpriseEditionUtil;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.topology.CreateDbInfo;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.ImportTableResult;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.DbEventUtil;
import com.alibaba.polardbx.gms.util.DbNameUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalImportDatabase;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.utils.KeyWordsUtil;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlImportDatabase;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class LogicalImportDatabaseHandler extends LogicalCreateDatabaseHandler {
    private final static Logger logger = LoggerFactory.getLogger(LogicalImportDatabaseHandler.class);

    public LogicalImportDatabaseHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalImportDatabase importDatabase = (LogicalImportDatabase) logicalPlan;
        final SqlImportDatabase sqlImportDatabase = (SqlImportDatabase) importDatabase.getNativeSqlNode();

        handleImportDatabase(sqlImportDatabase, executionContext);

        Map<String, ImportTableResult> result = handleImportTablesParallel(sqlImportDatabase, executionContext);

        return buildResult(result);
    }

    protected void handleImportDatabase(SqlImportDatabase sqlImportDatabase, ExecutionContext executionContext) {

        validateImportDatabase(sqlImportDatabase, executionContext);

        String logicalDbName = SQLUtils.normalize(sqlImportDatabase.getDstLogicalDb());
        String phyDbName = SQLUtils.normalize(sqlImportDatabase.getSrcPhyDb());
        String locality = SQLUtils.normalize(sqlImportDatabase.getLocality());
        LocalityDesc localityDesc = LocalityDesc.parse(locality);

        String dnName = localityDesc.getDnList().get(0);
        Predicate<StorageInfoRecord> predLocality = (x -> localityDesc.matchStorageInstance(x.getInstanceId()));

        Map<String, String> schemata = StandardToEnterpriseEditionUtil.queryDatabaseSchemata(dnName, phyDbName);
        if (schemata.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query information on phy database [%s]", phyDbName));
        }
        Boolean encryption = null;
        //for mysql 8.0
        if (schemata.get("DEFAULT_ENCRYPTION") != null &&
            (schemata.get("DEFAULT_ENCRYPTION").equalsIgnoreCase("YES") || schemata.get("DEFAULT_ENCRYPTION")
                .equalsIgnoreCase("TRUE"))) {
            encryption = true;
        }
        String charset = schemata.get("DEFAULT_CHARACTER_SET_NAME");
        String collate = schemata.get("DEFAULT_COLLATION_NAME");
        int dbType = DbInfoRecord.DB_TYPE_NEW_PART_DB;
        Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
        long socketTimeoutVal = socketTimeout == null ? -1 : socketTimeout;
        CreateDbInfo createDbInfo = StandardToEnterpriseEditionUtil.initCreateDbInfo(
            logicalDbName, phyDbName, charset, collate, encryption, localityDesc, predLocality, dbType,
            socketTimeoutVal, sqlImportDatabase.isExistStillImport()
        );

        long dbId = DbTopologyManager.createLogicalDb(createDbInfo);
        DbEventUtil.logStandardToEnterpriseEditionEvent(logicalDbName, phyDbName);
        CdcManagerHelper.getInstance()
            .notifyDdl(logicalDbName, null, sqlImportDatabase.getKind().name(), executionContext.getOriginSql(),
                null, CdcDdlMarkVisibility.Public, buildExtendParameter(executionContext));

        LocalityManager.getInstance().setLocalityOfDb(dbId, locality);
    }

    protected void validateImportDatabase(SqlImportDatabase sqlImportDatabase, ExecutionContext executionContext) {
        String logicalDbName = SQLUtils.normalize(sqlImportDatabase.getDstLogicalDb());
        String phyDbName = SQLUtils.normalize(sqlImportDatabase.getSrcPhyDb());
        String locality = SQLUtils.normalize(sqlImportDatabase.getLocality());
        LocalityDesc localityDesc = LocalityDesc.parse(locality);
        String storageInstId = localityDesc.getDnList().get(0);

        //每次只允许指定1个DN
        if (localityDesc.getDnList().size() != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "only one DN Id is allowed");
        }

        //validate db name
        if (!DbNameUtil.validateDbName(logicalDbName, KeyWordsUtil.isKeyWord(logicalDbName))) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("Failed to import database because the db name [%s] is invalid", logicalDbName));
        }

        //validate db count
        int normalDbCnt = DbTopologyManager.getNormalDbCountFromMetaDb();
        int maxDbCnt = DbTopologyManager.maxLogicalDbCount;
        if (normalDbCnt >= maxDbCnt) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Failed to import database because there are too many databases, the max count of database is %s",
                    maxDbCnt));
        }

        //validate "exist still import"
        if (!sqlImportDatabase.isExistStillImport()) {
            //不允许把 "有逻辑库的物理库" import到新的逻辑库
            List<DbGroupInfoRecord> dbGroupInfoRecords =
                DbTopologyManager.getAllDbGroupInfoRecordByInstIdAndPhyDbName(phyDbName, storageInstId);
            if (dbGroupInfoRecords.size() > 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                    "It's not allowed to import phy database [%s] because it belongs to another logical db", phyDbName
                ));
            }

            //不允许把 物理库 import 到正常的逻辑库中
            if (null != DbInfoManager.getInstance().getDbInfo(logicalDbName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                    "It's not allowed to import phy database on existing logical database [%s]", logicalDbName
                ));
            }
        } else {
            //必须是"已经import过"的物理库和逻辑库
            List<DbGroupInfoRecord> dbGroupInfoRecords =
                DbTopologyManager.getAllDbGroupInfoRecordByInstIdAndPhyDbName(phyDbName, storageInstId);
            if (dbGroupInfoRecords.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                    "phy database [%s] must belong to a logical database", phyDbName
                ));
            }

            if (null == DbInfoManager.getInstance().getDbInfo(logicalDbName)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                    "logical database [%s] must exist", logicalDbName
                ));
            }

            //库内不允许存在"分区表" "广播表"
            Set<String> tablesInLogicalDatabase = StandardToEnterpriseEditionUtil
                .getTableNamesFromLogicalDatabase(logicalDbName, executionContext);
            final SchemaManager sm = OptimizerContext.getContext(logicalDbName).getLatestSchemaManager();
            for (String table : tablesInLogicalDatabase) {
                TableMeta tableMeta = sm.getTable(table);
                if (tableMeta == null) {
                    continue;
                }
                if (tableMeta.getPartitionInfo().getTableType() != PartitionTableType.SINGLE_TABLE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, String.format(
                        "import database failed because of invalid table type, table [%s] is %s", table,
                        tableMeta.getPartitionInfo().getTableType()));
                }
            }
        }

        //validate charset
        Map<String, String> schemata = StandardToEnterpriseEditionUtil.queryDatabaseSchemata(storageInstId, phyDbName);
        if (schemata.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format("failed to query information on phy database [%s]", phyDbName));
        }
        Boolean encryption = schemata.get("DEFAULT_ENCRYPTION") != null &&
            !(schemata.get("DEFAULT_ENCRYPTION").equalsIgnoreCase("NO") || schemata.get("DEFAULT_ENCRYPTION")
                .equalsIgnoreCase("FALSE"));
        String charset = schemata.get("DEFAULT_CHARACTER_SET_NAME");
        String collate = schemata.get("DEFAULT_COLLATION_NAME");
        validateCharset(charset, collate);
    }

    protected Map<String, ImportTableResult> handleImportTablesParallel(SqlImportDatabase sqlImportDatabase,
                                                                        ExecutionContext executionContext) {

        String logicalDbName = sqlImportDatabase.getDstLogicalDb();
        String phyDbName = sqlImportDatabase.getSrcPhyDb();
        String locality = SQLUtils.normalize(sqlImportDatabase.getLocality());
        LocalityDesc localityDesc = LocalityDesc.parse(locality);
        String storageInstId = localityDesc.getDnList().get(0);

        Set<String> tablesInLogicalDatabase = StandardToEnterpriseEditionUtil
            .getTableNamesFromLogicalDatabase(logicalDbName, executionContext);

        Set<String> tablesInPhysicalDatabase = StandardToEnterpriseEditionUtil
            .queryPhysicalTableListFromPhysicalDabatase(storageInstId, phyDbName);

        //逻辑元信息还在，物理表已经被删除
        Set<String> needCleanTables = new TreeSet<>(String::compareToIgnoreCase);
        //没有逻辑元信息，物理表存在
        Set<String> needCreateTables = new TreeSet<>(String::compareToIgnoreCase);
        //逻辑元信息和物理表都存在，但check table不通过，需要重建元信息
        Set<String> needRecreateTables = new TreeSet<>(String::compareToIgnoreCase);

        for (String phyTable : tablesInPhysicalDatabase) {
            if (!tablesInLogicalDatabase.contains(phyTable)) {
                needCreateTables.add(phyTable);
            }
        }

        for (String logicalTable : tablesInLogicalDatabase) {
            if (!tablesInPhysicalDatabase.contains(logicalTable)) {
                needCleanTables.add(logicalTable);
            }
        }

        List<String> needCheckTables = tablesInLogicalDatabase.stream()
            .filter(tb -> !needCleanTables.contains(tb)).collect(Collectors.toList());
        for (String needCheckTable : needCheckTables) {
            if (!StandardToEnterpriseEditionUtil.logicalCheckTable(
                needCheckTable, logicalDbName
            )) {
                needRecreateTables.add(needCheckTable);
            }
        }

        Map<String, ImportTableResult> importTableResults = new CaseInsensitiveConcurrentHashMap<ImportTableResult>();
        int parallelism = executionContext.getParamManager().getInt(ConnectionParams.IMPORT_TABLE_PARALLELISM);
        ImportTableTaskManager manager = null;
        try {
            manager = new ImportTableTaskManager(parallelism);
            List<FutureTask<Object>> taskList = new ArrayList<>();

            for (String needCreateTable : needCreateTables) {
                taskList.add(new FutureTask<Object>(() -> {
                    importOneTable(storageInstId, logicalDbName, phyDbName, needCreateTable, false, importTableResults);
                    return new Object();
                }));
            }

            for (String needReCreateTable : needRecreateTables) {
                taskList.add(new FutureTask<Object>(() -> {
                    importOneTable(storageInstId, logicalDbName, phyDbName, needReCreateTable, true,
                        importTableResults);
                    return new Object();
                }));
            }

            for (String needCleanTable : needCleanTables) {
                taskList.add(new FutureTask<Object>(() -> {
                    cleanOneTable(logicalDbName, needCleanTable, importTableResults);
                    return new Object();
                }));
            }

            for (FutureTask<Object> task : taskList) {
                manager.execute(task);
            }

            //wait to finish
            for (FutureTask<Object> task : taskList) {
                try {
                    task.get();
                } catch (Exception e) {
                    logger.error(String.format("import table failed. " + task), e);
                }
            }

        } catch (Exception e) {
            logger.error(String.format("import database failed. "), e);
        } finally {
            if (manager != null) {
                manager.shutdown();
            }
        }

        return importTableResults;
    }

    protected void importOneTable(String dnName, String logicalDbName, String phyDbName, String tableName,
                                  boolean reimport, Map<String, ImportTableResult> result) {
        final String importTableHint = "/*+TDDL: import_table=true CN_FOREIGN_KEY_CHECKS=0*/ ";
        final String reimportTableHint = "/*+TDDL: reimport_table=true CN_FOREIGN_KEY_CHECKS=0*/ ";
        final String action = (reimport ? "reimport table" : "import table");

        String createTableSql = StandardToEnterpriseEditionUtil.queryCreateTableSql(
            dnName, phyDbName, tableName
        );

        LocalityDesc localityDesc = new LocalityDesc(ImmutableList.of(dnName));

        String finalSql = null;
        try {
            String normalizedCreateSql =
                StandardToEnterpriseEditionUtil.normalizePhyTableStructure(tableName, createTableSql,
                    localityDesc.toString());

            finalSql = (reimport ? reimportTableHint : importTableHint) + normalizedCreateSql;

            DdlHelper.getServerConfigManager().executeBackgroundSql(finalSql, logicalDbName, null);
        } catch (Exception e) {
            //collect failed msg
            ImportTableResult importTableResult = new ImportTableResult();
            importTableResult.setTableName(tableName);
            importTableResult.setAction(action);
            importTableResult.setPhysicalCreateTableSql(createTableSql);
            importTableResult.setLogicalCreateTableSql(finalSql);
            importTableResult.setErrMsg(e.getMessage());

            logger.error(String.format("import table %s failed. ", tableName), e);

            result.put(tableName, importTableResult);
        }
    }

    protected void cleanOneTable(String logicalDbName, String logicalTableName, Map<String, ImportTableResult> result) {
        final String importTableHint = "/*+TDDL: import_table=true CN_FOREIGN_KEY_CHECKS=0*/ ";
        final String dropTableSql = "drop table `%s` ";
        final String action = "clean table";

        final String finalSql = String.format(
            importTableHint + dropTableSql, logicalTableName
        );
        try {
            DdlHelper.getServerConfigManager().executeBackgroundSql(finalSql, logicalDbName, null);
        } catch (Exception e) {
            //collect failed msg
            ImportTableResult importTableResult = new ImportTableResult();
            importTableResult.setTableName(logicalTableName);
            importTableResult.setAction(action);
            importTableResult.setErrMsg(e.getMessage());

            logger.error(String.format("clean table %s failed. ", logicalTableName), e);

            result.put(logicalTableName, importTableResult);
        }
    }

    private void validateCharset(String charset, String collate) {
        boolean useMySql80 = ExecUtils.isMysql80Version();
        if (!MySQLCharsetDDLValidator.checkCharsetSupported(charset, collate, true)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The specified charset[%s] or collate[%s] is not supported",
                    charset, collate));
        }
        if (MySQLCharsetDDLValidator.checkIfMySql80NewCollation(collate) && !useMySql80) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                String.format(
                    "The specified charset[%s] or collate[%s] is only supported for mysql 8.0",
                    charset, collate));
        }
        if (!MySQLCharsetDDLValidator.checkCharset(charset)) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                String.format(
                    "Unknown character set: %s",
                    charset));
        }

        if (!StringUtils.isEmpty(collate)) {
            if (!MySQLCharsetDDLValidator.checkCollation(collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown collation: %s",
                        collate));
            }

            if (!MySQLCharsetDDLValidator.checkCharsetCollation(charset, collate)) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    String.format(
                        "Unknown character set and collation: %s %s",
                        charset, collate));
            }
        }
    }

    Cursor buildResult(Map<String, ImportTableResult> resultMap) {
        ArrayResultCursor result = new ArrayResultCursor("Result");
        if (!resultMap.isEmpty()) {
            result.addColumn("table_name", DataTypes.StringType);
            result.addColumn("action", DataTypes.StringType);
            result.addColumn("state", DataTypes.StringType);
            result.addColumn("physical_sql", DataTypes.StringType);
            result.addColumn("logical_sql", DataTypes.StringType);
            result.addColumn("err_msg", DataTypes.StringType);

            for (ImportTableResult record : resultMap.values()) {
                result.addRow(
                    new Object[] {
                        record.getTableName(),
                        record.getAction(),
                        "fail",
                        record.getPhysicalCreateTableSql(),
                        record.getLogicalCreateTableSql(),
                        record.getErrMsg()
                    }
                );
            }
        } else {
            result.addColumn("state", DataTypes.StringType);
            result.addRow(new Object[] {"ALL SUCCESS"});
        }

        return result;
    }

    public static class CaseInsensitiveConcurrentHashMap<T> extends ConcurrentHashMap<String, T> {

        @Override
        public T put(String key, T value) {
            return super.put(key.toLowerCase(), value);
        }

        public T get(String key) {
            return super.get(key.toLowerCase());
        }

        public boolean containsKey(String key) {
            return super.containsKey(key.toLowerCase());
        }

        public T remove(String key) {
            return super.remove(key.toLowerCase());
        }
    }

}
