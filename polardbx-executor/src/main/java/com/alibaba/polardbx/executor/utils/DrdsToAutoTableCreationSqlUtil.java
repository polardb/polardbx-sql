/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDataTypeImpl;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLIndexDefinition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByCoHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.SQLPartitionValue;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartition;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByCoHash;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByHash;
import com.alibaba.polardbx.druid.sql.ast.SQLSubPartitionByRange;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCharacterDataType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlPrimaryKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlUnique;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSubPartitionByKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlTableIndex;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.sharding.utils.DrdsDefaultPartitionNumUtil;
import com.alibaba.polardbx.repo.mysql.handler.LogicalShowTablesMyHandler;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlShowTables;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.TddlConstants.AUTO_SHARD_KEY_PREFIX;
import static com.alibaba.polardbx.gms.metadb.limit.Limits.MAX_LENGTH_OF_IDENTIFIER_NAME;
import static java.lang.Math.min;

/**
 * Created by zhuqiwei.
 */
public class DrdsToAutoTableCreationSqlUtil {
    static final int largestIndexPrefixLengthInBytes = 767;
    static final String TABLE_TYPE = "base table";

    //ignore the 'locality' option in auto-mode db
    public static String buildCreateAutoModeDatabaseSql(String drdsSchemaName, String autoSchemaName) {
        final String createAutoDatabaseSql =
            "create database if not exists " + SqlIdentifier.surroundWithBacktick(autoSchemaName);
        final String charSetOption = " CHARSET=";
        final String collateOption = " COLLATE=";
        final String partitionMode = " PARTITION_MODE=auto";

        String createSql = createAutoDatabaseSql;
        DbInfoManager dbInfoManager = DbInfoManager.getInstance();
        DbInfoRecord drdsRecord = dbInfoManager.getDbInfo(drdsSchemaName);
        if (StringUtils.isNotEmpty(drdsRecord.charset)) {
            createSql += (charSetOption + drdsRecord.charset);
        }
        if (StringUtils.isNotEmpty(drdsRecord.collation)) {
            createSql += (collateOption + drdsRecord.collation);
        }
        createSql += (partitionMode + ";");

        return createSql;
    }

    public static String convertDrdsModeCreateTableSqlToAutoModeSql(String drdsSql, boolean ifNotExists,
                                                                    int maxPartitionsNum,
                                                                    int maxPartitionColumnNum,
                                                                    ExecutionContext ec) {
        final MySqlCreateTableStatement createTableStatementDrds =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(drdsSql).get(0);

        MySqlCreateTableStatement createTableStatementAuto
            = convertDrdsStatementToAutoStatement(createTableStatementDrds, maxPartitionsNum,
            maxPartitionColumnNum, ec);
        if (ifNotExists) {
            createTableStatementAuto.setIfNotExiists(true);
        }
        return createTableStatementAuto.toString();
    }

    public static List<String> getAllGsiTableName(String createTableSql) {
        final MySqlCreateTableStatement createTableStatement =
            (MySqlCreateTableStatement) FastsqlUtils.parseSql(createTableSql).get(0);
        List<String> gsiNames = new ArrayList<>();
        List<SQLTableElement> tableElementList = createTableStatement.getTableElementList();

        for (int i = 0; i < tableElementList.size(); i++) {
            SQLTableElement element = tableElementList.get(i);
            if (element instanceof MySqlTableIndex) {
                MySqlTableIndex gsi = (MySqlTableIndex) element;
                if (gsi.isGlobal() || gsi.isClustered()) {
                    gsiNames.add(SQLUtils.normalize(gsi.getName().getSimpleName()));
                }
            } else if (element instanceof MySqlUnique) {
                MySqlUnique uniqueIndex = (MySqlUnique) element;
                if (uniqueIndex.isGlobal() || uniqueIndex.isClustered()) {
                    gsiNames.add(SQLUtils.normalize(uniqueIndex.getName().getSimpleName()));
                }
            }
        }

        return gsiNames;
    }

    public static MySqlCreateTableStatement convertDrdsStatementToAutoStatement(
        MySqlCreateTableStatement drdsCreateTableStatement,
        int maxPartitionsNum,
        int maxPartitionColumnNum,
        ExecutionContext ec) {
        String tableCharset = drdsCreateTableStatement.getOption("CHARSET") == null ? null :
            ((SQLIdentifierExpr) drdsCreateTableStatement.getOption("CHARSET")).getSimpleName();
        Map<String, Integer> columnsLengthsInBytes =
            tryToCaclColumnsMaxLenInBytes(drdsCreateTableStatement.getColumnDefinitions(), tableCharset);

        handleDrdsModeSequence(drdsCreateTableStatement);

        MySqlCreateTableStatement autoModeCreateTableStatement = drdsCreateTableStatement.clone();

        /**
         * eliminate some local index:
         * 1. implicit primary column and index
         * 2. auto_shard_key for sharding
         * */
        eliminateImplicitKeyAndAutoShardKey(autoModeCreateTableStatement);
        eliminateImplicitKeyAndAutoShardKey(drdsCreateTableStatement);

        eliminateDbPartitionAndTbPartition(autoModeCreateTableStatement);
        //single table
        if (drdsCreateTableStatement.isSingle()
            || !drdsCreateTableStatement.isBroadCast() && drdsCreateTableStatement.getDbPartitionBy() == null
            && drdsCreateTableStatement.getTablePartitionBy() == null) {
            autoModeCreateTableStatement.setSingle(true);
            return autoModeCreateTableStatement;
        } else if (drdsCreateTableStatement.isBroadCast()) {
            //broadcast table
            autoModeCreateTableStatement.setBroadCast(true);
            return autoModeCreateTableStatement;
        } else {
            SQLMethodInvokeExpr drdsDbPartitionBy = (SQLMethodInvokeExpr) drdsCreateTableStatement.getDbPartitionBy();
            SQLIntegerExpr drdsDbPartitions = (SQLIntegerExpr) drdsCreateTableStatement.getDbPartitions();
            SQLMethodInvokeExpr drdsTbPartitionBy =
                (SQLMethodInvokeExpr) drdsCreateTableStatement.getTablePartitionBy();
            SQLIntegerExpr drdsTbPartitions = (SQLIntegerExpr) drdsCreateTableStatement.getTablePartitions();
            final int dbPartitionNum =
                (drdsDbPartitions == null) ? DrdsDefaultPartitionNumUtil.getDrdsDefaultDbPartitionNum(ec) :
                    drdsDbPartitions.getNumber().intValue();
            final int tbPartitionNum =
                (drdsTbPartitions == null) ? DrdsDefaultPartitionNumUtil.getDrdsDefaultTbPartitionNum() :
                    drdsTbPartitions.getNumber().intValue();
            final int drdsDbPartitionNum = dbPartitionNum;
            final int drdsTbPartitionNum = min(tbPartitionNum, maxPartitionsNum / dbPartitionNum);

            List<String> primaryKey = drdsCreateTableStatement.getPrimaryKeyNames();

            //only dbpartition
            if (drdsDbPartitionBy != null && drdsTbPartitionBy == null
                || drdsDbPartitionBy == null && drdsTbPartitionBy != null) {
                //handle gsi
                handleAutoModeGsi(autoModeCreateTableStatement, maxPartitionsNum, maxPartitionColumnNum,
                    columnsLengthsInBytes, ec);

                SQLMethodInvokeExpr drdsPartitionBy =
                    (drdsDbPartitionBy == null) ? drdsTbPartitionBy : drdsDbPartitionBy;
                int drdsPartitionNum = (drdsDbPartitionBy == null) ? drdsTbPartitionNum : drdsDbPartitionNum;
                SQLPartitionBy autoPartitionBy =
                    convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsPartitionBy, drdsPartitionNum, primaryKey,
                        maxPartitionColumnNum, columnsLengthsInBytes);

                /**
                 * dbpartition by range_hash(c1,c2,n) ==> partition by co_hash(substr(c1,-n), substr(c2,-n)),
                 * so ignore creating gis for c2
                 */
//                if (drdsPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
//                    MySqlTableIndex cgsiOnCol2 =
//                        generateCgsiForRangeHash2ndCol(drdsPartitionBy, drdsPartitionNum, primaryKey,
//                            maxPartitionColumnNum, columnsLengthsInBytes);
//                    autoModeCreateTableStatement.getTableElementList().add(cgsiOnCol2);
//                }

                autoModeCreateTableStatement.setPartitioning(autoPartitionBy);
            } else if (drdsDbPartitionBy != null && drdsTbPartitionBy != null) {
                //contain dbpartition and tbpartition
                /**
                 * 这里不区db和tb的分拆分函数，只要db的sharding key 和 tb的 sharding key 一致(且不考虑拆分函数里面的数字)
                 * 转换为auto模式时，就只看dbPartition
                 * */
                Set<String> dbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
                Set<String> tbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
                drdsDbPartitionBy.getArguments().forEach(
                    arg -> {
                        if (arg instanceof SQLIdentifierExpr) {
                            dbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                        } else if (arg instanceof SQLIntegerExpr) {
                            //dbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                        }
                    }
                );
                drdsTbPartitionBy.getArguments().forEach(
                    arg -> {
                        if (arg instanceof SQLIdentifierExpr) {
                            tbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                        } else if (arg instanceof SQLIntegerExpr) {
                            //tbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                        }
                    }
                );

                boolean hasSameShardingKey = dbShardingKey.equals(tbShardingKey);
                if (hasSameShardingKey) {
                    //handle gsi
                    handleAutoModeGsi(autoModeCreateTableStatement, maxPartitionsNum, maxPartitionColumnNum,
                        columnsLengthsInBytes, ec);

                    SQLPartitionBy autoPartitionBy =
                        convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy,
                            min(drdsDbPartitionNum * drdsTbPartitionNum, maxPartitionsNum),
                            primaryKey,
                            maxPartitionColumnNum, columnsLengthsInBytes);

                    /**
                     * dbpartition by range_hash(c1,c2,n) ==> partition by co_hash(substr(c1,-n), substr(c2,-n)),
                     * so ignore creating gsi for c2
                     */
//                    if (drdsDbPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
//                        MySqlTableIndex cgsiOnCol2 =
//                            generateCgsiForRangeHash2ndCol(drdsDbPartitionBy, drdsPartitionNum, primaryKey,
//                                maxPartitionColumnNum, columnsLengthsInBytes);
//                        autoModeCreateTableStatement.getTableElementList().add(cgsiOnCol2);
//                    }
                    autoModeCreateTableStatement.setPartitioning(autoPartitionBy);
                } else {
                    //convert origin gsi
                    handleAutoModeGsi(autoModeCreateTableStatement, maxPartitionsNum, maxPartitionColumnNum,
                        columnsLengthsInBytes, ec);

                    //handle dbPartitionBy
                    SQLPartitionBy autoPartitionBy =
                        convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy, drdsDbPartitionNum, primaryKey,
                            maxPartitionColumnNum, columnsLengthsInBytes);
                    autoModeCreateTableStatement.setPartitioning(autoPartitionBy);

                    //handle tbPartitionBy
                    SQLSubPartitionBy autoSubPartitionBy =
                        convertDrdsTbPartitionByToAutoSQLPartitionBy(drdsTbPartitionBy, drdsTbPartitionNum, primaryKey,
                            maxPartitionColumnNum, columnsLengthsInBytes);
                    autoModeCreateTableStatement.getPartitioning().setSubPartitionBy(autoSubPartitionBy);

                    /**
                     * dbpartition by range_hash(c1,c2,n) ==> partition by co_hash(substr(c1,-n), substr(c2,-n)),
                     * so ignore creating gis for c2
                     */
//                    //add cgsi for dbpartitionBy range hash 2nd col
//                    if (drdsDbPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
//                        MySqlTableIndex cgsiOnCol2 =
//                            generateCgsiForRangeHash2ndCol(drdsDbPartitionBy, drdsPartitionNum, primaryKey,
//                                maxPartitionColumnNum, columnsLengthsInBytes);
//                        autoModeCreateTableStatement.getTableElementList().add(cgsiOnCol2);
//                    }

                    //handle tbPartitionBy
                    /**
                     * 1. create gsi on tbpartition sharding key
                     * 2. add auto mode partitionBy on gsi (it's generated by tbpartition)
                     * */

                    /**
                     * 由于添加GSI会影响写性能，因此修改逻辑为：tbpartition by直接被忽略，不做处理
                     * */
//                    SQLIdentifierExpr tbShardingKey =
//                        (SQLIdentifierExpr) drdsTbPartitionBy.getArguments().get(0).clone();
//                    MySqlTableIndex gsiOnTbShardingKey = generateGsi(tbShardingKey);
//                    SQLPartitionBy autoPartitionByOnGsi =
//                        convertDrdsPartitionByToAutoSQLPartitionBy(drdsTbPartitionBy, drdsPartitionNum, primaryKey, maxPartitionColumnNum);
//                    gsiOnTbShardingKey.setPartitioning(autoPartitionByOnGsi);
//                    autoModeCreateTableStatement.getTableElementList().add(gsiOnTbShardingKey);

                    /**
                     * dbpartition by range_hash(c1,c2,n) ==> partition by co_hash(substr(c1,-n), substr(c2,-n)),
                     * so ignore creating gis for c2
                     */
//                    //add cgsi for tbpartitionBy range hash 2nd col
//                    if (drdsTbPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
//                        MySqlTableIndex cgsiOnCol2 =
//                            generateCgsiForRangeHash2ndCol(drdsTbPartitionBy, drdsPartitionNum, primaryKey, maxPartitionColumnNum);
//                        autoModeCreateTableStatement.getTableElementList().add(cgsiOnCol2);
//                    }
                }
            }
            return autoModeCreateTableStatement;
        }
    }

    private static boolean checkDuplicatedIndexName(MySqlCreateTableStatement createTableStatement,
                                                    String newIndexname) {
        List<SQLTableElement> elementList = createTableStatement.getTableElementList();
        Set<String> indexNames = new TreeSet<>(String::compareToIgnoreCase);
        for (int i = 0; i < elementList.size(); i++) {
            SQLTableElement element = elementList.get(i);
            if (element instanceof MySqlTableIndex) {
                MySqlTableIndex gsi = (MySqlTableIndex) element;
                indexNames.add(SQLUtils.normalize(gsi.getName().getSimpleName()));
            } else if (element instanceof MySqlUnique) {
                MySqlUnique uniqueIndex = (MySqlUnique) element;
                indexNames.add(SQLUtils.normalize(uniqueIndex.getName().getSimpleName()));
            }
        }

        return indexNames.contains(SQLUtils.normalize(newIndexname));
    }

    private static void eliminateDbPartitionAndTbPartition(MySqlCreateTableStatement statement) {
        statement.setDbPartitionBy(null);
        statement.setDbPartitions(null);
        statement.setTablePartitionBy(null);
        statement.setTablePartitions(null);
    }

    /**
     * convert all kinds of sequence to default sequence(for auto mode, it's new sequence)
     */
    private static void handleDrdsModeSequence(MySqlCreateTableStatement statement) {
        List<SQLTableElement> elementList = statement.getTableElementList();
        for (int i = 0; i < elementList.size(); i++) {
            SQLTableElement element = elementList.get(i);
            if (element instanceof SQLColumnDefinition) {
                SQLColumnDefinition colDef = (SQLColumnDefinition) element;
                if (colDef.isAutoIncrement()) {
                    colDef.setUnitCount(null);
                    colDef.setUnitIndex(null);
                    colDef.setSequenceType(null);
                }
            }
        }
    }

    private static void eliminateImplicitKeyAndAutoShardKey(MySqlCreateTableStatement drdsCreateTableStatement) {
        List<SQLTableElement> tableElementList = drdsCreateTableStatement.getTableElementList();
        Iterator<SQLTableElement> iterator = tableElementList.iterator();
        List<String> dbPartitionCols = new ArrayList<>(), tbPartitionCols = new ArrayList<>();
        SQLMethodInvokeExpr dbPartitionBy = (SQLMethodInvokeExpr) drdsCreateTableStatement.getDbPartitionBy();
        SQLMethodInvokeExpr tbPartitionBy = (SQLMethodInvokeExpr) drdsCreateTableStatement.getTablePartitionBy();

        if (dbPartitionBy != null) {
            for (SQLExpr expr : dbPartitionBy.getArguments()) {
                if (expr instanceof SQLIdentifierExpr) {
                    dbPartitionCols.add(((SQLIdentifierExpr) expr).getSimpleName());
                }
            }
        }
        if (tbPartitionBy != null) {
            for (SQLExpr expr : tbPartitionBy.getArguments()) {
                if (expr instanceof SQLIdentifierExpr) {
                    tbPartitionCols.add(((SQLIdentifierExpr) expr).getSimpleName());
                }
            }
        }
        while (iterator.hasNext()) {
            SQLTableElement element = iterator.next();
            if (element instanceof SQLColumnDefinition) {
                //eliminate implicit column
                SQLColumnDefinition colDef = (SQLColumnDefinition) element;
                if (colDef.getNameAsString().toLowerCase().contains("_drds_implicit_id_")) {
                    iterator.remove();
                }
            } else if (element instanceof MySqlPrimaryKey) {
                //eliminate implicit primary key
                MySqlPrimaryKey primaryKey = (MySqlPrimaryKey) element;
                SQLIdentifierExpr keyName =
                    (SQLIdentifierExpr) primaryKey.getIndexDefinition().getColumns().get(0).getExpr();
                if ("PRIMARY".equalsIgnoreCase(primaryKey.getIndexDefinition().getType())
                    && keyName.getName().toLowerCase().contains("_drds_implicit_id_")) {
                    iterator.remove();
                }
            } else if (element instanceof MySqlUnique) {
                //eliminate auto_shared_key for sharding key
                MySqlUnique mySqlUnique = (MySqlUnique) element;
                if (!mySqlUnique.isLocal()) {
                    continue;
                }
                SQLIdentifierExpr keyName = (SQLIdentifierExpr) mySqlUnique.getIndexDefinition().getName();
                if (keyName.getSimpleName().toLowerCase().contains(AUTO_SHARD_KEY_PREFIX.toLowerCase())) {
                    //检查：如果该auto_shard_key和分区类型不一致，则说明其承担普通索引的作用，不应该被remove
                    List<String> autoShardKeyCols = new ArrayList<>();
                    SQLIndexDefinition sqlIndexDefinition = mySqlUnique.getIndexDefinition();
                    for (SQLSelectOrderByItem item : sqlIndexDefinition.getColumns()) {
                        if (item.getExpr() instanceof SQLIdentifierExpr) {
                            autoShardKeyCols.add(((SQLIdentifierExpr) item.getExpr()).getSimpleName());
                        }
                    }

                    if (columnsIsSame(dbPartitionCols, autoShardKeyCols) || columnsIsSame(tbPartitionCols,
                        autoShardKeyCols)) {
                        iterator.remove();
                    }
                }

            } else if (element instanceof MySqlKey) {
                //eliminate auto_shared_key for sharding key
                MySqlKey mySqlKey = (MySqlKey) element;
                SQLIdentifierExpr keyName = (SQLIdentifierExpr) mySqlKey.getIndexDefinition().getName();
                if (keyName.getSimpleName().toLowerCase().contains(AUTO_SHARD_KEY_PREFIX.toLowerCase())) {
                    //检查：如果该auto_shard_key和分区类型不一致，则说明其承担普通索引的作用，不应该被remove
                    List<String> autoShardKeyCols = new ArrayList<>();
                    SQLIndexDefinition sqlIndexDefinition = mySqlKey.getIndexDefinition();
                    for (SQLSelectOrderByItem item : sqlIndexDefinition.getColumns()) {
                        if (item.getExpr() instanceof SQLIdentifierExpr) {
                            autoShardKeyCols.add(((SQLIdentifierExpr) item.getExpr()).getSimpleName());
                        }
                    }

                    if (columnsIsSame(dbPartitionCols, autoShardKeyCols) || columnsIsSame(tbPartitionCols,
                        autoShardKeyCols)) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    private static boolean columnsIsSame(final List<String> a, final List<String> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (a.get(i) == null || !a.get(i).equalsIgnoreCase(b.get(i))) {
                return false;
            }
        }
        return true;
    }

    public static SQLPartitionBy convertDrdsDbPartitionByToAutoSQLPartitionBy(SQLMethodInvokeExpr drdsPartitionBy,
                                                                              int partitionNum, List<String> primaryKey,
                                                                              int maxPartitionColumnNum,
                                                                              Map<String, Integer> columnsLengthsInBytes) {
        SQLPartitionBy autoSqlPartitionBy = null;
        /**
         * 对于映射成key分区的各种partition by，在partition by key的拆分键中附加主键，如果将来需要热点分裂，可以使用
         * 但由于auto模式的partition columns个数存在上限:
         *    a) 因此如果在partition by key的拆分键中附加主键导致超出partition columns个数的上限，
         *       则只会附加部分主键列
         *    b) 如果在partition by key的拆分键中附加主键导致自动生成的auto_shard_key_name名字超出mysql最长限制(64)，则也只会附加部分主键列
         *    c) 如果在partiition by key的拆分键中附加主键，导致auto_sharding_key这个联合索引长度超过mysql限制，则也只会附加部分主键列
         *       (使用columnsLengthsInBytes来获取列的长度，并计算是否超出限制)
         * */
        if (drdsPartitionBy.getMethodName().equalsIgnoreCase("hash")) {
            final SQLPartitionBy partitionByKey = new MySqlPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            partitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        partitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlPartitionBy = partitionByKey;
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("str_hash")) {
            final SQLPartitionBy partitionByKey = new MySqlPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            List<SQLExpr> strHashArguements = drdsPartitionBy.getArguments();
            if (strHashArguements.isEmpty() || !(strHashArguements.size() == 1 || strHashArguements.size() == 3
                || strHashArguements.size() == 4 || strHashArguements.size() == 5)) {
                return null;
            }
            SQLExpr newArg = strHashArguements.get(0).clone();
            partitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        partitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlPartitionBy = partitionByKey;
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("uni_hash")) {
            final SQLPartitionBy partitionByKey = new MySqlPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            partitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        partitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlPartitionBy = partitionByKey;
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("right_shift")) {
            final SQLPartitionBy partitionByKey = new MySqlPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            partitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));

            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        partitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlPartitionBy = partitionByKey;
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
            final SQLPartitionBy partitionByCoHash = new SQLPartitionByCoHash();
            List<SQLExpr> strHashArguements = drdsPartitionBy.getArguments();
            if (strHashArguements.size() != 3) {
                return null;
            }
            SQLIdentifierExpr col1 = (SQLIdentifierExpr) strHashArguements.get(0);
            SQLIdentifierExpr col2 = (SQLIdentifierExpr) strHashArguements.get(1);
            SQLIntegerExpr suffixLen = (SQLIntegerExpr) strHashArguements.get(2);

            SQLMethodInvokeExpr rightExpr1 = new SQLMethodInvokeExpr("RIGHT");
            SQLExpr newArgCol1 = col1.clone();
            SQLExpr substrPosiExpr1 = suffixLen.clone();
            rightExpr1.addArgument(newArgCol1);
            rightExpr1.addArgument(substrPosiExpr1);

            SQLMethodInvokeExpr rightExpr2 = new SQLMethodInvokeExpr("RIGHT");
            SQLExpr newArgCol2 = col2.clone();
            SQLExpr substrPosiExpr2 = suffixLen.clone();
            rightExpr2.addArgument(newArgCol2);
            rightExpr2.addArgument(substrPosiExpr2);

            partitionByCoHash.addColumn(rightExpr1);
            partitionByCoHash.addColumn(rightExpr2);
            partitionByCoHash.setPartitionsCount(partitionNum);

            autoSqlPartitionBy = partitionByCoHash;

        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYMM") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYMM_OPT")) {
            //convert to hash(to_months(`col`))
            autoSqlPartitionBy = new SQLPartitionByHash();
            SQLMethodInvokeExpr toMonths = new SQLMethodInvokeExpr("TO_MONTHS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toMonths.addArgument(col);
            autoSqlPartitionBy.addColumn(toMonths);
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYWEEK") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYWEEK_OPT")) {
            autoSqlPartitionBy = new SQLPartitionByHash();
            SQLMethodInvokeExpr toWeeks = new SQLMethodInvokeExpr("TO_WEEKS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toWeeks.addArgument(col);
            autoSqlPartitionBy.addColumn(toWeeks);
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYDD") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYDD_OPT")) {
            autoSqlPartitionBy = new SQLPartitionByHash();
            SQLMethodInvokeExpr toDays = new SQLMethodInvokeExpr("TO_DAYS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toDays.addArgument(col);
            autoSqlPartitionBy.addColumn(toDays);
            autoSqlPartitionBy.setPartitionsCount(partitionNum);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("MM")) {
            //build month(col)
            autoSqlPartitionBy = new SQLPartitionByRange();
            SQLMethodInvokeExpr month = new SQLMethodInvokeExpr("MONTH");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            month.addArgument(col);
            autoSqlPartitionBy.addColumn(month);
            //build partition definition
            generateRangePartitionDefInAutoMode((SQLPartitionByRange) autoSqlPartitionBy, 12, 12);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("DD")) {
            //build dayofmonth(`col`)
            autoSqlPartitionBy = new SQLPartitionByRange();
            SQLMethodInvokeExpr dayOfMonth = new SQLMethodInvokeExpr("DAYOFMONTH");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfMonth.addArgument(col);
            autoSqlPartitionBy.addColumn(dayOfMonth);

            //build partition definition
            generateRangePartitionDefInAutoMode((SQLPartitionByRange) autoSqlPartitionBy, 31, 31);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("WEEK")) {
            //build dayofweek(`col`)
            autoSqlPartitionBy = new SQLPartitionByRange();
            SQLMethodInvokeExpr dayOfWeek = new SQLMethodInvokeExpr("DAYOFWEEK");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfWeek.addArgument(col);
            autoSqlPartitionBy.addColumn(dayOfWeek);
            //build partition definition
            generateRangePartitionDefInAutoMode((SQLPartitionByRange) autoSqlPartitionBy, 7, 7);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("MMDD")) {
            //build dayofyear(`col`)
            autoSqlPartitionBy = new SQLPartitionByRange();
            SQLMethodInvokeExpr dayOfYear = new SQLMethodInvokeExpr("DAYOFYEAR");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfYear.addArgument(col);
            autoSqlPartitionBy.addColumn(dayOfYear);
            //build partition definition
            generateRangePartitionDefInAutoMode((SQLPartitionByRange) autoSqlPartitionBy, 366, 366);
        }
        return autoSqlPartitionBy;
    }

    public static SQLSubPartitionBy convertDrdsTbPartitionByToAutoSQLPartitionBy(SQLMethodInvokeExpr drdsPartitionBy,
                                                                                 int partitionNum,
                                                                                 List<String> primaryKey,
                                                                                 int maxPartitionColumnNum,
                                                                                 Map<String, Integer> columnsLengthsInBytes) {
        SQLSubPartitionBy autoSqlSubPartitionBy = null;
        /**
         * 对于映射成key分区的各种partition by，在partition by key的拆分键中附加主键，如果将来需要热点分裂，可以使用
         * 但由于auto模式的partition columns个数存在上限:
         *    a) 因此如果在partition by key的拆分键中附加主键导致超出partition columns个数的上限，
         *       则只会附加部分主键列
         *    b) 如果在partition by key的拆分键中附加主键导致自动生成的auto_shard_key_name名字超出mysql最长限制(64)，则也只会附加部分主键列
         *    c) 如果在partiition by key的拆分键中附加主键，导致auto_sharding_key这个联合索引长度超过mysql限制，则也只会附加部分主键列
         *       (使用columnsLengthsInBytes来获取列的长度，并计算是否超出限制)
         * */
        if (drdsPartitionBy.getMethodName().equalsIgnoreCase("hash")) {
            final SQLSubPartitionBy subPartitionByKey = new MySqlSubPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            subPartitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        subPartitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlSubPartitionBy = subPartitionByKey;
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("str_hash")) {
            final SQLSubPartitionBy subPartitionByKey = new MySqlSubPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            List<SQLExpr> strHashArguements = drdsPartitionBy.getArguments();
            if (strHashArguements.isEmpty() || !(strHashArguements.size() == 1 || strHashArguements.size() == 3
                || strHashArguements.size() == 4 || strHashArguements.size() == 5)) {
                return null;
            }
            SQLExpr newArg = strHashArguements.get(0).clone();
            subPartitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        subPartitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlSubPartitionBy = subPartitionByKey;
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("uni_hash")) {
            final SQLSubPartitionBy subPartitionByKey = new MySqlSubPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            subPartitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));
            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        subPartitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlSubPartitionBy = subPartitionByKey;
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("right_shift")) {
            final SQLSubPartitionBy subPartitionByKey = new MySqlSubPartitionByKey();
            Set<String> hashKey = new TreeSet<>(String::compareToIgnoreCase);
            SQLExpr newArg = drdsPartitionBy.getArguments().get(0).clone();
            subPartitionByKey.addColumn(newArg);
            hashKey.add(SQLUtils.normalize(((SQLIdentifierExpr) newArg).getSimpleName()));

            primaryKey.forEach(
                pk -> {
                    String autoShardKeyName =
                        AUTO_SHARD_KEY_PREFIX + hashKey.stream().collect(Collectors.joining("_")) + "_" + pk;
                    if (!hashKey.contains(SQLUtils.normalize(pk))
                        && hashKey.size() + 1 <= maxPartitionColumnNum
                        && autoShardKeyName.length() <= MAX_LENGTH_OF_IDENTIFIER_NAME
                        && validateAutoShardKeyLength(columnsLengthsInBytes, hashKey, pk)) {
                        SQLExpr pkCol = new SQLIdentifierExpr(SQLUtils.encloseWithUnquote(pk));
                        subPartitionByKey.addColumn(pkCol);
                        hashKey.add(SQLUtils.normalize(pk));
                    }
                }
            );
            autoSqlSubPartitionBy = subPartitionByKey;
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("range_hash")) {
            final SQLSubPartitionBy subPartitionByCoHash = new SQLSubPartitionByCoHash();
            List<SQLExpr> strHashArguements = drdsPartitionBy.getArguments();
            if (strHashArguements.size() != 3) {
                return null;
            }
            SQLIdentifierExpr col1 = (SQLIdentifierExpr) strHashArguements.get(0);
            SQLIdentifierExpr col2 = (SQLIdentifierExpr) strHashArguements.get(1);
            SQLIntegerExpr suffixLen = (SQLIntegerExpr) strHashArguements.get(2);

            SQLMethodInvokeExpr rightExpr1 = new SQLMethodInvokeExpr("RIGHT");
            SQLExpr newArgCol1 = col1.clone();
            SQLExpr substrPosiExpr1 = suffixLen.clone();
            rightExpr1.addArgument(newArgCol1);
            rightExpr1.addArgument(substrPosiExpr1);

            SQLMethodInvokeExpr rightExpr2 = new SQLMethodInvokeExpr("RIGHT");
            SQLExpr newArgCol2 = col2.clone();
            SQLExpr substrPosiExpr2 = suffixLen.clone();
            rightExpr2.addArgument(newArgCol2);
            rightExpr2.addArgument(substrPosiExpr2);

            subPartitionByCoHash.addColumn(rightExpr1);
            subPartitionByCoHash.addColumn(rightExpr2);
            subPartitionByCoHash.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));

            autoSqlSubPartitionBy = subPartitionByCoHash;

        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYMM") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYMM_OPT")) {
            //convert to hash(to_months(`col`))
            autoSqlSubPartitionBy = new SQLSubPartitionByHash();
            SQLMethodInvokeExpr toMonths = new SQLMethodInvokeExpr("TO_MONTHS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toMonths.addArgument(col);
            autoSqlSubPartitionBy.addColumn(toMonths);
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYWEEK") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYWEEK_OPT")) {
            autoSqlSubPartitionBy = new SQLSubPartitionByHash();
            SQLMethodInvokeExpr toWeeks = new SQLMethodInvokeExpr("TO_WEEKS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toWeeks.addArgument(col);
            autoSqlSubPartitionBy.addColumn(toWeeks);
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("YYYYDD") || drdsPartitionBy.getMethodName()
            .equalsIgnoreCase("YYYYDD_OPT")) {
            autoSqlSubPartitionBy = new SQLSubPartitionByHash();
            SQLMethodInvokeExpr toDays = new SQLMethodInvokeExpr("TO_DAYS");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            toDays.addArgument(col);
            autoSqlSubPartitionBy.addColumn(toDays);
            autoSqlSubPartitionBy.setSubPartitionsCount(new SQLIntegerExpr(partitionNum));
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("MM")) {
            //build month(col)
            autoSqlSubPartitionBy = new SQLSubPartitionByRange();
            SQLMethodInvokeExpr month = new SQLMethodInvokeExpr("MONTH");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            month.addArgument(col);
            autoSqlSubPartitionBy.addColumn(month);
            //build subpartition definition
            generateRangeSubPartitionDefInAutoMode((SQLSubPartitionByRange) autoSqlSubPartitionBy, 12, 12);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("DD")) {
            //build dayofmonth(`col`)
            autoSqlSubPartitionBy = new SQLSubPartitionByRange();
            SQLMethodInvokeExpr dayOfMonth = new SQLMethodInvokeExpr("DAYOFMONTH");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfMonth.addArgument(col);
            autoSqlSubPartitionBy.addColumn(dayOfMonth);

            //build subpartition definition
            generateRangeSubPartitionDefInAutoMode((SQLSubPartitionByRange) autoSqlSubPartitionBy, 31, 31);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("WEEK")) {
            //build dayofweek(`col`)
            autoSqlSubPartitionBy = new SQLSubPartitionByRange();
            SQLMethodInvokeExpr dayOfWeek = new SQLMethodInvokeExpr("DAYOFWEEK");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfWeek.addArgument(col);
            autoSqlSubPartitionBy.addColumn(dayOfWeek);
            //build subpartition definition
            generateRangeSubPartitionDefInAutoMode((SQLSubPartitionByRange) autoSqlSubPartitionBy, 7, 7);
        } else if (drdsPartitionBy.getMethodName().equalsIgnoreCase("MMDD")) {
            //build dayofyear(`col`)
            autoSqlSubPartitionBy = new SQLSubPartitionByRange();
            SQLMethodInvokeExpr dayOfYear = new SQLMethodInvokeExpr("DAYOFYEAR");
            SQLExpr col = drdsPartitionBy.getArguments().get(0).clone();
            dayOfYear.addArgument(col);
            autoSqlSubPartitionBy.addColumn(dayOfYear);
            //build subpartition definition
            generateRangeSubPartitionDefInAutoMode((SQLSubPartitionByRange) autoSqlSubPartitionBy, 366, 366);
        }
        return autoSqlSubPartitionBy;
    }

    static private void handleAutoModeGsi(MySqlCreateTableStatement autoCreateTableStatement, int maxPartitionsNum,
                                          int maxPartitionColumnNum, Map<String, Integer> columnsLengthsInBytes,
                                          ExecutionContext ec) {
        List<SQLTableElement> autoTableElementList = autoCreateTableStatement.getTableElementList();
        for (int i = 0; i < autoTableElementList.size(); i++) {
            SQLTableElement element = autoTableElementList.get(i);
            if (element instanceof MySqlTableIndex) {
                MySqlTableIndex gsi = (MySqlTableIndex) element;
                if (gsi.isGlobal() || gsi.isClustered()) {
                    SQLTableElement newGsi =
                        convertDrdsGsiToAutoGsi((MySqlTableIndex) element, maxPartitionsNum, maxPartitionColumnNum,
                            columnsLengthsInBytes, ec);
                    autoTableElementList.set(i, newGsi);
                }
            } else if (element instanceof MySqlUnique) {
                MySqlUnique uniqueIndex = (MySqlUnique) element;
                if (uniqueIndex.isGlobal() || uniqueIndex.isClustered()) {
                    SQLTableElement newGsi =
                        convertDrdsUgsiToAutoGsi((MySqlUnique) element, maxPartitionsNum, maxPartitionColumnNum,
                            columnsLengthsInBytes, ec);
                    autoTableElementList.set(i, newGsi);
                }
            }
        }
    }

    //if existingHashKey lengths + newNeedAddedKey length doesn't exceed 767 bytes, this function return true.
    static private boolean validateAutoShardKeyLength(Map<String, Integer> columnsLengthsInBytes,
                                                      Set<String> existingHashKey, String newNeedAddedKey) {
        int length = 0;
        for (String existingKey : existingHashKey) {
            if (!columnsLengthsInBytes.containsKey(existingKey) || columnsLengthsInBytes.get(existingKey) == null) {
                return false;
            } else {
                length += columnsLengthsInBytes.get(existingKey);
            }
        }
        if (!columnsLengthsInBytes.containsKey(newNeedAddedKey) || columnsLengthsInBytes.get(newNeedAddedKey) == null) {
            return false;
        }

        length += columnsLengthsInBytes.get(newNeedAddedKey);
        if (length < largestIndexPrefixLengthInBytes) {
            return true;
        } else {
            return false;
        }
    }

    static private MySqlTableIndex generateGsi(SQLIdentifierExpr col) {
        MySqlTableIndex gsi = new MySqlTableIndex();
        gsi.setGlobal(true);
        gsi.setName("`auto_g_" + SQLUtils.normalize(col.getName()) + "`");

        SQLSelectOrderByItem sqlSelectOrderByItem = new SQLSelectOrderByItem();
        sqlSelectOrderByItem.setExpr(col.clone());
        gsi.addColumn(sqlSelectOrderByItem);

        return gsi;
    }

    static private MySqlTableIndex generateCgsi(SQLIdentifierExpr col, SQLPartitionBy cgsiPartitionBy) {
        MySqlTableIndex cgsi = new MySqlTableIndex();
        cgsi.setLocal(false);
        cgsi.setClustered(true);
        cgsi.setName("`auto_cg_" + SQLUtils.normalize(col.getName()) + "`");

        SQLSelectOrderByItem sqlSelectOrderByItem = new SQLSelectOrderByItem();
        sqlSelectOrderByItem.setExpr(col.clone());
        cgsi.addColumn(sqlSelectOrderByItem);

        cgsi.setPartitioning(cgsiPartitionBy);

        return cgsi;
    }

    static private MySqlTableIndex generateCgsiForRangeHash2ndCol(SQLMethodInvokeExpr drdsPartitionBy, int partitionNum,
                                                                  List<String> primaryKey, int maxPartitionColumnNum,
                                                                  Map<String, Integer> columnsLengthsInBytes) {
        List<SQLExpr> strHashArguements = drdsPartitionBy.getArguments();
        if (strHashArguements.size() != 3) {
            return null;
        }
        SQLMethodInvokeExpr drdsPartitionByCopy = drdsPartitionBy.clone();
        Collections.swap(drdsPartitionByCopy.getArguments(), 0, 1);
        SQLPartitionBy partitionByWithSubStrForCol2 =
            convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsPartitionByCopy, partitionNum, primaryKey,
                maxPartitionColumnNum, columnsLengthsInBytes);

        SQLIdentifierExpr newCol = (SQLIdentifierExpr) drdsPartitionByCopy.getArguments().get(0).clone();
        return generateCgsi(newCol, partitionByWithSubStrForCol2);
    }

    static private MySqlTableIndex convertDrdsGsiToAutoGsi(MySqlTableIndex drdsGsi,
                                                           int maxPartitionsNum,
                                                           int maxPartitionColumnNum,
                                                           Map<String, Integer> columnsLengthsInBytes,
                                                           ExecutionContext ec) {
        MySqlTableIndex autoGsi = drdsGsi.clone();
        autoGsi.setDbPartitionBy(null);
        autoGsi.setTablePartitionBy(null);
        autoGsi.setTablePartitions(null);

        SQLMethodInvokeExpr drdsDbPartitionBy = (SQLMethodInvokeExpr) drdsGsi.getDbPartitionBy();
        //SQLIntegerExpr drdsDbPartiions = (SQLIntegerExpr) drdsGsi.getDbPartitions();
        SQLMethodInvokeExpr drdsTbPartitionBy =
            (SQLMethodInvokeExpr) drdsGsi.getTablePartitionBy();
        final int tbPartitionNum =
            (drdsGsi.getTablePartitions() == null) ? DrdsDefaultPartitionNumUtil.getDrdsDefaultTbPartitionNum() :
                ((SQLIntegerExpr) drdsGsi.getTablePartitions()).getNumber().intValue();
        final int drdsDbPartitionNum = DrdsDefaultPartitionNumUtil.getDrdsDefaultDbPartitionNum(ec);
        final int drdsTbPartitionNum = min(tbPartitionNum, maxPartitionsNum / drdsDbPartitionNum);

        //only dbPartition by
        if (drdsDbPartitionBy != null && drdsTbPartitionBy == null) {
            SQLPartitionBy autoPartitionBy =
                convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy, drdsDbPartitionNum, new ArrayList<>(),
                    maxPartitionColumnNum, columnsLengthsInBytes);
            autoGsi.setPartitioning(autoPartitionBy);
            return autoGsi;
        } else if (drdsDbPartitionBy != null && drdsTbPartitionBy != null) {
            //contain dbpartition and tbpartition
            /**
             * 这里不区db和tb的分拆分函数，只要db的sharding key 和 tb的 sharding key 一致(且不考虑拆分函数里面的数字)
             * 转换为auto模式时，就只看dbPartition
             * */
            Set<String> dbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
            Set<String> tbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
            drdsDbPartitionBy.getArguments().forEach(
                arg -> {
                    if (arg instanceof SQLIdentifierExpr) {
                        dbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                    } else if (arg instanceof SQLIntegerExpr) {
                        //dbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                    }
                }
            );
            drdsTbPartitionBy.getArguments().forEach(
                arg -> {
                    if (arg instanceof SQLIdentifierExpr) {
                        tbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                    } else if (arg instanceof SQLIntegerExpr) {
                        //tbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                    }
                }
            );

            boolean hasSameShardingKey = dbShardingKey.equals(tbShardingKey);
            if (hasSameShardingKey) {
                //only use dbPartition key
                SQLPartitionBy autoPartitionBy =
                    convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy,
                        min(drdsDbPartitionNum * drdsTbPartitionNum, maxPartitionsNum),
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);
                autoGsi.setPartitioning(autoPartitionBy);
                return autoGsi;
            } else {
                //dbPartition by and tbPartition
                SQLPartitionBy autoPartitionBy =
                    convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy, drdsDbPartitionNum,
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);
                SQLSubPartitionBy autoSubPartitionBy =
                    convertDrdsTbPartitionByToAutoSQLPartitionBy(drdsTbPartitionBy, drdsTbPartitionNum,
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);

                autoPartitionBy.setSubPartitionBy(autoSubPartitionBy);
                autoGsi.setPartitioning(autoPartitionBy);
                return autoGsi;
            }
        } else {
            //only tbPartition, impossible
            return null;
        }
    }

    static private MySqlUnique convertDrdsUgsiToAutoGsi(MySqlUnique drdsGsi, int maxPartitionsNum,
                                                        int maxPartitionColumnNum,
                                                        Map<String, Integer> columnsLengthsInBytes,
                                                        ExecutionContext ec) {
        MySqlUnique autoGsi = drdsGsi.clone();
        autoGsi.setDbPartitionBy(null);
        autoGsi.setTablePartitionBy(null);
        autoGsi.setTablePartitions(null);

        SQLMethodInvokeExpr drdsDbPartitionBy = (SQLMethodInvokeExpr) drdsGsi.getDbPartitionBy();
        SQLMethodInvokeExpr drdsTbPartitionBy =
            (SQLMethodInvokeExpr) drdsGsi.getTablePartitionBy();
        final int tbPartitionNum =
            (drdsGsi.getTablePartitions() == null) ? DrdsDefaultPartitionNumUtil.getDrdsDefaultTbPartitionNum() :
                ((SQLIntegerExpr) drdsGsi.getTablePartitions()).getNumber().intValue();
        final int drdsDbPartitionNum = DrdsDefaultPartitionNumUtil.getDrdsDefaultDbPartitionNum(ec);
        final int drdsTbPartitionNum = min(tbPartitionNum, maxPartitionsNum / drdsDbPartitionNum);

        //only dbPartition by
        if (drdsDbPartitionBy != null && drdsTbPartitionBy == null) {
            SQLPartitionBy autoPartitionBy =
                convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy, drdsDbPartitionNum, new ArrayList<>(),
                    maxPartitionColumnNum, columnsLengthsInBytes);
            autoGsi.setPartitioning(autoPartitionBy);
            return autoGsi;
        } else if (drdsDbPartitionBy != null && drdsTbPartitionBy != null) {
            //contain dbpartition and tbpartition
            /**
             * 这里不区db和tb的分拆分函数，只要db的sharding key 和 tb的 sharding key 一致(且不考虑拆分函数里面的数字)
             * 转换为auto模式时，就只看dbPartition
             * */
            Set<String> dbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
            Set<String> tbShardingKey = new TreeSet<>(String::compareToIgnoreCase);
            drdsDbPartitionBy.getArguments().forEach(
                arg -> {
                    if (arg instanceof SQLIdentifierExpr) {
                        dbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                    } else if (arg instanceof SQLIntegerExpr) {
                        //dbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                    }
                }
            );
            drdsTbPartitionBy.getArguments().forEach(
                arg -> {
                    if (arg instanceof SQLIdentifierExpr) {
                        tbShardingKey.add(((SQLIdentifierExpr) arg).normalizedName().toLowerCase());
                    } else if (arg instanceof SQLIntegerExpr) {
                        //tbShardingKey.add(((SQLIntegerExpr) arg).getNumber().toString().toLowerCase());
                    }
                }
            );

            boolean hasSameShardingKey = dbShardingKey.equals(tbShardingKey);
            if (hasSameShardingKey) {
                //only use dbPartition key
                SQLPartitionBy autoPartitionBy =
                    convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy,
                        min(drdsDbPartitionNum * drdsTbPartitionNum, maxPartitionsNum),
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);
                autoGsi.setPartitioning(autoPartitionBy);
                return autoGsi;
            } else {
                //dbPartition by and tbPartition
                SQLPartitionBy autoPartitionBy =
                    convertDrdsDbPartitionByToAutoSQLPartitionBy(drdsDbPartitionBy, drdsDbPartitionNum,
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);
                SQLSubPartitionBy autoSubPartitionBy =
                    convertDrdsTbPartitionByToAutoSQLPartitionBy(drdsTbPartitionBy, drdsTbPartitionNum,
                        new ArrayList<>(),
                        maxPartitionColumnNum, columnsLengthsInBytes);

                autoPartitionBy.setSubPartitionBy(autoSubPartitionBy);
                autoGsi.setPartitioning(autoPartitionBy);
                return autoGsi;
            }
        } else {
            //only tbPartition, impossible
            return null;
        }
    }

    /**
     * generate uniform range partition interval
     */
    static private void generateRangePartitionDefInAutoMode(SQLPartitionByRange sqlPartitionByRange,
                                                            int needPartitionNum,
                                                            int maxPartitionBound) {
        /**
         * needPartitionNum should not exceed maxPartitionBound.
         * e.g.
         * in partition by range(month(`col`)), maxPartitionBound = 12
         * */
        needPartitionNum = min(needPartitionNum, maxPartitionBound);
        int partitionBoundStep = maxPartitionBound / needPartitionNum;
        int partitionBeginBound = (partitionBoundStep == 1 ? partitionBoundStep + 1 : partitionBoundStep);
        while (partitionBeginBound <= maxPartitionBound) {
            SQLPartition partition = new SQLPartition();
            SQLIdentifierExpr pName = new SQLIdentifierExpr("p" + String.valueOf(partitionBeginBound));
            partition.setName(pName);

            SQLPartitionValue sqlPartitionValue = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
            SQLIntegerExpr item = new SQLIntegerExpr(partitionBeginBound);
            sqlPartitionValue.addItem(item);
            partition.setValues(sqlPartitionValue);

            sqlPartitionByRange.addPartition(partition);
            partitionBeginBound += partitionBoundStep;
        }

        //build default(maxvalue) partition
        SQLPartition defaultPartition = new SQLPartition();
        SQLIdentifierExpr pName = new SQLIdentifierExpr("pd");
        defaultPartition.setName(pName);
        SQLPartitionValue sqlPartitionValue = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
        SQLIdentifierExpr item = new SQLIdentifierExpr("MAXVALUE");
        sqlPartitionValue.addItem(item);
        defaultPartition.setValues(sqlPartitionValue);
        sqlPartitionByRange.addPartition(defaultPartition);
    }

    /**
     * generate uniform range partition interval
     */
    static private void generateRangeSubPartitionDefInAutoMode(SQLSubPartitionByRange sqlSubPartitionByRange,
                                                               int needPartitionNum,
                                                               int maxPartitionBound) {
        /**
         * needPartitionNum should not exceed maxPartitionBound.
         * e.g.
         * in partition by range(month(`col`)), maxPartitionBound = 12
         * */
        needPartitionNum = min(needPartitionNum, maxPartitionBound);
        int partitionBoundStep = maxPartitionBound / needPartitionNum;
        int partitionBeginBound = (partitionBoundStep == 1 ? partitionBoundStep + 1 : partitionBoundStep);
        while (partitionBeginBound <= maxPartitionBound) {
            SQLSubPartition subPartition = new SQLSubPartition();
            SQLIdentifierExpr pName = new SQLIdentifierExpr("sp" + String.valueOf(partitionBeginBound));
            subPartition.setName(pName);

            SQLPartitionValue sqlPartitionValue = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
            SQLIntegerExpr item = new SQLIntegerExpr(partitionBeginBound);
            sqlPartitionValue.addItem(item);
            subPartition.setValues(sqlPartitionValue);

            sqlSubPartitionByRange.getSubPartitionTemplate().add(subPartition);
            partitionBeginBound += partitionBoundStep;
        }

        //build default(maxvalue) partition
        SQLSubPartition defaultSubPartition = new SQLSubPartition();
        SQLIdentifierExpr pName = new SQLIdentifierExpr("spd");
        defaultSubPartition.setName(pName);
        SQLPartitionValue sqlSubPartitionValue = new SQLPartitionValue(SQLPartitionValue.Operator.LessThan);
        SQLIdentifierExpr item = new SQLIdentifierExpr("MAXVALUE");
        sqlSubPartitionValue.addItem(item);
        defaultSubPartition.setValues(sqlSubPartitionValue);
        sqlSubPartitionByRange.getSubPartitionTemplate().add(defaultSubPartition);
    }

    /**
     * 尽可能求出每个column的最大bytes大小
     * 1. 对于char(20)和varchar(10)类型，括号内数字代表最大字符长度，具体所占字节数还需结合字符集确定
     * 2. 对于定长类型(如int、datetime)，直接求出其字节数
     * 3. 对于未知字符集的column，或blob等类型，直接不计算（返回的map中不包含该列，则表示该列的所占bytes暂无法求出）
     * <p>
     * 该函数仅用于：'是否将某主键添加到拆分列中' 提供参考。所以，可以容忍'部分列无法求出bytes长度'
     */
    protected static Map<String, Integer> tryToCaclColumnsMaxLenInBytes(List<SQLColumnDefinition> columnDefinitions,
                                                                        String tableDefaultCharset) {
        Map<String, Integer> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (SQLColumnDefinition colDef : columnDefinitions) {
            String colName = SQLUtils.normalize(colDef.getColumnName());
            if (colName == null) {
                continue;
            }
            colName = colName.toLowerCase();

            if (colDef.getDataType() instanceof SQLCharacterDataType) {
                String charSetName = tableDefaultCharset;
                Integer argLen = null;
                SQLCharacterDataType dt = (SQLCharacterDataType) colDef.getDataType();
                if (colDef.getCharsetExpr() != null && colDef.getCharsetExpr() instanceof SQLIdentifierExpr) {
                    charSetName = ((SQLIdentifierExpr) colDef.getCharsetExpr()).getName();
                }

                if (dt != null && !dt.getArguments().isEmpty() && dt.getArguments().get(0) instanceof SQLIntegerExpr) {
                    SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) dt.getArguments().get(0);
                    argLen = sqlIntegerExpr.getNumber().intValue();
                    if (dt.jdbcType() == Types.CHAR || dt.jdbcType() == Types.VARCHAR) {
                        Map<String, Integer> charsetAndLengths = DrdsToAutoCharsetUtil.getAllCharsetAndLength();
                        if (argLen != null && charSetName != null) {
                            Integer bytes = charsetAndLengths.containsKey(charSetName.toLowerCase()) ?
                                charsetAndLengths.get(charSetName.toLowerCase()) :
                                DrdsToAutoCharsetUtil.getMaxCharsetLengthInBytes();
                            result.put(colName, bytes * argLen);
                        }
                    }
                }
            } else if (colDef.getDataType() instanceof SQLDataTypeImpl) {
                SQLDataTypeImpl dt = (SQLDataTypeImpl) colDef.getDataType();
                if (dt.jdbcType() == Types.TINYINT || dt.jdbcType() == Types.BOOLEAN) {
                    result.put(colName, 1);
                } else if (dt.jdbcType() == Types.SMALLINT) {
                    result.put(colName, 2);
                } else if (dt.jdbcType() == Types.INTEGER) {
                    result.put(colName, 4);
                } else if (dt.jdbcType() == Types.BIGINT) {
                    result.put(colName, 8);
                } else if (dt.jdbcType() == Types.FLOAT) {
                    result.put(colName, 8);
                } else if (dt.jdbcType() == Types.DOUBLE || dt.jdbcType() == Types.REAL) {
                    result.put(colName, 8);
                } else if (dt.jdbcType() == Types.DATE) {
                    result.put(colName, 3);
                } else if (dt.jdbcType() == Types.TIMESTAMP) {
                    result.put(colName, 4);
                } else if (dt.jdbcType() == Types.TIME) {
                    result.put(colName, 3);
                } else if ("datetime".equalsIgnoreCase(dt.getName())) {
                    result.put(colName, 4);
                } else if ("year".equalsIgnoreCase(dt.getName())) {
                    result.put(colName, 1);
                } else if ("MEDIUMINT".equalsIgnoreCase(dt.getName())) {
                    result.put(colName, 3);
                }
            }
        }

        return result;
    }

    public static List<String> getTableNamesFromDatabase(String schemaName, ExecutionContext executionContext) {
        SqlShowTables sqlShowTables =
            SqlShowTables.create(SqlParserPos.ZERO, true, null, schemaName, null, null, null, null);
        ExecutionContext copiedContext = executionContext.copy();
        copiedContext.setSchemaName(schemaName);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(copiedContext);
        ExecutionPlan showTablesPlan = Planner.getInstance().getPlan(sqlShowTables, plannerContext);
        LogicalShow logicalShowTables = (LogicalShow) showTablesPlan.getPlan();

        IRepository sourceRepo = ExecutorContext
            .getContext(schemaName)
            .getTopologyHandler()
            .getRepositoryHolder()
            .get(Group.GroupType.MYSQL_JDBC.toString());
        LogicalShowTablesMyHandler logicalShowTablesMyHandler = new LogicalShowTablesMyHandler(sourceRepo);

        Cursor showTablesCursor =
            logicalShowTablesMyHandler.handle(logicalShowTables, copiedContext);

        List<String> tables = new ArrayList<>();
        Row showTablesResult = null;
        while ((showTablesResult = showTablesCursor.next()) != null) {
            if (showTablesResult.getColNum() >= 1 && showTablesResult.getString(0) != null
                && TABLE_TYPE.equalsIgnoreCase(showTablesResult.getString(1))) {
                tables.add(showTablesResult.getString(0));
            }
        }
        return tables;
    }

}
