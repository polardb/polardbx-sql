/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlEngineAccessorDelegate;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Generate [INSERT INTO SELECT BINLOG] to [BLACK HOLE] when dropping partition of primary table.
 * Used for [COLUMNAR].
 */
@TaskName(name = "DropPrimaryTblPartitionCleanColumnarDataTask")
@Getter
public class DropPrimaryTblPartitionCleanColumnarDataTask extends BaseDdlTask {
    public static final String CREATE_SHADOW_TABLE =
        "/*+TDDL:cmd_extra(IGNORE_CCI_WHEN_CREATE_SHADOW_TABLE=true, CREATE_SHADOW_TABLE_ENGINE=BLACKHOLE)*/ CREATE TABLE `%s` like `%s`";
    public static final String DROP_SHADOW_TABLE = "DROP TABLE IF EXISTS `%s`";

    private final String tableName;
    private final String shadowTableName;
    private final List<String> partitionNames;
    private final Boolean isSubPartition;

    // 批大小
    private final Long batchSize;

    // 跟踪上一个批次的主键值（按顺序存储每个主键列的值）
    private List<Object> lastProcessedPK = new ArrayList<>();
    // 控制每批次插入的间隔时间（以毫秒为单位）
    private final Long batchInterval;

    // 每个物理分区对应的batch进度，Map<partName, lastProcessedPK>
    @Setter
    private volatile Map<String, List<Object>> partitionToLastPKMap = new HashMap<>();

    // 已完成清理的物理分区
    @Setter
    protected volatile Set<String> cleanedPhyPartSet = new HashSet<>();

    @JSONCreator
    public DropPrimaryTblPartitionCleanColumnarDataTask(String schemaName, String tableName,
                                                        List<String> partitionNames, boolean isSubPartition,
                                                        Long batchSize, Long batchInterval) {
        super(schemaName);
        this.tableName = tableName;
        this.shadowTableName = getBlackHoleTableName(tableName);
        this.partitionNames = partitionNames;
        this.isSubPartition = isSubPartition;
        this.batchSize = batchSize;
        this.batchInterval = batchInterval;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // 初始化已完成分区集合，初始化未完成分区中进行到哪个Batch
        initMarkSet(executionContext);

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        List<String> pkColumnNames =
            tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList());

        final AtomicLong batchRound = new AtomicLong(0);
        final IServerConfigManager serverConfigManager = getServerConfigManager();
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        // 设置为XA事务
        sessionVariables.put("transaction_policy", "xa");

        final AtomicInteger totalInsertRows = new AtomicInteger(0);
        final AtomicInteger insertRows = new AtomicInteger(0);
        final AtomicLong insertTcNano = new AtomicLong(0);

        List<String> allPartitionNames = getUnprocessedPartitions(); // 只处理未完成的分区

        final Object[] innerTransConn = new Object[1];

        for (String partitionName : allPartitionNames) {
            boolean finished = false;
            // 处理未完成的batch
            lastProcessedPK.clear();
            if (partitionToLastPKMap.containsKey(partitionName)) {
                lastProcessedPK = partitionToLastPKMap.get(partitionName);
            }
            while (true) {
                /**
                 * Check if task is interrupt
                 */
                if (checkTaskInterrupted(executionContext)) {
                    doClearWorkForInterruptTask(partitionName);
                    break;
                }

                /**
                 * Exec sql to insert select into shadow table(black hole).
                 */
                wrapWithDistributedTrx(
                    serverConfigManager,
                    schemaName,
                    sessionVariables,
                    (transConn) -> {
                        batchRound.incrementAndGet();
                        innerTransConn[0] = transConn;

                        /**
                         * <pre>
                         *     exec the sql:
                         *         insert into shadow_tbl
                         *         select * from prim_tbl partition(part)
                         *         where pk > lower_bound
                         *         order by pk
                         *         limit batch_cnt;
                         * </pre>
                         */
                        long insertBeginTs = System.nanoTime();
                        String insertSelectSql = generateInsertSelectSql(pkColumnNames, partitionName);
                        int rows = execLogicalDmlOnInnerConnection(serverConfigManager,
                            schemaName, transConn, executionContext, insertSelectSql);
                        insertRows.set(rows);
                        long insertEndTs = System.nanoTime();

                        insertTcNano.set(insertEndTs - insertBeginTs);

                        // 更新 lastProcessedPK，生成 lower_bound
                        if (rows > 0 && rows == batchSize) {
                            lastProcessedPK = getLastProcessedPK(pkColumnNames, partitionName, innerTransConn[0]);
                        }

                        return insertRows;
                    }
                );

                // 标记该分区的这个batch为已完成
                markPartitionLastPKMap(partitionName, lastProcessedPK);

                totalInsertRows.addAndGet(insertRows.get());
                if (insertRows.get() < batchSize) {
                    /**
                     * if insertRows is less than batchCnt, that means
                     * current batch is the last batch.
                     */
                    finished = true;
                    break;
                }

                if (insertRows.get() > 0) {
                    // 控制速率，暂停一段时间
                    try {
                        Thread.sleep(batchInterval);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            if (finished) {
                // 标记该分区为已完成
                markCleanedPhyPartSet(partitionName);
            }
        }
    }

    /**
     * Useless for black hole
     */
    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // 初始化已完成分区集合，初始化未完成分区中进行到哪个Batch
        initMarkSet(executionContext);

        TableMeta tableMeta = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
        List<String> pkColumnNames =
            tableMeta.getPrimaryKey().stream().map(ColumnMeta::getName).collect(Collectors.toList());

        final AtomicLong batchRound = new AtomicLong(0);
        final IServerConfigManager serverConfigManager = getServerConfigManager();
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        // 设置为XA事务
        sessionVariables.put("transaction_policy", "xa");

        final AtomicInteger deleteRows = new AtomicInteger(0);
        final Object[] innerTransConn = new Object[1];

        List<String> allPartitionNames = getUnprocessedPartitions(); // 只处理未完成的分区

        for (String partitionName : allPartitionNames) {
            boolean finished = false;
            // 处理未完成的batch
            lastProcessedPK.clear();
            if (partitionToLastPKMap.containsKey(partitionName)) {
                lastProcessedPK = partitionToLastPKMap.get(partitionName);
            }
            while (true) {
                /**
                 * Check if task is interrupt
                 */
                if (checkTaskInterrupted(executionContext)) {
                    doClearWorkForInterruptTask(partitionName);
                    break;
                }

                /**
                 * Rollback sql to delete from shadow table(black hole is useless).
                 */
                wrapWithDistributedTrx(
                    serverConfigManager,
                    schemaName,
                    sessionVariables,
                    (transConn) -> {
                        batchRound.incrementAndGet();
                        innerTransConn[0] = transConn;

                        /**
                         * <pre>
                         *     exec the sql:
                         *         delete from shadow_tbl partition(part)
                         *         where pk > lower_bound
                         *         order by pk
                         *         limit batch_cnt;
                         * </pre>
                         */
                        String deleteSql = generateDeleteSql(pkColumnNames, partitionName);
                        int rows = execLogicalDmlOnInnerConnection(serverConfigManager,
                            schemaName, transConn, executionContext, deleteSql);
                        deleteRows.set(rows);

                        // 更新 lastProcessedPK，生成 lower_bound
                        if (rows > 0 && rows == batchSize) {
                            lastProcessedPK = getLastProcessedPK(pkColumnNames, partitionName, innerTransConn[0]);
                        }

                        return deleteRows;
                    }
                );

                // 标记该分区的这个batch为已完成
                markPartitionLastPKMap(partitionName, lastProcessedPK);

                if (deleteRows.get() < batchSize) {
                    /**
                     * if deleteRows is less than batchCnt, that means
                     * current batch is the last batch.
                     */
                    finished = true;
                    break;
                }

                if (deleteRows.get() > 0) {
                    // 控制速率，暂停一段时间
                    try {
                        Thread.sleep(batchInterval);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            if (finished) {
                // 标记该分区为已完成
                markCleanedPhyPartSet(partitionName);
            }
        }
    }

    public static String getBlackHoleTableName(final String tableName) {
        return "__$_" + tableName;
    }

    protected boolean checkTaskInterrupted(ExecutionContext executionContext) {
        if (Thread.currentThread().isInterrupted() || executionContext.getDdlContext().isInterrupted()) {
            return true;
        }
        return false;
    }

    protected void doClearWorkForInterruptTask(String phyPartName) {
        /**
         * Just do nothing
         */
        String msg = String.format("insert into shadow tbl job has been interrupted, phypart is %s, table is %s.%s",
            phyPartName, schemaName, tableName);

        /**
         * Throw a exception to notify ddl-engine this task is not finished and restart at next time
         */
        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, msg);
    }

    protected void initMarkSet(ExecutionContext ec) {
        if (this.cleanedPhyPartSet == null || this.cleanedPhyPartSet.isEmpty()) {
            storeTaskRecord();
        }
    }

    protected void storeTaskRecord() {
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                synchronized (DropPrimaryTblPartitionCleanColumnarDataTask.this) {
                    // 更新任务状态为 DIRTY，以便下次恢复时能读取最新状态
                    setState(DdlTaskState.DIRTY);
                    DdlEngineTaskRecord taskRecord =
                        TaskHelper.toDdlEngineTaskRecord(DropPrimaryTblPartitionCleanColumnarDataTask.this);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            }
        };
        delegate.execute();
    }

    protected void markCleanedPhyPartSet(String partitionName) {
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                synchronized (DropPrimaryTblPartitionCleanColumnarDataTask.this) {
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(getJobId(), getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);
                    DropPrimaryTblPartitionCleanColumnarDataTask newTask =
                        (DropPrimaryTblPartitionCleanColumnarDataTask) TaskHelper.fromDdlEngineTaskRecord(currTaskRec);

                    // 获取最新的 cleanedPhyPartSet
                    Set<String> newestCleanedPhyPartSet = newTask.getCleanedPhyPartSet();

                    // 标记该分区为已完成
                    newestCleanedPhyPartSet.add(partitionName);

                    // 更新状态并写回数据库
                    DropPrimaryTblPartitionCleanColumnarDataTask.this.setCleanedPhyPartSet(newestCleanedPhyPartSet);
                    DropPrimaryTblPartitionCleanColumnarDataTask.this.setState(DdlTaskState.DIRTY);

                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            }
        };
        delegate.execute();
    }

    protected void markPartitionLastPKMap(String partitionName, List<Object> lastPK) {
        DdlEngineAccessorDelegate delegate = new DdlEngineAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                synchronized (DropPrimaryTblPartitionCleanColumnarDataTask.this) {
                    List<DdlEngineTaskRecord> taskRecords =
                        engineTaskAccessor.queryTasksForUpdate(getJobId(), getName());

                    if (taskRecords.isEmpty()) {
                        return 0;
                    }

                    DdlEngineTaskRecord currTaskRec = taskRecords.get(0);
                    DropPrimaryTblPartitionCleanColumnarDataTask newTask =
                        (DropPrimaryTblPartitionCleanColumnarDataTask) TaskHelper.fromDdlEngineTaskRecord(currTaskRec);

                    // 获取最新的 partitionLastPKMap
                    Map<String, List<Object>> newesPartitionLastPKMap = newTask.getPartitionToLastPKMap();

                    // 标记该分区为已完成
                    newesPartitionLastPKMap.put(partitionName, new ArrayList<>(lastPK));

                    // 更新状态并写回数据库
                    DropPrimaryTblPartitionCleanColumnarDataTask.this.setPartitionToLastPKMap(newesPartitionLastPKMap);
                    DropPrimaryTblPartitionCleanColumnarDataTask.this.setState(DdlTaskState.DIRTY);

                    DdlEngineTaskRecord taskRecord = TaskHelper.toDdlEngineTaskRecord(newTask);
                    return engineTaskAccessor.updateTask(taskRecord);
                }
            }
        };
        delegate.execute();
    }

    private List<String> getUnprocessedPartitions() {
        List<String> unprocessed = new ArrayList<>();
        for (String partitionName : getAllPartitionsNames()) {
            if (!cleanedPhyPartSet.contains(partitionName)) {
                unprocessed.add(partitionName);
            }
        }
        return unprocessed;
    }

    /**
     * 生成动态的 INSERT SELECT SQL 语句
     *
     * @return 插入语句字符串
     */
    private String generateInsertSelectSql(List<String> pkColumnNames, String partitionName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("INSERT INTO %s ", shadowTableName));

        sqlBuilder.append(String.format("SELECT * FROM %s PARTITION (%s) ", tableName, partitionName));

        if (!lastProcessedPK.isEmpty()) {
            // 根据复合主键生成 WHERE 子句
            sqlBuilder.append("WHERE ");
            sqlBuilder.append(
                replacePlaceholdersWithParameters(generateCompositeWhereClause(pkColumnNames), lastProcessedPK));
            sqlBuilder.append(" ");
        }

        // 按主键排序
        sqlBuilder.append("ORDER BY ");
        sqlBuilder.append(String.join(", ", pkColumnNames));
        sqlBuilder.append(" ");

        // 限制批量大小
        sqlBuilder.append(String.format("LIMIT %d", batchSize));

        return sqlBuilder.toString();
    }

    /**
     * 生成动态的 DELETE SQL 语句
     *
     * @return 插入语句字符串
     */
    private String generateDeleteSql(List<String> pkColumnNames, String partitionName) {
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append(String.format("DELETE FROM %s PARTITION (%s) ", shadowTableName, partitionName));

        if (!lastProcessedPK.isEmpty()) {
            // 根据复合主键生成 WHERE 子句
            sqlBuilder.append("WHERE ");
            sqlBuilder.append(
                replacePlaceholdersWithParameters(generateCompositeWhereClause(pkColumnNames), lastProcessedPK));
            sqlBuilder.append(" ");
        }

        // 按主键排序
        sqlBuilder.append("ORDER BY ");
        sqlBuilder.append(String.join(", ", pkColumnNames));
        sqlBuilder.append(" ");

        // 限制批量大小
        sqlBuilder.append(String.format("LIMIT %d", batchSize));

        return sqlBuilder.toString();
    }

    /**
     * 生成复合主键的参数化 WHERE 子句 (pk1, pk2, ..., pkn) > (?, ?, ..., ?)
     *
     * @return WHERE 子句字符串
     */
    private String generateCompositeWhereClause(List<String> pkColumnNames) {
        return String.format("(%s) > (%s)",
            String.join(", ", pkColumnNames),
            String.join(", ", Collections.nCopies(pkColumnNames.size(), "?"))
        );
    }

    /**
     * 将SQL中的?占位符替换为实际的参数值
     *
     * @param sql 包含?占位符的SQL字符串
     * @param parameters 参数值列表
     * @return 替换后的SQL字符串
     */
    private String replacePlaceholdersWithParameters(String sql, List<Object> parameters) {
        StringBuilder finalSqlBuilder = new StringBuilder();
        int paramIndex = 0;
        int length = sql.length();

        for (int i = 0; i < length; i++) {
            char currentChar = sql.charAt(i);
            if (currentChar == '?' && paramIndex < parameters.size()) {
                Object param = parameters.get(paramIndex++);
                String formattedParam = formatParameter(param);
                finalSqlBuilder.append(formattedParam);
            } else {
                finalSqlBuilder.append(currentChar);
            }
        }

        return finalSqlBuilder.toString();
    }

    /**
     * 根据参数类型格式化参数值
     *
     * @param param 参数值
     * @return 格式化后的参数字符串
     */
    private String formatParameter(Object param) {
        if (param == null) {
            return "NULL";
        } else if (param instanceof String || param instanceof java.util.Date
            || param instanceof java.time.temporal.Temporal) {
            String value = param.toString().replace("'", "''");
            return "'" + value + "'";
        } else {
            return param.toString();
        }
    }

    /**
     * 获取所有分区
     */
    private List<String> getAllPartitionsNames() {
        List<String> partitions = new ArrayList<>();
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(schemaName).getPartitionInfoManager().getPartitionInfo(tableName);
        if (isSubPartition) {
            boolean isTempPart = partitionInfo.getPartitionBy().getSubPartitionBy().isUseSubPartTemplate();
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (PartitionSpec subPartSpec : partitionSpec.getSubPartitions()) {
                    for (String part : partitionNames) {
                        if (isTempPart && subPartSpec.getTemplateName().equalsIgnoreCase(part) ||
                            !isTempPart && subPartSpec.getName().equalsIgnoreCase(part)) {
                            partitions.add(subPartSpec.getName());
                        }
                    }
                }
            }
        } else {
            for (PartitionSpec partitionSpec : partitionInfo.getPartitionBy().getPartitions()) {
                for (String part : partitionNames) {
                    if (partitionSpec.getName().equalsIgnoreCase(part)) {
                        partitions.add(partitionSpec.getName());
                    }
                }
            }
        }
        return partitions;
    }

    /**
     * 获取当前批次插入的最后一个主键值
     * <pre>
     * SELECT pk1, pk2, ..., pkn
     * FROM (
     *     SELECT pk1, pk2, ..., pkn
     *     FROM tbl partition (part_name)
     *     WHERE (pk1, pk2, ..., pkn) > (lower_val1, lower_val2, ..., lower_valn)
     *     ORDER BY pk1, pk2, ..., pkn
     *     LIMIT batch
     * ) AS batch_data
     * ORDER BY pk1 DESC, pk2 DESC, ..., pkn DESC
     * LIMIT 1;
     * </pre>
     *
     * @return 当前批次的最后一个主键值列表
     */

    private List<Object> getLastProcessedPK(List<String> pkColumnNames, String partitionName, Object transConn) {
        StringBuilder innerSqlBuilder = new StringBuilder();
        innerSqlBuilder.append(String.format(
            "SELECT %s FROM %s PARTITION (%s) ",
            String.join(", ", pkColumnNames),
            tableName, partitionName
        ));

        if (!lastProcessedPK.isEmpty()) {
            innerSqlBuilder.append("WHERE ");
            innerSqlBuilder.append(generateCompositeWhereClause(pkColumnNames));
            innerSqlBuilder.append(" ");
        }

        // 正序排序取出 batch 数据
        innerSqlBuilder.append("ORDER BY ");
        innerSqlBuilder.append(String.join(", ", pkColumnNames));
        innerSqlBuilder.append(" LIMIT ");
        innerSqlBuilder.append(batchSize);

        // 外层包装：倒序取最大 PK 组合
        StringBuilder maxPkSqlBuilder = new StringBuilder();
        maxPkSqlBuilder.append("SELECT ");
        maxPkSqlBuilder.append(String.join(", ", pkColumnNames));
        maxPkSqlBuilder.append(" FROM (");
        maxPkSqlBuilder.append(innerSqlBuilder.toString());
        maxPkSqlBuilder.append(") AS batch_data ");
        maxPkSqlBuilder.append("ORDER BY ");

        // 拼接 DESC 排序字段
        List<String> descOrderColumns = pkColumnNames.stream()
            .map(col -> col + " DESC")
            .collect(Collectors.toList());
        maxPkSqlBuilder.append(String.join(", ", descOrderColumns));

        maxPkSqlBuilder.append(" LIMIT 1");

        String maxPkSql = maxPkSqlBuilder.toString();

        List<Object> newLastProcessedPK = new ArrayList<>();

        try (PreparedStatement stmt = ((Connection) transConn).prepareStatement(maxPkSql)) {
            // 设置参数
            if (!lastProcessedPK.isEmpty()) {
                int paramIndex = 1;
                for (int i = 0; i < pkColumnNames.size(); i++) {
                    for (int j = 0; j <= i; j++) {
                        stmt.setObject(paramIndex++, lastProcessedPK.get(j));
                    }
                }
            }

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    for (String pkColumnName : pkColumnNames) {
                        newLastProcessedPK.add(rs.getObject(pkColumnName));
                    }
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        }

        return newLastProcessedPK;
    }

    /**
     * Exec dml
     */
    private int execLogicalDmlOnInnerConnection(IServerConfigManager serverMgr,
                                                String schemaName,
                                                Object transConn,
                                                ExecutionContext ec,
                                                String sql) {
        return serverMgr.executeBackgroundDmlByTransConnection(sql, schemaName, null, transConn);
    }

    private IServerConfigManager getServerConfigManager() {
        return GsiUtils.getServerConfigManager();
    }

    private <R> R wrapWithDistributedTrx(IServerConfigManager serverMgr,
                                         String schemaName,
                                         Map<String, Object> sessionVariables,
                                         Function<Object, R> caller) {

        R result = null;
        Object transConn = null;
        try {
            transConn = serverMgr.getTransConnection(schemaName, sessionVariables);
            serverMgr.transConnectionBegin(transConn);
            result = caller.apply(transConn);
            serverMgr.transConnectionCommit(transConn);
        } catch (SQLException ex) {
            if (transConn != null) {
                try {
                    serverMgr.transConnectionRollback(transConn);
                } catch (Throwable err) {
                    // ignore
                }
            }
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex);
        } finally {
            if (null != transConn) {
                try {
                    serverMgr.closeTransConnection(transConn);
                } catch (Throwable ex) {
                    // ignore
                }
            }
        }
        return result;
    }
}
