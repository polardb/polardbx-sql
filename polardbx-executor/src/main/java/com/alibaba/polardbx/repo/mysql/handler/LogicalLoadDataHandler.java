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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.CrcAccumulator;
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.orc.OrcBloomFilter;
import com.alibaba.polardbx.common.oss.OSSMetaLifeCycle;
import com.alibaba.polardbx.common.oss.access.OSSKey;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillTimer;
import com.alibaba.polardbx.executor.archive.writer.OSSBackFillWriterTask;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.ddl.job.meta.FileStorageBackFillAccessor;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.newengine.meta.SchemaEvolutionAccessorDelegate;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.gsi.utils.Transformer;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.sync.TablesMetaChangeForOssSyncAction;
import com.alibaba.polardbx.executor.sync.TablesMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.engine.FileSystemUtils;
import com.alibaba.polardbx.gms.metadb.evolution.ColumnMappingRecord;
import com.alibaba.polardbx.gms.metadb.seq.SequenceBaseRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetaAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnMetasRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.OrcMetaUtils;
import com.alibaba.polardbx.optimizer.config.table.PolarDBXOrcSchema;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.context.LoadDataContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.In;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.string.Ord;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert.HandlerParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.SimpleShardProcessor;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.OssLoadDataRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastSqlParserException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sequence.ISequenceManager;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.aliyun.oss.OSS;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.Data;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.impl.RecordReaderImpl;
import org.apache.orc.impl.WriterImpl;
import org.jetbrains.annotations.Blocking;

import javax.validation.constraints.Null;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.constants.SequenceAttribute.AUTO_SEQ_PREFIX;
import static com.alibaba.polardbx.optimizer.context.LoadDataContext.END;
import static java.lang.Math.min;

public class LogicalLoadDataHandler extends LogicalInsertHandler {

    public String DEFAULT_GROUP = "DEFAULT_GROUP";

    public PropUtil.LOAD_NULL_MODE null_mode;

    private final Object UPLOAD_END = new Object();
    private final Object FLUSH_END = new Object();

    public LogicalLoadDataHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LoadDataContext loadDataContext = executionContext.getLoadDataContext();
        String nullMode = loadDataContext == null ?
            executionContext.getParamManager().getString(ConnectionParams.LOAD_DATA_HANDLE_EMPTY_CHAR) :
            loadDataContext.getParamManager().getString(ConnectionParams.LOAD_DATA_HANDLE_EMPTY_CHAR);
        this.null_mode = PropUtil.LOAD_NULL_MODE.valueOf(nullMode);
        HandlerParams handlerParams = new HandlerParams();

        LogicalInsert logicalInsert = (LogicalInsert) logicalPlan;
        checkInsertLimitation(logicalInsert, executionContext);
        String schemaName = logicalInsert.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        if (!OptimizerContext.getContext(schemaName).getPartitionInfoManager()
            .isNewPartDbTable(logicalInsert.getLogicalTableName())) {
            buildSimpleShard(logicalInsert, executionContext.getLoadDataContext());
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);

        int affectRows;
        // check if load data to OSS engine
        String logicalTableName = logicalInsert.getLogicalTableName();
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
        if (tableMeta.getEngine() == Engine.OSS) {
            affectRows = executeOssLoadData(logicalInsert, executionContext, tableMeta);
            return new AffectRowCursor(affectRows);
        }

        long oldLastInsertId = executionContext.getConnection().getLastInsertId();
        try {
            affectRows = executeInsert(logicalInsert, executionContext, handlerParams);

        } catch (Throwable e) {
            // If exception happens, reset last insert id.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
            throw GeneralUtil.nestedException(e);
        }

        // If it's a single table, only MyJdbcHandler knows last insert id, and
        // it writes the value into Connection.
        // If it's a sharded table, correct last insert id is in LogicalInsert,
        // so overwrite the value MyJdbcHandler wrote.
        if (handlerParams.returnedLastInsertId != 0) {
            executionContext.getConnection().setReturnedLastInsertId(handlerParams.returnedLastInsertId);
        }
        if (handlerParams.lastInsertId != 0) {
            // Using sequence, override the value set by MyJdbcHandler
            executionContext.getConnection().setLastInsertId(handlerParams.lastInsertId);
        } else if (handlerParams.usingSequence) {
            // Using sequence, but all auto increment column values are
            // specified.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
        } else {
            // Not using sequence. Use the value set by MyJdbcHandler.
        }
        return new AffectRowCursor(affectRows);
    }

    private void buildSimpleShard(LogicalInsert logicalInsert, LoadDataContext loadDataContext) {
        ShardProcessor processor = ShardProcessor.buildSimpleShard(logicalInsert);
        if (processor instanceof SimpleShardProcessor) {
            loadDataContext.setShardProcessor((SimpleShardProcessor) processor);
        }
    }

    protected int executeOssLoadData(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                     TableMeta tableMeta) {
        int affectRows = 0;
        LoadDataContext loadDataContext = executionContext.getLoadDataContext();

        // set use batch
        String schemaName = executionContext.getSchemaName();
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean inSingleDb = or.isTableInSingleDb(logicalInsert.getLogicalTableName());
        boolean useBatchMode =
            executionContext.getParamManager().getBoolean(ConnectionParams.LOAD_DATA_USE_BATCH_MODE);
        if (inSingleDb) {
            useBatchMode = false;
        }
        loadDataContext.setUseBatch(useBatchMode);

        int producerNum = executionContext.getParamManager().getInt(ConnectionParams.OSS_LOAD_DATA_PRODUCERS);
        int maxConsumerNum = executionContext.getParamManager().getInt(ConnectionParams.OSS_LOAD_DATA_MAX_CONSUMERS);
        int flushNum = executionContext.getParamManager().getInt(ConnectionParams.OSS_LOAD_DATA_FLUSHERS);
        int uploadNum = executionContext.getParamManager().getInt(ConnectionParams.OSS_LOAD_DATA_UPLOADERS);

        int physicalTableNum = tableMeta.getPartitionInfo().getAllPhysicalPartitionCount();
        Map<String, List<PhysicalPartitionInfo>> physicalPartitionInfos =
            tableMeta.getPartitionInfo().getPhysicalPartitionTopology(new ArrayList<>());

        int consumerNum = Math.min(maxConsumerNum, physicalTableNum);
        flushNum = Math.max(consumerNum, flushNum);

        List<OssLoadDataProducer> producers = new ArrayList<>();
        Map<Integer, OssLoadDataConsumer> consumers = new HashMap<>();
        List<OssLoadDataFlusher> flushers = new ArrayList<>();
        List<OssLoadDataUploader> uploaders = new ArrayList<>();

        Map<String, Integer> consumerMap = new HashMap<>();
        Map<Integer, BlockingQueue<Object>> consumeQueues = new HashMap<>();
        Map<Integer, BlockingQueue<Object>> flushQueues = new HashMap<>();
        BlockingQueue<Object> uploadBlockingQueue = new LinkedBlockingQueue<>();

        // build consumers
        for (List<PhysicalPartitionInfo> infos : physicalPartitionInfos.values()) {
            for (PhysicalPartitionInfo info : infos) {
                int consumeId = info.getPartBitSetIdx() % consumerNum;
                consumerMap.put(info.getPhyTable(), consumeId);
                if (!consumeQueues.containsKey(consumeId)) {
                    BlockingQueue<Object> consumeBlockingQueue = new LinkedBlockingQueue<>();
                    consumeQueues.put(consumeId, consumeBlockingQueue);
                    int flushId = consumeId % flushNum;
                    if (!flushQueues.containsKey(flushId)) {
                        BlockingQueue<Object> flushBlockingQueue = new LinkedBlockingQueue<>();
                        flushQueues.put(flushId, flushBlockingQueue);
                    }
                    OssLoadDataConsumer ossLoadDataConsumer =
                        new OssLoadDataConsumer(loadDataContext, executionContext, consumeBlockingQueue, tableMeta,
                            uploadBlockingQueue, flushQueues.get(flushId), info);
                    consumers.put(consumeId, ossLoadDataConsumer);
                } else {
                    consumers.get(consumeId).addPhyTableContext(info);
                }
            }
        }

        // build producers and run
        for (int i = 0; i < producerNum; i++) {
            OssLoadDataProducer ossLoadDataProducer =
                new OssLoadDataProducer(loadDataContext, executionContext, logicalInsert,
                    consumeQueues, consumerMap);
            ossLoadDataProducer.doProduce();
            producers.add(ossLoadDataProducer);
        }

        // run consumers
        for (OssLoadDataConsumer consumer : consumers.values()) {
            consumer.doConsume();
        }

        // run flushers
        for (int i = 0; i < flushNum && flushQueues.containsKey(i); i++) {
            OssLoadDataFlusher ossLoadDataFlusher = new OssLoadDataFlusher(executionContext, flushQueues.get(i));
            ossLoadDataFlusher.doFlush();
            flushers.add(ossLoadDataFlusher);
        }

        // build uploaders and run
        for (int i = 0; i < uploadNum; i++) {
            OssLoadDataUploader ossLoadDataUploader =
                new OssLoadDataUploader(loadDataContext, executionContext, uploadBlockingQueue, tableMeta);
            ossLoadDataUploader.doUpload();
            uploaders.add(ossLoadDataUploader);
        }

        try {
            // wait produce finish
            for (OssLoadDataProducer producer : producers) {
                producer.produceDoneFuture.get();
            }

            for (OssLoadDataConsumer consumer : consumers.values()) {
                consumer.ossLoadDataQueueAddEND();
            }

            // wait consume finish
            for (OssLoadDataConsumer consumer : consumers.values()) {
                affectRows += consumer.consumeDoneFuture.get();
            }

            for (BlockingQueue<Object> flushBlockingQueue : flushQueues.values()) {
                flushBlockingQueue.add(FLUSH_END);
            }

            // wait flush finish
            for (OssLoadDataFlusher flusher : flushers) {
                flusher.flushDoneFuture.get();
            }
            uploadBlockingQueue.add(UPLOAD_END);

            // wait flush finish
            for (OssLoadDataUploader uploader : uploaders) {
                uploader.uploadDoneFuture.get();
            }

            OssLoadDataPersistAndSync ossLoadDataPersistAndSync =
                new OssLoadDataPersistAndSync(executionContext, uploaders, tableMeta);
            // write to metaDb
            ossLoadDataPersistAndSync.writeMetaDbTransaction();

            // sync metadata
            ossLoadDataPersistAndSync.tableSync();
        } catch (Throwable t) {
            loadDataContext.finish(t);
            for (OssLoadDataConsumer consumer : consumers.values()) {
                consumer.ossLoadDataQueueClear();
                consumer.ossLoadDataQueueAddEND();
            }
            uploadBlockingQueue.add(UPLOAD_END);
            throw new TddlNestableRuntimeException(t);
        }

        return affectRows;
    }

    @Override
    protected int executeInsert(LogicalInsert logicalInsert, ExecutionContext executionContext,
                                HandlerParams handlerParams) {
        LoadDataContext loadDataContext = executionContext.getLoadDataContext();
        String schemaName = executionContext.getSchemaName();
        boolean hasIndex = GlobalIndexMeta.hasIndex(
            logicalInsert.getLogicalTableName(), schemaName, executionContext);
        boolean ignoreIsSimpleInsert = loadDataContext.getParamManager().getBoolean(
            ConnectionParams.LOAD_DATA_IGNORE_IS_SIMPLE_INSERT);
        final boolean gsiConcurrentWrite =
            loadDataContext.getParamManager().getBoolean(ConnectionParams.GSI_CONCURRENT_WRITE_OPTIMIZE);
        PhyTableOperationUtil.enableIntraGroupParallelism(schemaName, executionContext);
        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        final boolean isBroadcast = or.isBroadCast(logicalInsert.getLogicalTableName());
        final boolean inSingleDb = or.isTableInSingleDb(logicalInsert.getLogicalTableName());
        loadDataContext.setInSingleDb(inSingleDb);
        if (!isBroadcast &&
            (!hasIndex ||
                (gsiConcurrentWrite &&
                    logicalInsert.isSimpleInsert(ignoreIsSimpleInsert &&
                        null != logicalInsert.getPrimaryInsertWriter())))) {
            boolean useBatchMode =
                loadDataContext.getParamManager().getBoolean(ConnectionParams.LOAD_DATA_USE_BATCH_MODE);
            if (inSingleDb) {
                useBatchMode = false;
            }
            loadDataContext.setUseBatch(useBatchMode);
            Pair<List<ShardConsumer>, List<AdaptiveLoadDataCursor>> cursors = null;
            List<ListenableFuture<?>> waitFutures = new ArrayList<>();
            try {

                cursors = concurrentCursors(executionContext, logicalInsert);

                for (int i = 0; i < cursors.getKey().size(); i++) {
                    waitFutures.add(cursors.getKey().get(i).doInit());
                }

                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).doInit();
                }

                for (int i = 0; i < waitFutures.size(); i++) {
                    waitFutures.get(i).get();
                }

                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).getRelNodeBlockingQueue().add(END);
                }

                int totalAffectRows = ExecUtils.getAffectRowsByCursors(
                    Lists.newArrayList(cursors.getValue()), false);
                if (hasIndex) {
                    totalAffectRows /= GlobalIndexMeta.getGsiIndexNum(logicalInsert.getLogicalTableName(),
                        executionContext.getSchemaName(), executionContext) + 1;
                }
                loadDataContext.getLoadDataAffectRows().set(totalAffectRows);
                if (loadDataContext.getThrowable() != null) {
                    throw loadDataContext.getThrowable();
                }
                return totalAffectRows;
            } catch (Throwable t) {
                loadDataContext.finish(t);
                for (int i = 0; i < cursors.getValue().size(); i++) {
                    cursors.getValue().get(i).getRelNodeBlockingQueue().clear();
                    cursors.getValue().get(i).getRelNodeBlockingQueue().add(END);
                }
                throw new TddlNestableRuntimeException(t);
            }
        } else {
            try {
                int totalAffectRows = 0;
                while (true) {
                    List<String> lines = loadDataContext.getParameters().take();
                    if (lines == END) {
                        break;
                    }
                    List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        List<String> fields =
                            Lists.newArrayList(Splitter.on(loadDataContext.getFieldTerminatedBy()).split(line));
                        if (loadDataContext.getAutoFillColumnIndex() != -1) {
                            fields.add(loadDataContext.getAutoFillColumnIndex(),
                                loadDataContext.isInSingleDb() ? "NULL" :
                                    SequenceManagerProxy.getInstance().nextValue(executionContext.getSchemaName(),
                                        ISequenceManager.AUTO_SEQ_PREFIX + loadDataContext.getTableName()).toString());
                        }
                        Map<Integer, ParameterContext> parameterContexts = Transformer.buildColumnParam(
                            loadDataContext.getMetaList(), fields, loadDataContext.getCharset(), null_mode);
                        batchParams.add(parameterContexts);
                        long length = SizeOf.sizeOfCharArray(line.length());
                        loadDataContext.getDataCacheManager().releaseMemory(length);
                    }
                    if (batchParams.size() > 0) {
                        Parameters parameters = new Parameters();
                        parameters.setBatchParams(batchParams);
                        executionContext.setParams(parameters);
                        totalAffectRows += super.executeInsert(logicalInsert, executionContext, handlerParams);
                    }
                }
                if (hasIndex) {
                    totalAffectRows /= GlobalIndexMeta.getGsiIndexNum(logicalInsert.getLogicalTableName(),
                        executionContext.getSchemaName(), executionContext) + 1;
                }
                return totalAffectRows;
            } catch (Throwable t) {
                // Can't commit
                executionContext.getTransaction()
                    .setCrucialError(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_CONTINUE_AFTER_WRITE_FAIL, t.getMessage());
                throw GeneralUtil.nestedException(t);
            }
        }
    }

    public Pair<List<ShardConsumer>, List<AdaptiveLoadDataCursor>> concurrentCursors(
        ExecutionContext executionContext, LogicalInsert logicalInsert) {
        executionContext.getExtraCmds().put(ConnectionProperties.MPP_METRIC_LEVEL, 0);
        String schemaName = logicalInsert.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        boolean useTrans = executionContext.getTransaction() instanceof IDistributedTransaction;
        LoadDataContext loadDataContext = executionContext.getLoadDataContext();
        int realParallism = loadDataContext == null ?
            executionContext.getParamManager().getInt(ConnectionParams.PARALLELISM) :
            loadDataContext.getParamManager().getInt(ConnectionParams.PARALLELISM);

        final TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        boolean isSingleTable = or.isTableInSingleDb(logicalInsert.getLogicalTableName());
        final Set<String> dbNames;
        PartitionInfoManager partitionInfoManager = OptimizerContext.getContext(schemaName).getPartitionInfoManager();
        if (partitionInfoManager.isNewPartDbTable(logicalInsert.getLogicalTableName())) {
            PartitionInfo partitionInfo =
                partitionInfoManager.getPartitionInfo(logicalInsert.getLogicalTableName());
            dbNames = partitionInfo.getTopology().keySet();
        } else {
            dbNames = or.getTableRule(
                logicalInsert.getLogicalTableName()).getActualTopology().keySet();
        }

        List<String> groupNames = Lists.newArrayList(dbNames);
        List<String> groupConnIdSet = PhyTableOperationUtil.buildGroConnSetFromGroups(executionContext, groupNames);
        CursorMeta cursorMeta = CalciteUtils.buildDmlCursorMeta();

        if (realParallism <= 0) {
            if (isSingleTable) {
                realParallism = 1;
            } else {
                realParallism = groupConnIdSet.size();
            }
        } else {
            if (useTrans) {
                realParallism = min(groupConnIdSet.size(), realParallism);
            }
        }

        List<AdaptiveLoadDataCursor> indexCursors = new ArrayList<>();
        List<ShardConsumer> shardConsumers = new ArrayList<>();
        Map<String, BlockingQueue<Object>> multiQueus = new HashMap<>();
        if (useTrans) {
            List<BlockingQueue<Object>> blockingQueueList = new ArrayList<>();
            for (int i = 0; i < groupConnIdSet.size(); i++) {
                if (i < realParallism) {
                    BlockingQueue<Object> queue = new LinkedBlockingQueue<>();
                    AdaptiveLoadDataCursor cursor = new AdaptiveLoadDataCursor(
                        executionContext, queue, cursorMeta, multiQueus);
                    indexCursors.add(cursor);
                    multiQueus.put(groupConnIdSet.get(i), queue);
                    blockingQueueList.add(queue);
                } else {
                    int index = (i - realParallism) % realParallism;
                    multiQueus.put(groupConnIdSet.get(i), blockingQueueList.get(index));
                }
            }

            for (int i = 0; i < realParallism; i++) {
                ShardConsumer shardConsumer = new ShardConsumer(
                    executionContext.getLoadDataContext(), executionContext, logicalInsert, multiQueus);
                shardConsumers.add(shardConsumer);
            }
        } else {
            BlockingQueue<Object> relNodeBlockingQueue = new LinkedBlockingQueue<>();
            multiQueus.put(DEFAULT_GROUP, relNodeBlockingQueue);
            for (int i = 0; i < realParallism; i++) {
                ShardConsumer shardConsumer = new ShardConsumer(
                    executionContext.getLoadDataContext(), executionContext, logicalInsert, multiQueus);
                shardConsumers.add(shardConsumer);
                AdaptiveLoadDataCursor cursor = new AdaptiveLoadDataCursor(
                    executionContext, relNodeBlockingQueue, cursorMeta, multiQueus);
                indexCursors.add(cursor);
            }
        }
        return new Pair<>(shardConsumers, indexCursors);
    }

    public class AdaptiveLoadDataCursor extends AbstractCursor {
        private final Logger
            logger = LoggerFactory.getLogger(AdaptiveLoadDataCursor.class);

        private final List<Throwable> exceptionsWhenClose = new ArrayList<>();
        private final ExecutionContext executionContext;
        private final BlockingQueue<Object> relNodeBlockingQueue;
        private final CursorMeta cursorMeta;
        private AtomicInteger affectRows = new AtomicInteger(-1);
        private int affectNum = 0;
        private ListenableFuture<?> future;
        private volatile Cursor currentCusor;
        private LoadDataContext loadDataContext;
        private Map<String, BlockingQueue<Object>> concurrentQueues;

        public AdaptiveLoadDataCursor(
            ExecutionContext executionContext, BlockingQueue<Object> relNodeBlockingQueue, CursorMeta cursorMeta,
            Map<String, BlockingQueue<Object>> concurrentQueues) {
            super(false);
            this.executionContext = executionContext.copy();
            this.cursorMeta = cursorMeta;
            this.returnColumns = cursorMeta.getColumns();
            this.loadDataContext = executionContext.getLoadDataContext();
            this.relNodeBlockingQueue = relNodeBlockingQueue;
            this.concurrentQueues = concurrentQueues;
        }

        @Override
        public void doInit() {
            if (this.inited) {
                return;
            }
            super.doInit();

            final Map mdcContext = MDC.getCopyOfContextMap();
            this.future = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                () -> {
                    this.takePhyOpAndThenExec(mdcContext);
                    return null;
                }, executionContext.getRuntimeStatistics());
        }

        private void takePhyOpAndThenExec(Map mdcContext) {
            MDC.setContextMap(mdcContext);
            try {
                while (true) {
                    if (cursorClosed) {
                        break;
                    }

                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    Object object = relNodeBlockingQueue.take();
                    if (object == END) {
                        relNodeBlockingQueue.add(END);
                        break;
                    }

                    if (object instanceof Long) {
                        loadDataContext.getDataCacheManager().releaseMemory((Long) object);
                        continue;
                    }
                    currentCusor = ExecutorContext.getContext(
                        executionContext.getSchemaName()).getTopologyExecutor().execByExecPlanNode(
                        (PhyTableOperation) object, executionContext);
                    int num =
                        ExecUtils.getAffectRowsByCursor(currentCusor);
                    affectNum += num;
                    loadDataContext.getLoadDataAffectRows().addAndGet(num);
                }
            } catch (Throwable t) {
                exceptionsWhenClose.add(t);
                logger.error("AdaptiveParallelLoadDataCursor failed", t);
                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (concurrentQueues != null) {
                    concurrentQueues.values().stream().forEach(q -> {
                        q.clear();
                        q.add(END);
                    });
                }
            }
        }

        @Override
        public Row doNext() {
            try {
                future.get();
            } catch (Throwable e) {
                throw GeneralUtil.nestedException(e);
            }
            if (exceptionsWhenClose.size() > 0) {
                throw GeneralUtil.nestedException(exceptionsWhenClose.get(0));
            }
            if (affectRows.compareAndSet(-1, affectNum)) {
                ArrayRow arrayRow = new ArrayRow(1, cursorMeta);
                arrayRow.setObject(0, affectNum);
                arrayRow.setCursorMeta(cursorMeta);
                return arrayRow;
            } else {
                return null;
            }
        }

        @Override
        public List<Throwable> doClose(List<Throwable> exs) {
            this.isFinished = true;
            exs.addAll(exceptionsWhenClose);
            if (future != null) {
                future.cancel(true);
            }
            if (currentCusor != null) {
                currentCusor.close(exs);
            }
            return exs;
        }

        public BlockingQueue<Object> getRelNodeBlockingQueue() {
            return relNodeBlockingQueue;
        }
    }

    private String getGroupConnIdStr(PhyTableOperation phyOp, ExecutionContext ec) {
        return PhyTableOperationUtil.buildGroConnIdStr(phyOp, ec);
    }

    public class ShardConsumer {

        private final Logger
            logger = LoggerFactory.getLogger(ShardConsumer.class);

        private LoadDataContext loadDataContext;
        private ExecutionContext executionContext;
        private LogicalInsert logicalInsert;
        private Map<String, BlockingQueue<Object>> concurrentQueues;

        public ShardConsumer(LoadDataContext loadDataContext,
                             ExecutionContext executionContext, LogicalInsert logicalInsert,
                             Map<String, BlockingQueue<Object>> concurrentQueues) {
            this.loadDataContext = loadDataContext;
            this.executionContext = executionContext.copy();
            this.logicalInsert = logicalInsert;
            this.concurrentQueues = concurrentQueues;
        }

        public ListenableFuture<?> doInit() {

            final Map mdcContext = MDC.getCopyOfContextMap();
            return executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                () -> {
                    MDC.setContextMap(mdcContext);
                    return run();
                }, executionContext.getRuntimeStatistics());
        }

        public Object run() {
            try {
                while (true) {

                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    List<String> lines = loadDataContext.getParameters().take();
                    if (lines == END) {
                        loadDataContext.getParameters().add(END);
                        break;
                    }
                    List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
                    long totalMemory = 0L;
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        totalMemory += SizeOf.sizeOfCharArray(line.length());
                        List<String> fields =
                            Lists.newArrayList(Splitter.on(loadDataContext.getFieldTerminatedBy()).split(line));
                        if (loadDataContext.getAutoFillColumnIndex() != -1) {
                            fields.add(loadDataContext.getAutoFillColumnIndex(),
                                loadDataContext.isInSingleDb() ? "NULL" :
                                    SequenceManagerProxy.getInstance().nextValue(executionContext.getSchemaName(),
                                        ISequenceManager.AUTO_SEQ_PREFIX + loadDataContext.getTableName()).toString());
                        }
                        Map<Integer, ParameterContext> parameterContexts = Transformer.buildColumnParam(
                            loadDataContext.getMetaList(), fields, loadDataContext.getCharset(), null_mode);
                        batchParams.add(parameterContexts);
                    }
                    if (batchParams.size() > 0) {
                        Parameters parameters = new Parameters();
                        parameters.setBatchParams(batchParams);
                        executionContext.setParams(parameters);
                        List<RelNode> allPhyPlan = getAllRelNode(
                            logicalInsert, executionContext);
                        if (concurrentQueues.size() > 1) {
                            //use trans
                            long averageSize = totalMemory / allPhyPlan.size();
                            long div = totalMemory % allPhyPlan.size();
                            for (int i = 0; i < allPhyPlan.size(); i++) {
                                long calcSize = 0;
                                if (i == allPhyPlan.size() - 1) {
                                    calcSize = div + averageSize;
                                } else {
                                    calcSize = averageSize;
                                }
                                PhyTableOperation phyTableOperation = (PhyTableOperation) allPhyPlan.get(i);
                                BlockingQueue<Object> blockingQueue =
                                    concurrentQueues.get(getGroupConnIdStr(phyTableOperation, executionContext));
                                blockingQueue.add(phyTableOperation);
                                blockingQueue.add(calcSize);
                            }
                        } else {
                            BlockingQueue<Object> blockingQueue =
                                concurrentQueues.get(DEFAULT_GROUP);
                            blockingQueue.addAll(allPhyPlan);
                            blockingQueue.add(totalMemory);
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("ShardConsumer failed", t);

                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (concurrentQueues != null) {
                    concurrentQueues.values().stream().forEach(q -> {
                        q.clear();
                        q.add(END);
                    });
                }
            }
            return null;
        }

        private List<RelNode> getAllRelNode(
            LogicalInsert logicalInsert,
            ExecutionContext executionContext) {
            List<RelNode> allPhyPlan = new ArrayList<>();

            if (null != logicalInsert.getPrimaryInsertWriter()) {
                // Get plan for primary
                final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
                List<RelNode> inputs = primaryWriter.getInput(executionContext);

                allPhyPlan.addAll(inputs);

                executionContext.getLoadDataContext().setGsiInsertTurn(true);
                final List<InsertWriter> gsiWriters = logicalInsert.getGsiInsertWriters();
                gsiWriters.stream()
                    .map(gsiWriter -> gsiWriter.getInput(executionContext))
                    .filter(w -> !w.isEmpty())
                    .forEach(w -> {
                        allPhyPlan.addAll(w);
                    });
                executionContext.getLoadDataContext().setGsiInsertTurn(false);
            } else {

                List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
                PhyTableInsertSharder insertSharder = new PhyTableInsertSharder(logicalInsert,
                    executionContext.getParams(),
                    SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
                allPhyPlan.addAll(logicalInsert.getInput(insertSharder, shardResults, executionContext));
            }
            return allPhyPlan;
        }

    }

    public class OssLoadDataProducer {
        private final Logger
            logger = LoggerFactory.getLogger(OssLoadDataProducer.class);

        private LoadDataContext loadDataContext;
        private ExecutionContext executionContext;
        private LogicalInsert logicalInsert;
        private Map<Integer, BlockingQueue<Object>> consumeQueues;

        private Map<String, Integer> consumerMap;
        public ListenableFuture<?> produceDoneFuture;

        public OssLoadDataProducer(LoadDataContext loadDataContext,
                                   ExecutionContext executionContext, LogicalInsert logicalInsert,
                                   Map<Integer, BlockingQueue<Object>> consumeQueues,
                                   Map<String, Integer> consumerMap) {
            this.loadDataContext = loadDataContext;
            this.executionContext = executionContext.copy();
            this.logicalInsert = logicalInsert;
            this.consumeQueues = consumeQueues;
            this.consumerMap = consumerMap;
        }

        public void doProduce() {

            final Map mdcContext = MDC.getCopyOfContextMap();
            this.produceDoneFuture = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                () -> {
                    MDC.setContextMap(mdcContext);
                    return produce();
                }, executionContext.getRuntimeStatistics());
        }

        public Object produce() {
            try {
                while (true) {

                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    List<String> lines = loadDataContext.getParameters().take();
                    if (lines == END) {
                        loadDataContext.getParameters().add(END);
                        break;
                    }
                    List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
                    long totalMemory = 0L;
                    for (int i = 0; i < lines.size(); i++) {
                        String line = lines.get(i);
                        totalMemory += SizeOf.sizeOfCharArray(line.length());
                        List<String> fields =
                            Lists.newArrayList(Splitter.on(loadDataContext.getFieldTerminatedBy()).split(line));
                        if (loadDataContext.getAutoFillColumnIndex() != -1) {
                            fields.add(loadDataContext.getAutoFillColumnIndex(),
                                loadDataContext.isInSingleDb() ? "NULL" :
                                    SequenceManagerProxy.getInstance().nextValue(executionContext.getSchemaName(),
                                        ISequenceManager.AUTO_SEQ_PREFIX + loadDataContext.getTableName()).toString());
                        }
                        Map<Integer, ParameterContext> parameterContexts = Transformer.buildColumnParam(
                            loadDataContext.getMetaList(), fields, loadDataContext.getCharset(), null_mode);
                        batchParams.add(parameterContexts);
                    }
                    if (batchParams.size() > 0) {
                        Parameters parameters = new Parameters();
                        parameters.setBatchParams(batchParams);
                        executionContext.setParams(parameters);
                        List<RelNode> allPhyPlan = getAllRelNode(
                            logicalInsert, executionContext);

                        for (RelNode relNode : allPhyPlan) {
                            PhyTableOperation phyTableOperation = (PhyTableOperation) relNode;
                            int hashId = consumerMap.get(phyTableOperation.getTableNames().get(0).get(0));
                            if (phyTableOperation.getBatchParameters() != null) {
                                consumeQueues.get(hashId).add(relNode);
                            }
                        }
                        consumeQueues.get(0).add(totalMemory);
                    }
                }
            } catch (Throwable t) {
                logger.error("OssLoadDataProducer failed", t);

                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (consumeQueues != null) {
                    consumeQueues.values().stream().forEach(q -> {
                        q.clear();
                        q.add(END);
                    });
                }
            }
            return null;
        }

        private List<RelNode> getAllRelNode(
            LogicalInsert logicalInsert,
            ExecutionContext executionContext) {
            List<RelNode> allPhyPlan = new ArrayList<>();

            if (null != logicalInsert.getPrimaryInsertWriter()) {
                // Get plan for primary
                final InsertWriter primaryWriter = logicalInsert.getPrimaryInsertWriter();
                List<RelNode> inputs = primaryWriter.getInput(executionContext);

                allPhyPlan.addAll(inputs);
            } else {

                List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>();
                PhyTableInsertSharder insertSharder = new PhyTableInsertSharder(logicalInsert,
                    executionContext.getParams(),
                    SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
                allPhyPlan.addAll(logicalInsert.getInput(insertSharder, shardResults, executionContext));
            }
            return allPhyPlan;
        }

    }

    public class OssLoadDataConsumer {
        @Data
        public class UploadContext {
            private OSSKey ossKey;
            private String localFilePath;
            private long fileSize;
            private final String physicalSchema;
            private final String physicalTable;
            private final String physicalPartitionName;
            public FilesRecord filesRecord;

            public UploadContext(PhyTableContext phyTableContext, int fileIndex) {
                this.localFilePath = phyTableContext.localFilePaths.get(fileIndex);
                this.ossKey = phyTableContext.ossKeys.get(fileIndex);

                this.filesRecord = phyTableContext.filesRecords.get(fileIndex);
                this.physicalSchema = phyTableContext.physicalSchema;
                this.physicalTable = phyTableContext.physicalTable;
                this.physicalPartitionName = phyTableContext.physicalPartitionName;
            }

            public void setFileSize(Long fileSize) {
                this.fileSize = fileSize;
            }
        }

        public class FlushContext {
            private boolean ifUpload;
            private UploadContext uploadContext;
            private VectorizedRowBatch batch;
            private Writer writer;

            public FlushContext(VectorizedRowBatch batch, Writer writer, boolean ifUpload) {
                this.batch = batch;
                this.writer = writer;
                this.ifUpload = ifUpload;
            }

            public void setUploadContext(
                UploadContext uploadContext) {
                this.uploadContext = uploadContext;
            }

            public ByteBuffer getSerializedTail(String localFilePath) {
                try {
                    Configuration conf = new Configuration();
                    Reader reader = OrcFile.createReader(new Path(localFilePath),
                        OrcFile.readerOptions(conf));
                    return reader.getSerializedFileFooter();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }

            public void flush() {
                try {
                    writer.addRowBatch(batch);
                    if (ifUpload) {
                        writer.close();

                        // update fileRecord after writer close
                        File localFile = new File(uploadContext.getLocalFilePath());
                        long fileSize = localFile.length();

                        ByteBuffer tailBuffer = this.getSerializedTail(uploadContext.getLocalFilePath());
                        byte[] fileMeta = new byte[tailBuffer.remaining()];
                        tailBuffer.get(fileMeta);

                        uploadContext.getFilesRecord().setExtentSize(fileSize);
                        uploadContext.getFilesRecord().setFileMeta(fileMeta);
                        try {
                            ossUploadQueue.put(uploadContext);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                } catch (IOException e) {
                    throw GeneralUtil.nestedException(e);
                }
            }
        }

        public class PhyTableContext {
            private final String physicalSchema;
            private final String physicalTable;
            private String physicalPartitionName;
            public List<FilesRecord> filesRecords;
            private OrderInvariantHasher orderInvariantHasher;
            private volatile int currentFileIndex;
            private List<OSSKey> ossKeys;
            private List<String> localFilePaths;
            private List<Long> tableRows;
            private List<Long> fileChecksum;
            private Writer writer;
            private VectorizedRowBatch batch;
            private List<Row> lowerRows;
            private List<Row> upperRows;
            private long totalRows;

            public PhyTableContext(PhysicalPartitionInfo info) {
                this.lowerRows = new ArrayList<>();
                this.upperRows = new ArrayList<>();

                this.currentFileIndex = 0;
                this.localFilePaths = new ArrayList<>();
                this.ossKeys = new ArrayList<>();
                this.tableRows = new ArrayList<>();
                this.fileChecksum = new ArrayList<>();
                this.filesRecords = new ArrayList<>();

                this.batch = schema.createRowBatch(indexStride);

                this.physicalSchema = info.getGroupKey();
                this.physicalTable = info.getPhyTable();
                initNextFile();
            }

            public void initNextFile() {
                if (this.currentFileIndex == this.localFilePaths.size()) {
                    final String uniqueId = UUID.randomUUID().toString();
                    String currentLocalFilePath =
                        OSSKey.localFilePath(physicalSchema, physicalTable, uniqueId);

                    this.localFilePaths.add(currentLocalFilePath);

                    // todo : get physicalPartitionName, temporally empty
                    OSSKey currentOssKey =
                        TStringUtil.isEmpty(physicalPartitionName)
                            ? OSSKey.createTableFileOSSKey(physicalSchema, physicalTable, uniqueId)
                            : OSSKey.createTableFileOSSKey(physicalSchema, physicalTable, physicalPartitionName,
                            uniqueId);

                    this.ossKeys.add(currentOssKey);
                    File tmpFile = new File(currentLocalFilePath);
                    if (tmpFile.exists()) {
                        tmpFile.delete();
                    }

                    // create File Record for metaDb
                    createFileMeta(currentFileIndex);

                    if (this.localFilePaths.size() == 1) {
                        lowerRows.add(null);
                    } else {
                        lowerRows.add(upperRows.get(upperRows.size() - 1));
                    }
                    upperRows.add(null);

                    this.orderInvariantHasher = new OrderInvariantHasher();
                    Path path = new Path(currentLocalFilePath);
                    try {
                        path.getFileSystem(conf).setWriteChecksum(false);
                        path.getFileSystem(conf).setVerifyChecksum(false);
                        OrcFile.WriterOptions opts = OrcFile.writerOptions(conf).setSchema(schema);
                        writer = new WriterImpl(path.getFileSystem(opts.getConfiguration()), path, opts);
                    } catch (IOException e) {
                        throw GeneralUtil.nestedException(e);
                    }

                    this.totalRows = 0L;
                }
            }

            private void createFileMeta(int fileIndex) {
                // construct files record
                FilesRecord filesRecord = new FilesRecord();
                filesRecord.fileName = this.getOssKey(fileIndex).toString();
                filesRecord.fileType = this.getOssKey(fileIndex).getFileType().toString();
                filesRecord.fileMeta = new byte[] {};
                filesRecord.tableCatalog = "";
                filesRecord.tableSchema = physicalSchema;
                filesRecord.tableName = physicalTable;
                filesRecord.engine = engine.name();
                filesRecord.status = "";
                filesRecord.lifeCycle = OSSMetaLifeCycle.CREATING.ordinal();
                filesRecord.localPath = this.localFilePaths.get(fileIndex);
                filesRecord.logicalSchemaName = logicalSchema;
                filesRecord.logicalTableName = logicalTable;
                filesRecord.localPartitionName = physicalPartitionName;

                // write to db later with all other info
                filesRecords.add(filesRecord);
            }

            public OSSKey getOssKey(int fileIndex) {
                return this.ossKeys.get(fileIndex);
            }

            public void prepareForFlush(int fileIndex) {
                this.currentFileIndex++;

                this.fileChecksum.add(orderInvariantHasher.getResult());

                this.tableRows.add(this.totalRows);
                totalRows = 0;

                FilesRecord filesRecord = filesRecords.get(fileIndex);

                // update filesRecord
                filesRecord.lifeCycle = OSSMetaLifeCycle.READY.ordinal();
                filesRecord.setTableRows(this.tableRows.get(fileIndex));
                filesRecord.setFileHash(this.fileChecksum.get(fileIndex));
            }

            public void consume(PhyTableOperation phyOp) {
                for (Map<Integer, ParameterContext> batchParam : phyOp.getBatchParameters()) {
                    this.totalRows++;

                    // build row from phyOp
                    OssLoadDataRow row = new OssLoadDataRow(batchParam);

                    // fill row data to batch
                    int rowNumber = batch.size++;
                    CrcAccumulator accumulator = new CrcAccumulator();
                    for (int columnId = 1; columnId < redundantId; columnId++) {
                        ColumnProvider columnProvider = columnProviders.get(columnId - 1);
                        ColumnVector columnVector = batch.cols[columnId - 1];
                        DataType dataType = dataTypes.get(columnId - 1);

                        int redundantColumnId = redundantMap[columnId - 1];
                        if (redundantColumnId == -1) {
                            // data convert
                            columnProvider
                                .putRow(columnVector, rowNumber, row, columnId, dataType,
                                    sessionProperties.getTimezone(), Optional.ofNullable(accumulator));
                        } else {
                            // data convert with redundant sort key
                            ColumnVector redundantColumnVector = batch.cols[redundantColumnId - 1];
                            columnProvider.putRow(columnVector, redundantColumnVector, rowNumber, row, columnId,
                                dataType,
                                sessionProperties.getTimezone(), Optional.ofNullable(accumulator));
                        }
                    }

                    // Merge the crc result of the last row.
                    long crcResult = accumulator.getResult();
                    orderInvariantHasher.add(crcResult);
                    accumulator.reset();

                    // flush the batch to disk
                    if (batch.size == batch.getMaxSize()) {
                        boolean ifUpload = this.totalRows >= maxRowsPerFile;

                        FlushContext flushContext =
                            new FlushContext(batch, writer, ifUpload);
                        if (ifUpload) {
                            int fileIndex = this.currentFileIndex;
                            totalEffectRows += this.totalRows;
                            prepareForFlush(fileIndex);
                            UploadContext uploadContext = new UploadContext(this, fileIndex);
                            flushContext.setUploadContext(uploadContext);

                            initNextFile();
                        }
                        ossFlushQueue.add(flushContext);

                        this.batch = schema.createRowBatch(indexStride);
                    }
                }
            }

            public void consumeFinish() throws Exception {
                try {
                    // get all effect rows (load rows)s
                    if (this.totalRows != 0) {
                        FlushContext flushContext =
                            new FlushContext(batch, writer, true);
                        int fileIndex = this.currentFileIndex;
                        totalEffectRows += this.totalRows;
                        prepareForFlush(fileIndex);
                        UploadContext uploadContext = new UploadContext(this, fileIndex);
                        flushContext.setUploadContext(uploadContext);
                        ossFlushQueue.add(flushContext);
                    }
                } catch (Throwable t) {
                    throw new Exception(t);
                }
            }
        }

        private final Logger
            logger = LoggerFactory.getLogger(OssLoadDataConsumer.class);
        private final TableMeta tableMeta;
        private final Engine engine;
        private final Configuration conf;
        private PolarDBXOrcSchema polarDBXOrcSchema;
        private final TypeDescription schema;
        private Map<String, PhyTableContext> phyTableContextMap;
        private List<ColumnMeta> columnMetas;
        private List<DataType> dataTypes;
        private final List<Throwable> exceptionsWhenClose = new ArrayList<>();
        private LoadDataContext loadDataContext;
        private ExecutionContext executionContext;
        private BlockingQueue<Object> ossLoadDataQueue;
        private BlockingQueue<Object> ossUploadQueue;
        private BlockingQueue<Object> ossFlushQueue;
        public ListenableFuture<Integer> consumeDoneFuture;
        private List<ColumnProvider> columnProviders;
        private long maxRowsPerFile;
        String logicalSchema;
        String logicalTable;
        int indexStride;
        String versionName;
        private int redundantId;
        private int[] redundantMap;
        SessionProperties sessionProperties;
        int totalEffectRows;

        public OssLoadDataConsumer(LoadDataContext loadDataContext,
                                   ExecutionContext executionContext, BlockingQueue<Object> queue,
                                   TableMeta tableMeta,
                                   BlockingQueue<Object> uploadBlockingQueue,
                                   BlockingQueue<Object> flushBlockingQueue,
                                   PhysicalPartitionInfo info) {
            this.loadDataContext = loadDataContext;
            this.executionContext = executionContext.copy();
            this.ossLoadDataQueue = queue;
            this.tableMeta = tableMeta;
            this.maxRowsPerFile = executionContext.getParamManager().getLong(ConnectionParams.OSS_MAX_ROWS_PER_FILE);
            this.ossUploadQueue = uploadBlockingQueue;
            this.ossFlushQueue = flushBlockingQueue;

            this.engine = this.tableMeta.getEngine();
            this.logicalSchema = this.tableMeta.getSchemaName();
            this.logicalTable = this.tableMeta.getTableName();

            sessionProperties = SessionProperties.fromExecutionContext(executionContext);

            totalEffectRows = 0;

            // read field_id
            Map<String, String> columnToFieldIdMap = new SchemaEvolutionAccessorDelegate<Map<String, String>>() {
                @Override
                protected Map<String, String> invoke() {
                    Map<String, String> map = TreeMaps.caseInsensitiveMap();
                    for (ColumnMappingRecord record :
                        columnMappingAccessor.querySchemaTable(logicalSchema, logicalTable)) {
                        map.put(record.getColumnName(), record.getFieldIdString());
                    }
                    return map;
                }
            }.execute();

            // build orc schema
            this.polarDBXOrcSchema =
                OrcMetaUtils.buildPolarDBXOrcSchema(tableMeta, Optional.of(columnToFieldIdMap), false);

            this.schema = polarDBXOrcSchema.getSchema();
            // data config
            this.conf = OrcMetaUtils.getConfiguration(executionContext, polarDBXOrcSchema);

            this.indexStride = (int) conf.getLong("orc.row.index.stride", 1000);
            this.versionName = OrcConf.WRITE_FORMAT.getString(conf);

            this.redundantId = polarDBXOrcSchema.getRedundantId();
            this.redundantMap = polarDBXOrcSchema.getRedundantMap();

            this.columnProviders = ColumnProviders.getColumnProviders(this.polarDBXOrcSchema);

            this.columnMetas = polarDBXOrcSchema.getColumnMetas();

            this.dataTypes = columnMetas.stream().map(ColumnMeta::getDataType).collect(Collectors.toList());

            phyTableContextMap = new HashMap<>();

            PhyTableContext phyTableContext = new PhyTableContext(info);

            phyTableContextMap.put(info.getPhyTable(), phyTableContext);
        }

        public void addPhyTableContext(PhysicalPartitionInfo info) {
            PhyTableContext phyTableContext = new PhyTableContext(info);
            phyTableContextMap.put(info.getPhyTable(), phyTableContext);
        }

        public void doConsume() {
            this.consumeDoneFuture = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                this::consume, executionContext.getRuntimeStatistics());
        }

        private int consume() {
            try {
                while (true) {
                    if (loadDataContext.getThrowable() != null) {
                        throw loadDataContext.getThrowable();
                    }

                    Object object = ossLoadDataQueue.take();
                    if (object == END) {
                        ossLoadDataQueue.add(END);
                        break;
                    }
                    if (object instanceof Long) {
                        loadDataContext.getDataCacheManager().releaseMemory((Long) object);
                        continue;
                    }

                    PhyTableOperation phyOp = (PhyTableOperation) object;
                    String phyTableName = phyOp.getTableNames().get(0).get(0);
                    PhyTableContext phyTableContext = phyTableContextMap.get(phyTableName);

                    phyTableContext.consume(phyOp);
                }
                for (PhyTableContext phyTableContext : phyTableContextMap.values()) {
                    phyTableContext.consumeFinish();
                }
                return totalEffectRows;
            } catch (Throwable t) {
                exceptionsWhenClose.add(t);
                logger.error("OssLoadDataExecuter consume failed", t);
                try {
                    loadDataContext.finish(t);
                } catch (Throwable ignore) {
                    //ignore
                }
                if (ossLoadDataQueue != null) {
                    ossLoadDataQueue.clear();
                    ossLoadDataQueue.add(END);
                }
                return -1;
            }
        }

        public void ossLoadDataQueueAddEND() {
            ossLoadDataQueue.add(END);
        }

        public void ossLoadDataQueueClear() {
            ossLoadDataQueue.clear();
        }
    }

    public class OssLoadDataFlusher {
        private final Logger
            logger = LoggerFactory.getLogger(OssLoadDataFlusher.class);
        private ExecutionContext executionContext;
        private BlockingQueue<Object> ossFlushQueue;
        public ListenableFuture<Boolean> flushDoneFuture;

        public OssLoadDataFlusher(ExecutionContext executionContext, BlockingQueue<Object> ossFlushQueue) {
            this.executionContext = executionContext;
            this.ossFlushQueue = ossFlushQueue;
        }

        public void doFlush() {
            this.flushDoneFuture = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                this::flush, executionContext.getRuntimeStatistics());
        }

        private boolean flush() {
            try {
                while (true) {
                    Object object = ossFlushQueue.take();

                    if (object == FLUSH_END) {
                        ossFlushQueue.add(FLUSH_END);
                        break;
                    }

                    OssLoadDataConsumer.FlushContext flushContext = (OssLoadDataConsumer.FlushContext) object;

                    flushContext.flush();
                }
                return true;
            } catch (Throwable t) {
                return false;
            }
        }

    }

    public class OssLoadDataUploader {
        private final Logger
            logger = LoggerFactory.getLogger(OssLoadDataUploader.class);
        private final TableMeta tableMeta;
        private final Engine engine;
        private final Configuration conf;
        private List<ColumnMetasRecord> columnMetasRecords;
        public List<FilesRecord> filesRecords;
        private TypeDescription bfSchema;
        private List<ColumnProvider> bfColumnProviders;
        private ExecutionContext executionContext;
        private String logicalSchema;
        private String logicalTable;
        private BlockingQueue<Object> ossUploadQueue;
        final private boolean removeTmpFiles;
        public ListenableFuture<Boolean> uploadDoneFuture;
        private double fpp;

        public OssLoadDataUploader(LoadDataContext loadDataContext,
                                   ExecutionContext executionContext,
                                   BlockingQueue<Object> uploadQueue,
                                   TableMeta tableMeta) {
            this.executionContext = executionContext.copy();
            this.ossUploadQueue = uploadQueue;
            this.tableMeta = tableMeta;

            this.engine = this.tableMeta.getEngine();
            this.logicalSchema = this.tableMeta.getSchemaName();
            this.logicalTable = this.tableMeta.getTableName();

            // read field_id
            Map<String, String> columnToFieldIdMap = new SchemaEvolutionAccessorDelegate<Map<String, String>>() {
                @Override
                protected Map<String, String> invoke() {
                    Map<String, String> map = TreeMaps.caseInsensitiveMap();
                    for (ColumnMappingRecord record :
                        columnMappingAccessor.querySchemaTable(logicalSchema, logicalTable)) {
                        map.put(record.getColumnName(), record.getFieldIdString());
                    }
                    return map;
                }
            }.execute();

            // build orc schema
            PolarDBXOrcSchema polarDBXOrcSchema =
                OrcMetaUtils.buildPolarDBXOrcSchema(tableMeta, Optional.of(columnToFieldIdMap), false);

            this.bfColumnProviders = ColumnProviders.getBfColumnProviders(polarDBXOrcSchema);

            // data config
            this.conf = OrcMetaUtils.getConfiguration(executionContext, polarDBXOrcSchema);

            this.bfSchema = polarDBXOrcSchema.getBfSchema();

            this.filesRecords = new ArrayList<>();
            this.columnMetasRecords = new ArrayList<>();

            this.removeTmpFiles = true;

            this.fpp = conf.getDouble("orc.bloom.filter.fpp", 0.01D);
        }

        public void doUpload() {
            this.uploadDoneFuture = executionContext.getExecutorService().submitListenableFuture(
                executionContext.getSchemaName(), executionContext.getTraceId(), -1,
                this::upload, executionContext.getRuntimeStatistics());
        }

        private boolean upload() {
            try {
                while (true) {
                    Object object = ossUploadQueue.take();

                    if (object == UPLOAD_END) {
                        ossUploadQueue.add(UPLOAD_END);
                        break;
                    }

                    OssLoadDataConsumer.UploadContext uploadContext = (OssLoadDataConsumer.UploadContext) object;

                    filesRecords.add(uploadContext.filesRecord);

                    uploadToOss(uploadContext);

                    if (shouldPutBloomFilter()) {
                        putBloomFilter(uploadContext);
                    }

                    // delete file from local.
                    if (removeTmpFiles) {
                        File tmpFile = new File(uploadContext.getLocalFilePath());
                        if (tmpFile.exists()) {
                            tmpFile.delete();
                        }
                    }
                }
                return true;
            } catch (Throwable t) {
                return false;
            }
        }

        public void uploadToOss(OssLoadDataConsumer.UploadContext writerContext) {
            try {
                String localFilePath = writerContext.getLocalFilePath();
                OSSKey ossKey = writerContext.getOssKey();

                File localFile = new File(localFilePath);
                writerContext.setFileSize(localFile.length());
                logger.info("orc generation done: " + localFilePath);
                logger.info("file size(in bytes): " + writerContext.getFileSize());

                FileSystemUtils.writeFile(localFile, ossKey.toString(), this.engine);
                logger.info("file upload done: " + localFilePath);
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        private boolean shouldPutBloomFilter() {
            return this.bfSchema != null && !this.bfSchema.getChildren().isEmpty();
        }

        public void putBloomFilter(OssLoadDataConsumer.UploadContext uploadContext) {
            try {
                // prepare for index file
                final String uniqueId = UUID.randomUUID().toString();
                OSSKey metaKey = OSSKey.createBloomFilterFileOSSKey(
                    uploadContext.getPhysicalSchema(),
                    uploadContext.getPhysicalTable(),
                    uniqueId,
                    "",
                    0
                );
                String localIndexFilePath = metaKey.localPath();
                File localIndexFile = new File(localIndexFilePath);

                int lastOffset, currentOffset = 0;

                // construct for all index key.
                // for each orc file
                String localFilePath = uploadContext.getLocalFilePath();

                try (FileOutputStream outputStream = new FileOutputStream(localIndexFile);
                    Reader reader = OrcFile.createReader(new Path(localFilePath), OrcFile.readerOptions(conf))) {

                    List<StripeInformation> stripes = reader.getStripes();
                    for (int stripeIndex = 0; stripeIndex < stripes.size(); stripeIndex++) {
                        // for each stripe
                        StripeInformation stripe = stripes.get(stripeIndex);
                        Reader.Options readerOptions = new Reader.Options(conf)
                            .schema(bfSchema)
                            .range(stripe.getOffset(), stripe.getLength());

                        long stripeRows = stripe.getNumberOfRows();
                        final List<TypeDescription> children = bfSchema.getChildren();

                        // <column id> - bloom filter
                        Map<Integer, OrcBloomFilter> bloomFilterMap = new HashMap<>();
                        for (TypeDescription child : children) {
                            OrcBloomFilter bloomFilter = new OrcBloomFilter(stripeRows, fpp);
                            bloomFilterMap.put(child.getId(), bloomFilter);
                        }

                        try (RecordReaderImpl rows = (RecordReaderImpl) reader.rows(readerOptions)) {
                            VectorizedRowBatch batch = bfSchema.createRowBatch();
                            while (rows.nextBatch(batch)) {
                                int batchSize = batch.size;
                                for (int col = 0; col < children.size(); col++) {
                                    // for each column, put vector to bloom filter.
                                    TypeDescription child = children.get(col);
                                    ColumnVector vector = batch.cols[col];
                                    OrcBloomFilter bf = bloomFilterMap.get(child.getId());
                                    bfColumnProviders.get(col).putBloomFilter(vector, bf, 0, batchSize);
                                }
                            }
                        }

                        for (int col = 0; col < children.size(); col++) {
                            // for each column in this stripe,
                            // upload meta file, and record meta info.
                            int colId = children.get(col).getId();
                            String colName = bfSchema.getFieldNames().get(col);

                            // serialize the bloom-filter data to local file
                            // update files table
                            OrcBloomFilter bf = bloomFilterMap.get(colId);
                            int writtenBytes = OrcBloomFilter.serialize(outputStream, bf);
                            lastOffset = currentOffset;
                            currentOffset += writtenBytes;

                            storeColumnMeta(uploadContext, uploadContext.getOssKey(), metaKey,
                                lastOffset, stripeIndex, stripe, colId, colName, writtenBytes);
                        }
                    }

                    storeIndexFileMeta(uploadContext, metaKey, localIndexFilePath, localIndexFile);
                } finally {
                    if (removeTmpFiles) {
                        if (localIndexFile.exists()) {
                            localIndexFile.delete();
                        }
                    }
                }
            } catch (IOException e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        private void storeColumnMeta(OssLoadDataConsumer.UploadContext uploadContext, OSSKey fileKey, OSSKey metaKey,
                                     int lastOffset,
                                     int stripeIndex,
                                     StripeInformation stripe, int colId, String colName, int writtenBytes) {
            // store the column meta for <file, stripe, column>
            ColumnMetasRecord columnMetasRecord = new ColumnMetasRecord();
            columnMetasRecord.tableFileName = fileKey.toString();
            columnMetasRecord.tableName = uploadContext.getPhysicalTable();
            columnMetasRecord.tableSchema = uploadContext.getPhysicalSchema();
            columnMetasRecord.stripeIndex = stripeIndex;
            columnMetasRecord.stripeOffset = stripe.getOffset();
            columnMetasRecord.stripeLength = stripe.getLength();
            columnMetasRecord.columnName = colName;
            columnMetasRecord.columnIndex = colId;
            columnMetasRecord.bloomFilterPath = metaKey.toString();
            columnMetasRecord.bloomFilterOffset = lastOffset;
            columnMetasRecord.bloomFilterLength = writtenBytes;
            columnMetasRecord.isMerged = 1;
            columnMetasRecord.lifeCycle = OSSMetaLifeCycle.READY.ordinal();
            columnMetasRecord.engine = this.engine.name();
            columnMetasRecord.logicalSchemaName = logicalSchema;
            columnMetasRecord.logicalTableName = logicalTable;

            columnMetasRecords.add(columnMetasRecord);
        }

        private void storeIndexFileMeta(OssLoadDataConsumer.UploadContext uploadContext, OSSKey metaKey,
                                        String localIndexFilePath,
                                        File localIndexFile) throws IOException {
            // handle index file
            // write to metaDB files table.
            FilesRecord filesRecord = new FilesRecord();
            filesRecord.fileName = metaKey.toString();
            filesRecord.fileType = metaKey.getFileType().toString();
            filesRecord.tableSchema = uploadContext.getPhysicalSchema();
            filesRecord.tableName = uploadContext.getPhysicalTable();
            filesRecord.tableCatalog = "";
            filesRecord.engine = this.engine.name();
            filesRecord.lifeCycle = OSSMetaLifeCycle.READY.ordinal();
            filesRecord.localPath = localIndexFilePath;
            filesRecord.status = "";
            filesRecord.logicalSchemaName = logicalSchema;
            filesRecord.logicalTableName = logicalTable;
            filesRecord.localPartitionName = uploadContext.physicalPartitionName;

            filesRecords.add(filesRecord);

            // upload to oss
            FileSystemUtils.writeFile(localIndexFile, metaKey.toString(), this.engine);
        }
    }

    public class OssLoadDataPersistAndSync {
        private final Logger
            logger = LoggerFactory.getLogger(OssLoadDataPersistAndSync.class);

        private final List<OssLoadDataUploader> uploaders;
        private final ExecutionContext executionContext;
        private final String logicalSchema;
        private final String logicalTable;

        public OssLoadDataPersistAndSync(ExecutionContext executionContext, List<OssLoadDataUploader> uploaders,
                                         TableMeta tableMeta) {
            this.executionContext = executionContext;
            this.uploaders = uploaders;

            this.logicalSchema = tableMeta.getSchemaName();
            this.logicalTable = tableMeta.getTableName();
        }

        public void writeMetaDbTransaction() {
            // get tso to fill commitTs and taskId
            final ITimestampOracle timestampOracle =
                executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
            if (null == timestampOracle) {
                throw new UnsupportedOperationException("Do not support timestamp oracle");
            }
            long ts = timestampOracle.nextTimestamp();

            for (OssLoadDataUploader uploader : uploaders) {
                for (FilesRecord filesRecord : uploader.filesRecords) {
                    filesRecord.commitTs = ts;
                }
            }

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                try {
                    // 用事务保证原子性
                    MetaDbUtil.beginTransaction(metaDbConn);

                    // fill in with same tso
                    for (OssLoadDataUploader uploader : uploaders) {
                        for (ColumnMetasRecord columnMetasRecord : uploader.columnMetasRecords) {
                            columnMetasRecord.taskId = ts;
                            TableMetaChanger.addOssColumnMeta(metaDbConn, logicalSchema, logicalTable,
                                columnMetasRecord);
                        }

                        for (FilesRecord filesRecord : uploader.filesRecords) {
                            filesRecord.commitTs = ts;
                            // no task id , fill with ts
                            filesRecord.taskId = ts;
                            TableMetaChanger.addOssFileWithTso(metaDbConn, logicalSchema, logicalTable,
                                filesRecord);
                        }
                    }

                    // update table version
                    TablesAccessor tableAccessor = new TablesAccessor();
                    tableAccessor.setConnection(metaDbConn);
                    long tableMetaVersion = tableAccessor.getTableMetaVersionForUpdate(logicalSchema, logicalTable);
                    long newVersion = tableMetaVersion + 1;
                    tableAccessor.updateVersion(logicalSchema, logicalTable, newVersion);

                    MetaDbUtil.commit(metaDbConn);
                } catch (Exception e) {
                    MetaDbUtil.rollback(metaDbConn, e, null, null);
                    throw GeneralUtil.nestedException(e);
                } finally {
                    MetaDbUtil.endTransaction(metaDbConn, logger);
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }
        }

        public void tableSync() {
            long trxId = executionContext.getTransaction().getId();
            SyncManagerHelper.sync(new TablesMetaChangeForOssSyncAction(logicalSchema,
                Collections.singletonList(logicalTable),
                executionContext.getConnId(), trxId));
        }
    }
}
