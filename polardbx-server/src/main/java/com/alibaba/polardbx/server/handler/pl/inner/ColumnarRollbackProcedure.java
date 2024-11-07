package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.utils.AddressUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.engine.FileSystemManager;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarLeaseRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableEvolutionRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.sql.Connection;
import java.util.List;

/**
 * @author lijiu
 */
public class ColumnarRollbackProcedure extends BaseInnerProcedure {

    private static final Logger LOGGER = LoggerFactory.getLogger("COLUMNAR_TRANS");

    //特殊值，用于标识rollback的pid
    private static final long ROLLBACK_PID = 1;

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<SQLExpr> params = statement.getParameters();
        //必须polardbx_root用户执行，防止普通用户执行
        checkPrivilege(c);
        //参数解析
        long tso = checkParameters(params, statement);

        columnarRollback(tso);

        //返回结果
        cursor.addColumn("ROLLBACK_TSO", DataTypes.LongType);
        cursor.addRow(new Object[] {tso});
    }

    private void checkPrivilege(ServerConnection c) {
        if (!PolarPrivUtil.isPolarxRootUser(c.getUser())) {
            throw new RuntimeException("Access denied for user '" + c.getUser() + "'@'"
                + c.getHost() + "'" + " to do columnar_rollback()");
        }
    }

    private long checkParameters(List<SQLExpr> params, SQLCallStatement statement) {
        if (params.size() != 1) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 1 parameters");
        }
        if (!(params.get(0) instanceof SQLIntegerExpr)) {
            throw new IllegalArgumentException(statement.toString() + " parameters need Long number");
        }
        SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) params.get(0);
        long tso = sqlIntegerExpr.getNumber().longValue();
        if (tso < 0) {
            throw new IllegalArgumentException(statement.toString() + " parameters is invalid Long number, need >= 0");
        }
        return tso;
    }

    private void columnarRollback(long binlogTso) {
        ColumnarCheckpointsRecord columnarCheckPoint;
        //1、先检测tso值是否有效，获取列存对应的TSO
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);

            List<ColumnarCheckpointsRecord> checkpointsRecords =
                checkpointsAccessor.queryColumnarTsoByBinlogTso(binlogTso);
            if (checkpointsRecords.isEmpty()) {
                throw new RuntimeException("Not found columnar commit tso, can't rollback to " + binlogTso);
            }
            columnarCheckPoint = checkpointsRecords.get(0);

            //获取purgeTso
            ColumnarPurgeHistoryAccessor purgeHistoryAccessor = new ColumnarPurgeHistoryAccessor();
            purgeHistoryAccessor.setConnection(metaDbConn);
            List<ColumnarPurgeHistoryRecord> purgeRecords = purgeHistoryAccessor.queryLastPurgeTso();
            long purgeTso = 0;
            if (!purgeRecords.isEmpty()) {
                purgeTso = purgeRecords.get(0).tso;
            }
            if (purgeTso > columnarCheckPoint.checkpointTso) {
                throw new RuntimeException("Can't rollback to " + binlogTso + " which < purgeTso: " + purgeTso);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        LOGGER.warn(
            "START to ROLLBACK COLUMNAR to " + columnarCheckPoint.checkpointTso + " from binlogTso: " + binlogTso);

        //2、抢主执行回滚操作
        String owner = getColumnarNodeName();
        long startMs;
        long lastNodeLease;
        try {
            //抢主
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarLeaseAccessor leaseAccessor = new ColumnarLeaseAccessor();
                leaseAccessor.setConnection(metaDbConn);

                //默认1小时
                startMs = System.currentTimeMillis();
                lastNodeLease = leaseAccessor.forceElectInsert(owner, startMs, 3600 * 1000);
                if (lastNodeLease < 0) {
                    //insert失败，开启事务执行, select for update
                    metaDbConn.setAutoCommit(false);

                    //锁超时设置为5s，防止select for update等太久
                    MetaDbUtil.execute("SET SESSION innodb_lock_wait_timeout = 5;", metaDbConn);
                    List<ColumnarLeaseRecord> lastLeaseRecords = leaseAccessor.forceElectSelectForUpdate();
                    if (lastLeaseRecords.size() != 1) {
                        throw new RuntimeException("Force Elect columnar Leader failed by select for update");
                    }
                    ColumnarLeaseRecord lastLeaseRecord = lastLeaseRecords.get(0);
                    String[] parts = lastLeaseRecord.owner.split("@");
                    if (lastLeaseRecord.lease > startMs && parts.length == 3
                        && Long.parseLong(parts[1]) == ROLLBACK_PID) {
                        //存在别的节点执行columnar_rollback()
                        throw new RuntimeException(
                            "Force Elect columnar Leader failed by have another columnar_rollback() op by " + parts[0]);
                    }

                    startMs = System.currentTimeMillis();
                    leaseAccessor.forceElectUpdate(owner, startMs, 3600 * 1000);
                    lastNodeLease = lastLeaseRecord.lease;

                    metaDbConn.commit();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            //等待上一个节点的leader时间过期
            if (lastNodeLease > 0 && lastNodeLease > startMs) {
                if ((lastNodeLease - startMs) > 100000) {
                    //超过100s，认为不可等待，抛错
                    throw new RuntimeException(
                        "Can't rollback to " + binlogTso + ", because last columnar leader have lease time: "
                            + (lastNodeLease - startMs) + " ms > 100 s");
                }
                LOGGER.warn("Wait last columnar leader lease time: " + (lastNodeLease - startMs) + " ms");
                Thread.sleep(lastNodeLease - startMs);
            }

            //回滚列存元数据
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                //TODO：事务超过DN限制的binlog大小，一般备份恢复不会时间间隔很大，暂时开事务，保证一致性
                metaDbConn.setAutoCommit(false);
                FilesAccessor filesAccessor = new FilesAccessor();
                filesAccessor.setConnection(metaDbConn);
                ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
                checkpointsAccessor.setConnection(metaDbConn);

                //1、删除新增的orc文件
                filesAccessor.deleteOrcMetaByCommitTso(columnarCheckPoint.checkpointTso);

                //2、修改remove_ts,删除文件删除标记
                filesAccessor.updateRemoveTsByTso(columnarCheckPoint.checkpointTso);

                //3、删除新增的追加写文件，csv文件，del文件，set文件
                filesAccessor.deleteThreeMetaByCommitTso(columnarCheckPoint.checkpointTso);

                //4、删除新增追加写的append记录
                ColumnarAppendedFilesAccessor appendedFilesAccessor = new ColumnarAppendedFilesAccessor();
                appendedFilesAccessor.setConnection(metaDbConn);
                appendedFilesAccessor.deleteByTso(columnarCheckPoint.checkpointTso);

                //5、ddl事件恢复
                ColumnarTableEvolutionAccessor tableEvolutionAccessor = new ColumnarTableEvolutionAccessor();
                tableEvolutionAccessor.setConnection(metaDbConn);
                List<ColumnarTableEvolutionRecord> tableEvolutionRecords =
                    tableEvolutionAccessor.queryByOverCommitTs(columnarCheckPoint.checkpointTso);
                if (!tableEvolutionRecords.isEmpty()) {
                    for (ColumnarTableEvolutionRecord tableEvolutionRecord : tableEvolutionRecords) {
                        if (tableEvolutionRecord.ddlType.equals(DdlType.CREATE_INDEX.name())) {
                            //创建索引，如果是public状态，改会creating状态
                            ColumnarTableMappingAccessor tableMappingAccessor = new ColumnarTableMappingAccessor();
                            tableMappingAccessor.setConnection(metaDbConn);
                            tableMappingAccessor.updateStatusByTableIdAndStatus(tableEvolutionRecord.tableId,
                                ColumnarTableStatus.PUBLIC.name(), ColumnarTableStatus.CREATING.name());
                        }
                        //其它ddl，暂时没有额外操作
                    }
                    //将ddl commit ts该回未提交状态
                    tableEvolutionAccessor.updateCommitTs(columnarCheckPoint.checkpointTso);
                }

                //6、compaction事件恢复
                if (columnarCheckPoint.minCompactionTso > 0) {
                    //如果有compaction，找到所有小于checkpoint Tso后提交的compaction
                    List<ColumnarCheckpointsRecord> columnarCheckpointsRecords =
                        checkpointsAccessor.queryCompactionByStartTsoAndEndTso(columnarCheckPoint.minCompactionTso,
                            columnarCheckPoint.checkpointTso);
                    for (ColumnarCheckpointsRecord compactionRecord : columnarCheckpointsRecords) {
                        //判断该compaction需不需要回滚
                        boolean needRollback = false;
                        List<FilesRecord> compactionFilesRecords =
                            filesAccessor.queryCompactionFileByTsoAndTable(compactionRecord.checkpointTso,
                                compactionRecord.logicalSchema, compactionRecord.logicalTable);
                        if (!compactionFilesRecords.isEmpty()) {
                            //检查所有文件是否存在，理论上检查一个文件即可，因为批量文件提交是原子的
                            try {
                                FileSystem fileSystem = FileSystemManager.getFileSystemGroup(
                                    Engine.of(compactionFilesRecords.get(0).getEngine())).getMaster();
                                boolean exists = fileSystem.exists(new Path(compactionFilesRecords.get(0).fileName));
                                if (!exists) {
                                    needRollback = true;
                                }
                            } catch (Exception e) {
                                //检查报错，认为需要回滚
                                LOGGER.warn("check " + compactionFilesRecords.get(0).fileName + " file in "
                                    + compactionFilesRecords.get(0).getEngine() + " exist failed!", e);
                                needRollback = true;
                            }
                        }
                        if (needRollback) {
                            LOGGER.warn(compactionRecord.info + " compaction will rollback, tso: "
                                + compactionRecord.checkpointTso);
                            //compaction 删除标的恢复
                            filesAccessor.updateCompactionRemoveTsByTso(compactionRecord.logicalSchema,
                                compactionRecord.logicalTable, compactionRecord.checkpointTso);
                            //compaction 新增文件删除
                            filesAccessor.deleteCompactionFileByCommitTso(compactionRecord.logicalSchema,
                                compactionRecord.logicalTable, compactionRecord.checkpointTso);
                            //compaction 追加写回滚
                            appendedFilesAccessor.deleteByEqualTso(compactionRecord.checkpointTso);
                            //compaction 版本记录删除
                            checkpointsAccessor.deleteCompactionByTso(compactionRecord.checkpointTso);
                        }

                    }
                }

                //7、删除多余的checkpoints
                checkpointsAccessor.deleteByTso(columnarCheckPoint.checkpointTso);
                metaDbConn.commit();
            }

            LOGGER.warn(
                "Success ROLLBACK COLUMNAR to " + columnarCheckPoint.checkpointTso + " from binlogTso: " + binlogTso);
        } catch (Exception e) {
            LOGGER.warn(e);
            throw new RuntimeException(e);
        } finally {
            //必须释放主
            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                ColumnarLeaseAccessor leaseAccessor = new ColumnarLeaseAccessor();
                leaseAccessor.setConnection(metaDbConn);
                leaseAccessor.delete(owner);
            } catch (Exception e) {
                LOGGER.warn(e);
            }
        }
    }

    public static String getColumnarNodeName() {
        String nodeIp = AddressUtils.getHostIp();
        if (StringUtils.isEmpty(nodeIp)) {
            nodeIp = "notFound";
        }
        final long startTime = System.currentTimeMillis();

        return nodeIp + '@' + ROLLBACK_PID + '@' + startTime;
    }
}
