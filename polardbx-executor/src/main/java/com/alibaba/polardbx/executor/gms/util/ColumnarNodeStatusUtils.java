package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ColumnarNodeStatusUtils {

    private final static Logger LOGGER = LoggerFactory.getLogger("ColumnarNodeStatusUtils");

    /**
     * 检测操作的锁，防止并发执行
     */
    private final static Object lock = new Object();

    /**
     * 请求过来，距离上次检测时间超过5s，则再次检测，否则直接返回
     */
    private final static AtomicLong lastCheckTimeMs = new AtomicLong(0);

    /**
     * 列存节点同步状态
     */
    private final static AtomicReference<ColumnarNodeStatusInfo> columnarInfo =
        new AtomicReference<>(new ColumnarNodeStatusInfo(ColumnarNodeStatus.NONE, ""));

    public enum ColumnarNodeStatus {
        /**
         * 无状态
         */
        NONE,
        /**
         * 正常状态
         */
        NORMAL,
        /**
         * 中止状态
         */
        PAUSED
    }

    @Data
    public static class ColumnarNodeStatusInfo {
        /**
         * 状态
         */
        public ColumnarNodeStatus columnarNodeStatus;
        /**
         * 相关信息
         */
        public String info;

        public ColumnarNodeStatusInfo(ColumnarNodeStatus columnarNodeStatus, String info) {
            this.columnarNodeStatus = columnarNodeStatus;
            this.info = info;
        }
    }

    /**
     * 获取列存节点同步状态
     */
    public static ColumnarNodeStatusInfo getColumnarNodeStatus() {
        if (System.currentTimeMillis() - lastCheckTimeMs.get() <= 5000L) {
            // 距离上次检测时间小于5s，直接返回
            return columnarInfo.get();
        } else {
            //主动检测一次，更新检测时间
            checkColumnarNodeStatus();
            return columnarInfo.get();
        }
    }

    /**
     * 检查列存同步状态，设置状态和信息
     */
    private static void checkColumnarNodeStatus() {
        synchronized (lock) {
            if (System.currentTimeMillis() - lastCheckTimeMs.get() <= 5000) {
                // 再次检测，防止并发，距离上次检测时间小于5s，直接返回
                return;
            } else {
                try (Connection connection = MetaDbUtil.getConnection()) {
                    ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
                    accessor.setConnection(connection);
                    List<ColumnarTableMappingRecord> records = accessor.queryLimitOne();
                    if (records.isEmpty()) {
                        // 一个列存索引都没有，认为是正常的，大部分情况
                        columnarInfo.set(new ColumnarNodeStatusInfo(ColumnarNodeStatus.NORMAL, ""));
                        lastCheckTimeMs.set(System.currentTimeMillis());
                        return;
                    }

                    ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
                    checkpointsAccessor.setConnection(connection);

                    List<ColumnarCheckpointsRecord> columnarCheckpointsRecords = checkpointsAccessor.queryLastByTypes(
                        ImmutableList.of(ColumnarCheckpointsAccessor.CheckPointType.STREAM,
                            ColumnarCheckpointsAccessor.CheckPointType.DDL,
                            ColumnarCheckpointsAccessor.CheckPointType.HEARTBEAT));
                    if (columnarCheckpointsRecords.isEmpty()) {
                        // 没有检测到任何列存提交，认为是暂停状态，可能没有列存节点或启动有问题
                        columnarInfo.set(
                            new ColumnarNodeStatusInfo(ColumnarNodeStatus.PAUSED, "No columnar checkpoint"));
                        lastCheckTimeMs.set(System.currentTimeMillis());
                        return;
                    }

                    //获取CDC位点和延迟
                    Pair<Long, Long> cdcTsoAndDelay = CdcUtils.getCdcTsoAndDelay();

                    long cdcTso = cdcTsoAndDelay.getKey();
                    long cdcMs = cdcTso >>> 22;
                    long cdcDelay = cdcTsoAndDelay.getValue();

                    if (cdcDelay > 600 * 1000L) {
                        //cdc 延迟超过10分钟，认为暂停状态
                        columnarInfo.set(
                            new ColumnarNodeStatusInfo(ColumnarNodeStatus.PAUSED, "CDC delay too long, > 10min"));
                        lastCheckTimeMs.set(System.currentTimeMillis());
                        return;
                    }
                    long columnarMs = columnarCheckpointsRecords.get(0).binlogTso >>> 22;
                    if (cdcMs - columnarMs > 600 * 1000L) {
                        //cdc位点与列存位点相差超过10分钟，认为列存中止了，暂停状态
                        columnarInfo.set(
                            new ColumnarNodeStatusInfo(ColumnarNodeStatus.PAUSED, "Columnar delay too long, > 10min"));
                        lastCheckTimeMs.set(System.currentTimeMillis());
                        return;
                    } else {
                        //正常状态
                        columnarInfo.set(new ColumnarNodeStatusInfo(ColumnarNodeStatus.NORMAL, ""));
                        lastCheckTimeMs.set(System.currentTimeMillis());
                    }

                } catch (Exception e) {
                    LOGGER.error("checkColumnarNodeStatus error", e);
                    throw new RuntimeException(e);
                }

            }
        }
    }

    /**
     * 测试使用
     */
    public static AtomicLong getLastCheckTimeMs() {
        return lastCheckTimeMs;
    }

}
