package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.metadb.table.FileInfoRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import lombok.Data;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author lijiu
 */
public class ColumnarSnapshotFilesProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<SQLExpr> params = statement.getParameters();
        //参数解析
        long tso = checkParameters(params, statement);

        //获取文件
        List<FileInfo> fileInfos = getSnapshotFilesInfo(tso);

        //排序
        fileInfos.sort((o1, o2) -> {
            if (o1.getFileType() == o2.getFileType()) {
                return o1.getFileName().compareTo(o2.getFileName());
            } else {
                return Integer.compare(o1.getFileType(), o2.getFileType());
            }
        });

        //返回结果
        cursor.addColumn("file_name", DataTypes.StringType);
        cursor.addColumn("file_length", DataTypes.LongType);
        cursor.addColumn("file_type", DataTypes.IntegerType);
        for (FileInfo fileInfo : fileInfos) {
            cursor.addRow(new Object[] {fileInfo.getFileName(), fileInfo.getFileLength(), fileInfo.getFileType()});
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

    @Data
    private static class FileInfo {
        String fileName;
        long fileLength;
        /**
         * 0:固定长度文件；1:追加写文件
         */
        int fileType;
    }

    private List<FileInfo> getSnapshotFilesInfo(long binlogTso) {
        List<FileInfo> fileInfos = new ArrayList<>();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            //1,根据行存的tso获取列存的tso
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);
            ColumnarCheckpointsRecord columnarCheckPoint;
            long waitTimeLimit = DynamicConfig.getInstance().getWaitForColumnarCommitMS();
            long waitTime = 0;
            while (true) {
                //循环等待1s查询，直到列存同步到该位点，或60s超时
                List<ColumnarCheckpointsRecord> checkpointsRecords =
                    checkpointsAccessor.queryColumnarTsoByBinlogTso(binlogTso);
                if (!checkpointsRecords.isEmpty()) {
                    columnarCheckPoint = checkpointsRecords.get(0);
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                waitTime += 1000;
                if (waitTime > waitTimeLimit) {
                    throw new RuntimeException(
                        "Wait for Columnar commit to " + binlogTso + ", wait " + waitTimeLimit + " ms timeout.");
                }
            }

            //2、根据列存版本tso获取所有有效文件，长度，类型
            //a、获取purge的水位线，当作下水位限
            ColumnarPurgeHistoryAccessor purgeHistoryAccessor = new ColumnarPurgeHistoryAccessor();
            purgeHistoryAccessor.setConnection(metaDbConn);
            List<ColumnarPurgeHistoryRecord> purgeRecords = purgeHistoryAccessor.queryLastPurgeTso();
            long purgeTso = 0;
            if (!purgeRecords.isEmpty()) {
                purgeTso = purgeRecords.get(0).tso;
            }
            //b、直接files系统表扫描，COLUMNAR_TABLE_MAPPING_TABLE表直接关联，以防冷数据归档文件，这样获取的文件可能会多一些，精细化的话要列存版本tso需要排除ddl操作的表，在恢复时处理更好
            //先获取固定长度文件，orc文件，主键索引sst文件
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(metaDbConn);
            List<FileInfoRecord> filesRecords =
                filesAccessor.queryORCAndSSTFileInfoByTso(purgeTso, columnarCheckPoint.checkpointTso);
            for (FileInfoRecord fileInfoRecord : filesRecords) {
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(fileInfoRecord.fileName);
                fileInfo.setFileLength(fileInfoRecord.extentSize);
                fileInfo.setFileType(0);
                fileInfos.add(fileInfo);
            }
            //c、再获取追加写文件，csv文件、del文件、set文件，也需获取长度
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(metaDbConn);
            List<ColumnarAppendedFilesRecord> appendedFilesRecords =
                columnarAppendedFilesAccessor.queryLastValidAppendByStartTsoAndEndTso(purgeTso,
                    columnarCheckPoint.checkpointTso);
            for (ColumnarAppendedFilesRecord fileInfoRecord : appendedFilesRecords) {
                if (fileInfoRecord.getAppendOffset() + fileInfoRecord.getAppendLength() == 0) {
                    //过滤长度为0的文件
                    continue;
                }
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(fileInfoRecord.fileName);
                fileInfo.setFileLength(fileInfoRecord.getAppendOffset() + fileInfoRecord.getAppendLength());
                fileInfo.setFileType(1);
                fileInfos.add(fileInfo);
            }
            //d、获取主键索引的log文件和长度
            List<ColumnarAppendedFilesRecord> pkIndexRecords =
                columnarAppendedFilesAccessor.queryFilesByFileType("pk_idx_log_meta");
            for (ColumnarAppendedFilesRecord fileInfoRecord : pkIndexRecords) {
                if (fileInfoRecord.getAppendOffset() + fileInfoRecord.getAppendLength() == 0) {
                    //过滤长度为0的文件
                    continue;
                }
                FileInfo fileInfo = new FileInfo();
                fileInfo.setFileName(fileInfoRecord.fileName);
                fileInfo.setFileLength(fileInfoRecord.getAppendOffset() + fileInfoRecord.getAppendLength());
                fileInfo.setFileType(1);
                fileInfos.add(fileInfo);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return fileInfos;
    }
}
