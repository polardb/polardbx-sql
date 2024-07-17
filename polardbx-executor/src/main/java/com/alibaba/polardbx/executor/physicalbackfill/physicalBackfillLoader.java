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

package com.alibaba.polardbx.executor.physicalbackfill;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.mysql.cj.polarx.protobuf.PolarxPhysicalBackfill;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

/**
 * Fill batch data into index table with duplication check
 */
public class physicalBackfillLoader {

    private final String schemaName;
    private final String tableName;
    private final static int MAX_RETRY = 3;

    public physicalBackfillLoader(String schemaName, String tableName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    public void applyBatch(final Pair<String, String> targetDbAndGroup, final Pair<String, String> targetFileAndDir,
                           final List<Pair<String, Integer>> targetHosts,
                           final Pair<String, String> userInfo,
                           final PolarxPhysicalBackfill.TransferFileDataOperator transferFileData,
                           final ExecutionContext ec) {
        if (GeneralUtil.isEmpty(targetHosts)) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                String.format("invalid target address for group:[%s]", targetDbAndGroup.getValue()));
        }
        PolarxPhysicalBackfill.TransferFileDataOperator.Builder builder =
            PolarxPhysicalBackfill.TransferFileDataOperator.newBuilder();

        builder.setOperatorType(PolarxPhysicalBackfill.TransferFileDataOperator.Type.PUT_DATA_TO_TAR_IBD);
        PolarxPhysicalBackfill.FileInfo.Builder fileInfoBuilder = PolarxPhysicalBackfill.FileInfo.newBuilder();
        fileInfoBuilder.setFileName(targetFileAndDir.getKey());
        fileInfoBuilder.setTempFile(false);
        fileInfoBuilder.setDirectory(targetFileAndDir.getValue());
        fileInfoBuilder.setPartitionName("");
        builder.setFileInfo(fileInfoBuilder.build());
        builder.setBufferLen(transferFileData.getBufferLen());
        builder.setOffset(transferFileData.getOffset());
        builder.setBuffer(transferFileData.getBuffer());
        List<Future> futures = new ArrayList<>(targetHosts.size());

        targetHosts.forEach(v -> {
            FutureTask<Void> task = new FutureTask<>(() -> {
                boolean success = false;
                int tryTime = 1;
                do {
                    try (XConnection conn = (XConnection) (PhysicalBackfillUtils.getXConnectionForStorage(
                        targetDbAndGroup.getKey(), v.getKey(), v.getValue(), userInfo.getKey(), userInfo.getValue(),
                        -1))) {
                        long writeSize = conn.execTransferFile(builder);
                        if (writeSize != transferFileData.getBufferLen()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                                "the length of buffer write to target file is different from read from source file");
                        }
                        success = true;
                    } catch (SQLException ex) {
                        if (tryTime > MAX_RETRY) {
                            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, ex);
                        }
                        tryTime++;
                    }
                } while (!success);
            }, null);
            futures.add(task);
            ec.getExecutorService().submit(ec.getSchemaName(), ec.getTraceId(), AsyncTask.build(task));
        });
        waitApplyFinish(futures);

    }

    private void waitApplyFinish(List<Future> futures) {
        for (Future future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                futures.forEach(f -> {
                    try {
                        f.cancel(true);
                    } catch (Throwable ignore) {
                    }
                });

                throw GeneralUtil.nestedException(e);
            }
        }

        futures.clear();
    }
}
