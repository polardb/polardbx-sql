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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.rpc.cdc.BinaryLog;
import com.alibaba.polardbx.rpc.cdc.BinlogEvent;
import com.alibaba.polardbx.rpc.cdc.FullBinaryLog;
import io.grpc.Channel;
import io.grpc.ManagedChannel;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class CdcResultCursor extends ArrayResultCursor {

    private Function<Object, Row> function = obj -> {
        if (obj instanceof BinaryLog) {
            return new ArrayRow(getMeta(),
                new Object[] {((BinaryLog) obj).getLogName(), ((BinaryLog) obj).getFileSize()});
        } else if (obj instanceof BinlogEvent) {
            return new ArrayRow(getMeta(),
                new Object[] {
                    ((BinlogEvent) obj).getLogName(), ((BinlogEvent) obj).getPos(),
                    ((BinlogEvent) obj).getEventType(), ((BinlogEvent) obj).getServerId(),
                    ((BinlogEvent) obj).getEndLogPos(),
                    ((BinlogEvent) obj).getInfo()});
        } else if (obj instanceof FullBinaryLog) {
            return new ArrayRow(getMeta(), new Object[] {
                ((FullBinaryLog) obj).getLogName(),
                ((FullBinaryLog) obj).getFileSize(),
                ((FullBinaryLog) obj).getCreatedTime(),
                ((FullBinaryLog) obj).getLastModifyTime(),
                ((FullBinaryLog) obj).getFirstEventTime(),
                ((FullBinaryLog) obj).getLastEventTime(),
                ((FullBinaryLog) obj).getLastTso(),
                ((FullBinaryLog) obj).getUploadStatus(),
                ((FullBinaryLog) obj).getFileLocation(),
                ((FullBinaryLog) obj).getExt()
            });
        }
        return null;
    };

    private Iterator<?> iterator;

    private Channel channel;

    public CdcResultCursor(String tableName, Iterator<?> iterator) {
        super(tableName);
        this.iterator = iterator;
    }

    public CdcResultCursor(String tableName, Iterator<?> iterator, Channel channel) {
        super(tableName);
        this.iterator = iterator;
        this.channel = channel;
    }

    @Override
    public Row doNext() {
        if (iterator.hasNext()) {
            return function.apply(iterator.next());
        }
        return null;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (channel instanceof ManagedChannel) {
            ((ManagedChannel) channel).shutdownNow();
        }
        return super.doClose(exceptions);
    }
}
