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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.operator.spill;

import com.alibaba.polardbx.common.datatype.UInt64;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.NumberType;
import com.alibaba.polardbx.optimizer.core.expression.bean.EnumValue;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.spill.SpillMonitor;
import io.airlift.slice.Slice;
import org.apache.calcite.sql.OutFileParams;

import javax.annotation.concurrent.GuardedBy;
import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.alibaba.polardbx.executor.operator.spill.SingleStreamSpiller.NOT_BLOCKED;
import static java.util.Objects.requireNonNull;

public class AsyncFileBufferWriter {
    private static final Logger logger = LoggerFactory.getLogger(AsyncFileBufferWriter.class);
    private final FileHolder id;
    private final OutFileParams outFileParams;
    private final WriteQueue requetsQueue;

    private final String nullFormat;
    private final List<ColumnMeta> columns;
    private final Object lock = new Object();
    private final Set<Integer> notNumberTypeIndex = new HashSet<>();

    @GuardedBy("lock")
    private SettableFuture<?> writeFuture = null;
    @GuardedBy("lock")
    private IOException iex;
    /**
     * closed flag:
     * true -> not accecpt new IO request, wait for all pendingRequest to be done
     */
    private volatile boolean closed;
    private BufferedOutputStream outToFile;

    private Runnable onClose;

    private final ArrayList<IORequest.WriteIORequest> pendingRequest = new ArrayList<>();

    private SpillMonitor spillMonitor;

    public AsyncFileBufferWriter(FileHolder id, List<ColumnMeta> columns,
                                 OutFileParams outFileParams, WriteQueue requestQueue,
                                 Runnable noThrowableOnClose, SpillMonitor spillMonitor)
        throws IOException {
        this.id = requireNonNull(id, "FileId is null");
        this.columns = columns;
        this.outFileParams = requireNonNull(outFileParams, "OutFileParams is null");
        this.requetsQueue = requireNonNull(requestQueue, "runningRequests is null");
        HashMap<String, Object> cmdObjects = new HashMap<>();
        ParamManager paramManager = new ParamManager(cmdObjects);
        long selectIntoBufferSize = paramManager.getLong(ConnectionParams.SELECT_INTO_OUTFILE_BUFFER_SIZE);
        this.outToFile =
            new BufferedOutputStream(new FileOutputStream(id.getFilePath().toFile()), (int) selectIntoBufferSize);
        this.onClose = requireNonNull(noThrowableOnClose);
        this.spillMonitor = spillMonitor;
        this.nullFormat = outFileParams.getFieldEscape() == null ?
            "NULL" : (char) ((byte) outFileParams.getFieldEscape()) + "N";
        for (int i = 0; i < columns.size(); ++i) {
            if (!(columns.get(i).getDataType() instanceof NumberType)) {
                notNumberTypeIndex.add(i);
            }
        }
    }

    public void flush() {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (outToFile != null) {
                try {
                    outToFile.flush();
                } catch (IOException e) {
                    logger.error("Flush to disk failed!");
                    throw new TddlRuntimeException(ErrorCode.ERR_DATA_OUTPUT, "Flush to disk failed!");
                }
            }
        }
    }

    public void close(boolean errorClose) throws IOException {
        synchronized (lock) {
            if (closed) {
                return;
            }
            try {
                if (errorClose) {
                    if (pendingRequest.size() > 0) {
                        for (IORequest.WriteIORequest request : pendingRequest) {
                            request.close();
                        }
                    }
                }
                closed = true;
                checkIOException();
                while (pendingRequest.size() > 0) {
                    try {
                        this.lock.wait(100);
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                    checkIOException();
                }
            } finally {
                this.onClose.run();
                if (outToFile != null) {
                    outToFile.close();
                    outToFile = null;
                }
            }
        }
    }

    public ListenableFuture<?> writeRows(Iterator<Row> rows, long size) throws IOException {
        PagesWriteRequest request = new PagesWriteRequest(rows, size);
        // addRequest must return NOT_BLOCKED
        addRequest(request, size);
        return request.getWriteFuture();
    }

    private ListenableFuture addRequest(IORequest.WriteIORequest request, long bytes) throws IOException {
        synchronized (lock) {
            checkIOException();
            if (closed) {
                request.close();
                throw new IOException(
                    "The FileChannel is already closed, " + id.getFilePath().toFile().getAbsolutePath());
            }
            pendingRequest.add(request);
            if (requetsQueue.addRequest(request, bytes)) {
                return addListener();
            } else {
                return NOT_BLOCKED;
            }
        }
    }

    private void checkIOException() {
        if (iex != null) {
            throw new RuntimeException(iex);
        }
    }

    private void notifyListener(IOException ioe) {
        SettableFuture toSetFuture = null;
        synchronized (lock) {
            if (writeFuture != null) {
                toSetFuture = writeFuture;
                writeFuture = null;
            }
        }
        if (toSetFuture != null) {
            toSetFuture.set(ioe);
        }
    }

    private void handleFinish(IORequest.WriteIORequest request, IOException e, boolean nextRound, boolean needNotify) {
        synchronized (lock) {
            boolean notified = false;
            try {
                if (iex == null && e != null) {
                    iex = e;
                    notifyListener(iex);
                    notified = true;
                }

                if (!notified && needNotify) {
                    notifyListener(null);
                    notified = true;
                }
            } finally {
                if (!nextRound || e != null || iex != null) {
                    request.close();
                    pendingRequest.remove(request);
                    if (this.pendingRequest.size() == 0) {
                        if (!notified) {
                            notifyListener(null);
                        }

                        if (closed) {
                            lock.notify();
                        }
                    }
                    // 目前不会走到这里来，因为每个IORequest会循环完
                    // 问题是如果单个IORequest执行时间过长，会导致线程一直被占用，优势在于IORequest中row的顺序与文件中相同
                } else if (request != null && !request.isClosed() && nextRound && iex == null) {
                    this.requetsQueue.addRequest(request, request.getBytes());
                } else if (request.isClosed()) {
                    pendingRequest.remove(request);
                    if (this.pendingRequest.size() == 0) {
                        if (!notified) {
                            notifyListener(null);
                        }

                        if (closed) {
                            lock.notify();
                        }
                    }
                }
            }
        }
    }

    private ListenableFuture addListener() {
        ListenableFuture returnFuture = null;
        synchronized (lock) {
            checkIOException();
            if (null == outFileParams) {
                checkState(writeFuture == null, "writeFuture already set");
            }
            writeFuture = SettableFuture.create();
            returnFuture = writeFuture;
        }
        return returnFuture;
    }

    private class PagesWriteRequest implements IORequest.WriteIORequest {
        private Iterator<Row> rowIterator;
        private ListenableFuture<?> writeFuture;
        private boolean closed;
        private final Object requestLock = new Object();
        private long size;

        public PagesWriteRequest(Iterator<Row> rowIterator, long size) {
            this.rowIterator = rowIterator;
            writeFuture = addListener();
            this.size = size;
        }

        public ListenableFuture getWriteFuture() {
            return writeFuture;
        }

        @Override
        public void requestDone(IOException ioe, boolean needNotify) {
            boolean nextRound = false;
            synchronized (requestLock) {
                try {
                    nextRound = rowIterator.hasNext();
                } catch (Exception e) {
                    if (ioe == null) {
                        ioe = new IOException(e);
                    }
                }
            }
            handleFinish(this, ioe, nextRound, needNotify);
        }

        @Override
        public long write() throws IOException {
            synchronized (requestLock) {
                if (closed || !rowIterator.hasNext()) {
                    return 0;
                }
                Row nextRow;
                long writeSizeInBytes = 0;
                while (rowIterator.hasNext()) {
                    nextRow = rowIterator.next();
                    writeSizeInBytes += writeRow(nextRow);
                }
                spillMonitor.updateBytes(writeSizeInBytes);
                return writeSizeInBytes;
            }
        }

        @Override
        public long getBytes() {
            return size;
        }

        @Override
        public void close() {
            synchronized (requestLock) {
                if (closed) {
                    return;
                }
                while (rowIterator.hasNext()) {
                    rowIterator.next();
                }
                closed = true;
            }
        }

        @Override
        public boolean isClosed() {
            return closed;
        }
    }

    private long writeRow(Row row) {
        StringBuilder lineBuilder = new StringBuilder((int) row.estimateSize());
        long spillSize = 0;
        lineBuilder.append(outFileParams.getLinesStartingBy());
        for (int columnIndex = 0; columnIndex < columns.size(); ++columnIndex) {
            if (columnIndex != 0) {
                lineBuilder.append(outFileParams.getFieldTerminatedBy());
            }

            if (row.getObject(columnIndex) == null) {
                lineBuilder.append(nullFormat);
                continue;
            }
            boolean enclose = false;
            if (outFileParams.getFieldEnclose() != null &&
                (!outFileParams.isFieldEnclosedOptionally() || notNumberTypeIndex.contains(columnIndex))) {
                enclose = true;
                lineBuilder.append((char) ((byte) outFileParams.getFieldEnclose()));
            }
            lineBuilder.append(processEscape(row.getObject(columnIndex)));
            if (enclose) {
                lineBuilder.append((char) ((byte) outFileParams.getFieldEnclose()));
            }
        }
        lineBuilder.append(outFileParams.getLineTerminatedBy());
        byte[] lineBytes = new byte[0];
        try {
            lineBytes = lineBuilder.toString().getBytes(outFileParams.getCharset());
            outToFile.write(lineBytes);
        } catch (UnsupportedEncodingException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CHARACTER_NOT_SUPPORT, "Character set is not support!");
        } catch (IOException e) {
            logger.error("[select into outfile] Failed to write one line", e);
        }
        spillSize += lineBytes.length;
        return spillSize;
    }

    private String processEscape(Object object) {
        if (object instanceof Number) {
            return object.toString();
        }
        byte[] bytes;
        if (object instanceof EnumValue) {
            bytes = ((EnumValue) object).getValue().getBytes();
        } else if (object instanceof UInt64) {
            bytes = object.toString().getBytes();
        } else if (object instanceof Slice) {
            bytes = ((Slice) object).getBytes();
        } else if (object instanceof byte[]) {
            bytes = (byte[]) object;
        } else if (object instanceof String || object instanceof Timestamp
            || object instanceof Time || object instanceof Date) {
            bytes = object.toString().getBytes();
            // 其他类型暂时没遇到，如果遇到了就暂时直接抛错吧，然后来这里加上去就好了
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DATATYPE_NOT_SUPPORT,
                String.format("[select into outfile] Type: %s not support!", object.getClass()));
        }
        bytes = processEscapeUnderString(bytes);
        return new String(bytes);
    }

    private byte[] processEscapeUnderString(byte[] bytes) {
        ArrayList<Byte> bytesAfterProcess = new ArrayList<>(bytes.length);
        for (Byte b : bytes) {
            boolean escape = false;
            if (b == 0) {
                b = '0';
                escape = true;
            } else if (b.equals(outFileParams.getFieldEscape()) || b.equals(outFileParams.getFieldEnclose())) {
                escape = true;
            } else if (outFileParams.getFieldEnclose() == null
                && outFileParams.getFieldTerminatedBy().getBytes().length > 0
                && b == outFileParams.getFieldTerminatedBy().getBytes()[0]) {
                escape = true;
            } else if (outFileParams.getLineTerminatedBy().getBytes().length > 0
                && outFileParams.getLineTerminatedBy().getBytes()[0] == b) {
                escape = true;
            }
            if (escape) {
                bytesAfterProcess.add(outFileParams.getFieldEscape());
            }
            bytesAfterProcess.add(b);
        }
        return Bytes.toArray(bytesAfterProcess);
    }
}