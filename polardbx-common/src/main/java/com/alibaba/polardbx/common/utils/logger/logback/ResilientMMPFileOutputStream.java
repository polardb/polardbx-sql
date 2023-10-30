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

package com.alibaba.polardbx.common.utils.logger.logback;

import ch.qos.logback.core.Context;
import ch.qos.logback.core.recovery.RecoveryCoordinator;
import ch.qos.logback.core.recovery.ResilientOutputStreamBase;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.InfoStatus;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusManager;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 模仿ResilientFileOutputStream
 */
public class ResilientMMPFileOutputStream extends OutputStream {
    static final int STATUS_COUNT_LIMIT = 8;
    private int noContextWarning = 0;
    private int statusCount = 0;
    private Context context;
    private RecoveryCoordinator recoveryCoordinator;
    protected OutputStream os;
    protected boolean presumedClean = true;
    private File file;

    public ResilientMMPFileOutputStream(File file, boolean append) throws IOException {
        this.file = file;
        this.os = new MMPFileOutputStream(file, append);
        this.presumedClean = true;
    }

    String getDescription() {
        return "file [" + this.file + "]";
    }

    OutputStream openNewOutputStream() throws IOException {
        return new MMPFileOutputStream(file, true);
    }

    private boolean isPresumedInError() {
        return this.recoveryCoordinator != null && !this.presumedClean;
    }

    public void write(byte[] b, int off, int len) {
        if (this.isPresumedInError()) {
            if (!this.recoveryCoordinator.isTooSoon()) {
                this.attemptRecovery();
            }

        } else {
            try {
                this.os.write(b, off, len);
                this.postSuccessfulWrite();
            } catch (IOException var5) {
                this.postIOFailure(var5);
            }

        }
    }

    public void write(int b) {
        if (this.isPresumedInError()) {
            if (!this.recoveryCoordinator.isTooSoon()) {
                this.attemptRecovery();
            }

        } else {
            try {
                this.os.write(b);
                this.postSuccessfulWrite();
            } catch (IOException var3) {
                this.postIOFailure(var3);
            }

        }
    }

    public void flush() {
        if (this.os != null) {
            try {
                this.os.flush();
                this.postSuccessfulWrite();
            } catch (IOException var2) {
                this.postIOFailure(var2);
            }
        }

    }

    private void postSuccessfulWrite() {
        if (this.recoveryCoordinator != null) {
            this.recoveryCoordinator = null;
            this.statusCount = 0;
            this.addStatus(new InfoStatus("Recovered from IO failure on " + this.getDescription(), this));
        }

    }

    public void postIOFailure(IOException e) {
        this.addStatusIfCountNotOverLimit(
            new ErrorStatus("IO failure while writing to " + this.getDescription(), this, e));
        this.presumedClean = false;
        if (this.recoveryCoordinator == null) {
            this.recoveryCoordinator = new RecoveryCoordinator();
        }

    }

    public void close() throws IOException {
        if (this.os != null) {
            this.os.close();
        }

    }

    void attemptRecovery() {
        try {
            this.close();
        } catch (IOException var3) {
        }

        this.addStatusIfCountNotOverLimit(
            new InfoStatus("Attempting to recover from IO failure on " + this.getDescription(), this));

        try {
            this.os = this.openNewOutputStream();
            this.presumedClean = true;
        } catch (IOException var2) {
            this.addStatusIfCountNotOverLimit(new ErrorStatus("Failed to open " + this.getDescription(), this, var2));
        }

    }

    void addStatusIfCountNotOverLimit(Status s) {
        ++this.statusCount;
        if (this.statusCount < 8) {
            this.addStatus(s);
        }

        if (this.statusCount == 8) {
            this.addStatus(s);
            this.addStatus(new InfoStatus("Will supress future messages regarding " + this.getDescription(), this));
        }

    }

    public void addStatus(Status status) {
        if (this.context == null) {
            if (this.noContextWarning++ == 0) {
                System.out.println("LOGBACK: No context given for " + this);
            }

        } else {
            StatusManager sm = this.context.getStatusManager();
            if (sm != null) {
                sm.add(status);
            }

        }
    }

    public Context getContext() {
        return this.context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    /**
     * 线程不安全
     */
    private static class MMPFileOutputStream extends OutputStream {

        private RandomAccessFile raf;

        private MappedByteBuffer buffer;

        private static final int DEFAULT_REGION_SIZE = 32 * 1024 * 1024;

        private long mappingOffset;

        private int regionLength;

        public MMPFileOutputStream(File file, boolean append) throws IOException {
            this.raf = new RandomAccessFile(file, "rw");
            this.regionLength = DEFAULT_REGION_SIZE;

            if (append && raf.length() != 0) {
                this.mappingOffset = raf.length() - checkEmptySize();
                ;
                this.buffer = mmap(raf, mappingOffset, 2 * regionLength - mappingOffset % regionLength);
            } else {
                this.mappingOffset = 0;
                this.buffer = mmap(raf, mappingOffset, regionLength);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    close();
                } catch (IOException ignored) {
                }
            }));
        }

        /**
         * 查看日志文件中空洞的大小
         * 由于mmap，如果jvm不正常关闭，则日志文件最后可能就出现空洞
         */
        private int checkEmptySize() throws IOException {
            int emptySize = 0;
            for (long end = raf.length(); end > 0; end -= regionLength) {
                MappedByteBuffer buf = mmap(raf, Math.max(0, end - regionLength), Math.min(end, regionLength));
                for (int i = buf.capacity() - 1; i >= 0; i--, emptySize++) {
                    //对于ascii值为0属于正常日志的情况，以下逻辑可能出现错误；需要使用魔数或者校验值等方式
                    if (buf.get(i) != 0) {
                        unsafeUnmap(buf);
                        return emptySize;
                    }
                }
                unsafeUnmap(buf);
            }
            return emptySize;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b});
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            while (len > buffer.remaining()) {
                final int chunk = buffer.remaining();
                buffer.put(b, off, chunk);
                off += chunk;
                len -= chunk;
                remap();
            }
            buffer.put(b, off, len);
        }

        @Override
        public void flush() throws IOException {
            //do nothing，data already be written to kernel memory
        }

        @Override
        public void close() throws IOException {
            long length = mappingOffset + buffer.position();
            unsafeUnmap(buffer);
            raf.setLength(length);
            raf.close();
        }

        private void remap() throws IOException {
            long offset = this.mappingOffset + buffer.position();
            int length = buffer.remaining() + regionLength;
            unsafeUnmap(buffer);
            raf.setLength(raf.length() + regionLength);
            this.buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, offset, length);
            this.mappingOffset = offset;
        }

        private static void unsafeUnmap(MappedByteBuffer mbb) {
            ((DirectBuffer) mbb).cleaner().clean();
            ;
        }

        private static MappedByteBuffer mmap(RandomAccessFile raf, long offset, long length) throws IOException {
            return raf.getChannel().map(FileChannel.MapMode.READ_WRITE, offset, length);
        }
    }

}
