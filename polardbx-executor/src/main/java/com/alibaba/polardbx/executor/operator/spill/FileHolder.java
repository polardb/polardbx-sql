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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

@ThreadSafe
final class FileHolder implements Closeable {
    private final FileCleaner fileCleaner;
    private final Path filePath;
    private final int threadId;

    @GuardedBy("this")
    private boolean deleted;

    public FileHolder(Path filePath, FileCleaner fileCleaner) {
        this(filePath, fileCleaner, -1);
    }

    public FileHolder(Path filePath, FileCleaner fileCleaner, int threadId) {
        this.filePath = requireNonNull(filePath, "filePath is null");
        this.fileCleaner = requireNonNull(fileCleaner, "fileCleaner is null");
        this.threadId = threadId;
    }

    public Path getFilePath() {
        return filePath;
    }

    public synchronized OutputStream newOutputStream(OpenOption... options)
        throws IOException {
        checkState(!deleted, "File already deleted");
        return Files.newOutputStream(filePath, options);
    }

    public synchronized InputStream newInputStream(OpenOption... options)
        throws IOException {
        checkState(!deleted, "File already deleted");
        return Files.newInputStream(filePath, options);
    }

    @Override
    public synchronized void close() {
        if (deleted) {
            return;
        }
        deleted = true;
        if (fileCleaner != null) {
            fileCleaner.recycleFile(this);
        } else {
            doClean();
        }
    }

    public synchronized void doClean() {
        try {
            if (Files.exists(filePath)) {
                Files.delete(filePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public int getThreadId() {
        return threadId;
    }
}
