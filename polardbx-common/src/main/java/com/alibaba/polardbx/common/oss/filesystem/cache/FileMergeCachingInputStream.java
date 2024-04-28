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

package com.alibaba.polardbx.common.oss.filesystem.cache;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public final class
FileMergeCachingInputStream
    extends FSDataInputStream {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileMergeCachingInputStream.class);
    private static final String LOG_FORMAT = "%s: [ %s, %s ] size: %s time: %s file: %s";

    private final FSDataInputStream inputStream;
    private final CacheManager cacheManager;
    private final Path path;
    private final CacheQuota cacheQuota;
    private final boolean cacheValidationEnabled;

    public FileMergeCachingInputStream(
        FSDataInputStream inputStream,
        CacheManager cacheManager,
        Path path,
        CacheQuota cacheQuota,
        boolean cacheValidationEnabled) {
        super(inputStream);
        this.inputStream = requireNonNull(inputStream, "inputStream is null");
        this.cacheManager = requireNonNull(cacheManager, "cacheManager is null");
        this.path = requireNonNull(path, "path is null");
        this.cacheQuota = requireNonNull(cacheQuota, "cacheQuota is null");
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public void readFully(long position, byte[] buffer)
        throws IOException {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
        throws IOException {
        long s = System.currentTimeMillis();
        FileReadRequest key = new FileReadRequest(path, position, length);
        switch (cacheManager.get(key, buffer, offset, cacheQuota)) {
        case HIT_HOT_CACHE:
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                    LOG_FORMAT,
                    "HIT_HOT_CACHE",
                    position,
                    position + length,
                    length,
                    System.currentTimeMillis() - s,
                    key.getPath()
                ));
            }

            return;
        case HIT:
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                    LOG_FORMAT,
                    "HIT_CACHE",
                    position,
                    position + length,
                    length,
                    System.currentTimeMillis() - s,
                    key.getPath()
                ));
            }

            break;
        case MISS:
            inputStream.readFully(position, buffer, offset, length);
            cacheManager.put(key, wrappedBuffer(buffer, offset, length), cacheQuota);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                    LOG_FORMAT,
                    "OSS_READ",
                    position,
                    position + length,
                    length,
                    System.currentTimeMillis() - s,
                    key.getPath()
                ));
            }

            return;
        case CACHE_QUOTA_EXCEED:
        case CACHE_IS_UNAVAILABLE:
            inputStream.readFully(position, buffer, offset, length);

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(String.format(
                    LOG_FORMAT,
                    "CACHE_EXCEED",
                    position,
                    position + length,
                    length,
                    System.currentTimeMillis() - s,
                    key.getPath()
                ));
            }

            return;
        }

        if (cacheValidationEnabled) {
            byte[] validationBuffer = new byte[length];
            inputStream.readFully(position, validationBuffer, 0, length);
            for (int i = 0; i < length; i++) {
                verify(buffer[offset + i] == validationBuffer[i], "corrupted buffer at position " + i);
            }
        }
    }
}
