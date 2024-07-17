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
package com.alibaba.polardbx.optimizer.spill;

import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.properties.SpillConfig;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.MppConfig;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.Closeable;

public class QuerySpillSpaceMonitor extends SpillSpaceMonitor {

    private String tag;

    private final Closer closer = Closer.create();

    private long spillCnt;

    public QuerySpillSpaceMonitor(String tag) {
        this.tag = tag;
    }

    @Override
    public synchronized void updateBytes(long bytes) {
        super.updateBytes(bytes);
        SpillSpaceManager.getInstance().updateBytes(bytes);
    }

    @Override
    public long getCurrentMaxSpillBytes() {
        return FileConfig.getInstance().getSpillConfig().getMaxQuerySpillSpaceThreshold().toBytes();
    }

    @Override
    public String tag() {
        return tag;
    }

    public <C extends Closeable> C register(@Nullable C closeable) {
        if (closeable != null) {
            spillCnt++;
            closer.register(closeable);
        }
        return closeable;
    }

    public long getSpillCnt() {
        return spillCnt;
    }

    @Override
    public void close() {
        try {
            closer.close();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_SPILL, e, "Failed to close spiller");
        }
        super.close();
    }
}
