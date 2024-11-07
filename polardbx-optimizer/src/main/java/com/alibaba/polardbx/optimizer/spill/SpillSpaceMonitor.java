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

public abstract class SpillSpaceMonitor implements SpillMonitor {

    protected long currentBytes;

    protected boolean isSpillManager = false;

    @Override
    public synchronized void updateBytes(long bytes) {
        if (bytes > 0) {
            long theMaxBytes = getCurrentMaxSpillBytes();
            if ((currentBytes + bytes) >= theMaxBytes) {
                throw new RuntimeException(
                    String.format("%s exceeded the spill limit of %s bytes, when trying to get %s bytes",
                        isSpillManager ? "Spill Manager" : "Query", theMaxBytes, bytes));
            }
            currentBytes += bytes;
        } else {
            currentBytes += bytes;
        }
    }

    public abstract long getCurrentMaxSpillBytes();
}
