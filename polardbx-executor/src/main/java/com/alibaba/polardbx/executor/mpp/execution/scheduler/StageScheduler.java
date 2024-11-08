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
package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import java.io.Closeable;
import java.util.List;

public interface StageScheduler extends Closeable {
    /**
     * Schedules as much work as possible without blocking.
     * The schedule results is a hint to the query scheduler if and
     * when the stage scheduler should be invoked again.  It is
     * important to note that this is only a hint and the query
     * scheduler may call the schedule method at any time.
     */
    ScheduleResult schedule();

    int getTaskNum();

    int requireChildOutputNum();

    int getDriverParallelism();

    @Override
    default void close() {
    }

    default List<Integer> getPrunePartitions() {
        return null;
    }
}
