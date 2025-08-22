/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.server;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.ccl.common.RescheduleTask;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class SwitchoverManager {
    private static final Logger logger = LoggerFactory.getLogger(SwitchoverManager.class);

    public static final Queue<RescheduleTask> rescheduleTaskQueue = new ConcurrentLinkedQueue<>();
    private static final Thread worker;

    static {
        worker = new Thread(() -> {
            while (true) {
                rescheduleAll();

                // sleep
                try {
                    final int interval = DynamicConfig.getInstance().getSwitchoverCheckIntervalMillis();
                    Thread.sleep(interval);
                } catch (Throwable e) {
                    logger.error("Switchover-manager sleep error", e);
                }
            }
        }, "Switchover-manager");
        worker.setDaemon(true);
        worker.start();
    }

    public static void rescheduleAll() {
        try {
            final List<RescheduleTask> tasks = new ArrayList<>(rescheduleTaskQueue.size());
            while (!rescheduleTaskQueue.isEmpty()) {
                final RescheduleTask task = rescheduleTaskQueue.poll();
                if (task == null) {
                    continue;
                }
                tasks.add(task);
            }

            for (final RescheduleTask task : tasks) {
                if (!task.isSwitchoverReschedule()) {
                    logger.error("Switchover-manager rescheduleAll error, unknown task");
                } else {
                    task.getReschedulable().reschedule(null);
                }
            }
        } catch (Throwable e) {
            logger.error("Switchover-manager rescheduleAll error", e);
        }
    }
}
