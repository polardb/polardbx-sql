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

package com.alibaba.polardbx.executor.balancer;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.balancer.action.BalanceAction;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Main thread of balancer.
 * Keep running whether enable or not
 */
public class BalancerThread implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(BalancerThread.class);

    /**
     * Interval of balancer
     */
    private static final int SLEEP_INTERVAL = 5;

    private void sleep() {
        try {
            Thread.sleep(SLEEP_INTERVAL * 1000L);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * Whether now is in the time window
     */
    private boolean withinWindow(Pair<DateTime, DateTime> window) {
        DateTime start = window.getKey();
        DateTime end = window.getValue();
        DateTime now = DateTime.now();

        int nowSecond = now.getSecondOfDay();
        int startSecond = now.secondOfDay().getMinimumValue();
        int endSecond = now.secondOfDay().getMaximumValue();

        if (start != null) {
            startSecond = start.getSecondOfDay();
        }
        if (end != null) {
            endSecond = end.getSecondOfDay();
        }

        return startSecond <= nowSecond && nowSecond <= endSecond;
    }

    @Override
    public void run() {
        LOG.info("SchedulerThread started");

        final Balancer balancer = Balancer.getInstance();
        final String defaultSchema = DefaultDbSchema.NAME;
        // NOTE: it's an incomplete context, used for background thread
        ExecutionContext ec = new ExecutionContext();
        ec.setSchemaName(defaultSchema);

        while (true) {
            Pair<DateTime, DateTime> window = balancer.getBalancerWindow();

            boolean enable = balancer.isEnabled() &&
                ExecUtils.hasLeadership(defaultSchema) &&
                withinWindow(window);

            if (enable) {
                BalanceOptions options = BalanceOptions.withBackground();

                try {
                    LOG.debug("balancer task start");
                    List<BalanceAction> actions = balancer.rebalanceCluster(ec, options);
                    LOG.debug("executed background balance actions: " + actions);
                } catch (Throwable e) {
                    LOG.error("background rebalanceCluster failed: " + e);
                    e.printStackTrace();
                }
            }

            sleep();
        }
    }
}
