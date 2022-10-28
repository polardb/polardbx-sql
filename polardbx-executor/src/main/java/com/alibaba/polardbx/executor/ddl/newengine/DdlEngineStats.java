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

package com.alibaba.polardbx.executor.ddl.newengine;

import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Statistics for ddl engine
 *
 * @author moyi
 * @since 2021/11
 */
public class DdlEngineStats {

    private final static Map<String, Metric> metrics = new HashMap<>();

    public static Metric METRIC_DDL_JOBS_TOTAL = new Metric("DDL_JOBS_TOTAL");
    public static Metric METRIC_DDL_JOBS_FINISHED = new Metric("DDL_JOBS_FINISHED");
    public static Metric METRIC_DDL_EXECUTION_TIME_MILLIS = new Metric("DDL_EXECUTION_TIME_MILLIS");
    public static Metric METRIC_DDL_TASK_TOTAL = new Metric("DDL_TASK_TOTAL");
    public static Metric METRIC_DDL_TASK_FINISHED = new Metric("DDL_TASK_FINISHED");
    public static Metric METRIC_DDL_TASK_FAILED = new Metric("DDL_TASK_FAILED");

    public static Metric METRIC_CHECKER_ROWS_FINISHED = new Metric("CHECKER_ROWS_FINISHED");
    public static Metric METRIC_CHECKER_TIME_MILLIS = new Metric("CHECKER_TIME_MILLIS");

    public static Metric METRIC_BACKFILL_ROWS_FINISHED = new Metric("BACKFILL_ROWS_FINISHED");
    public static Metric METRIC_BACKFILL_ROWS_SPEED = new Metric("BACKFILL_ROWS_SPEED");
    public static Metric METRIC_BACKFILL_TIME_MILLIS = new Metric("BACKFILL_TIME_MILLIS");

    public static Metric METRIC_BACKFILL_TASK_TOTAL = new Metric("BACKFILL_TASK_TOTAL");
    public static Metric METRIC_BACKFILL_TASK_FINISHED = new Metric("BACKFILL_TASK_FINISHED");
    public static Metric METRIC_BACKFILL_TASK_FAILED = new Metric("BACKFILL_TASK_FAILED");

    public static Metric METRIC_BACKFILL_PARALLELISM = new Metric("BACKFILL_PARALLELISM");

    public static Metric METRIC_THROTTLE_RATE =
        new Metric.DelegatorMetric("THROTTLE_RATE", (x) -> Throttle.getTotalThrottleRate());

    public static Map<String, Metric> getAllMetrics() {
        return metrics;
    }

    public static void updateMetric(String name, long delta) {
        metrics.get(name).getValue().addAndGet(delta);
    }

    public static long getMetric(String name) {
        return metrics.get(name).getValue().get();
    }

    @Data
    public static class Metric {
        String name;
        AtomicLong value;

        public Metric(String name) {
            this.name = name;
            this.value = new AtomicLong();
            metrics.put(name, this);
        }

        public Metric(String name, long value) {
            this.name = name;
            this.value = new AtomicLong(value);
        }

        public static Metric fromMap(Map<String, Object> map) {
            String name = DataTypes.StringType.convertFrom(map.get("METRIC"));
            long value = DataTypes.LongType.convertFrom(map.get("VALUE"));
            return new Metric(name, value);
        }

        public static ArrayResultCursor buildCursor() {
            ArrayResultCursor cursor = new ArrayResultCursor("METRICS");
            cursor.addColumn("METRIC", DataTypes.VarcharType);
            cursor.addColumn("VALUE", DataTypes.LongType);
            cursor.initMeta();
            return cursor;
        }

        public static Object[] toRow(Map<String, Object> map) {
            return new Object[] {map.get("METRIC"), map.get("VALUE")};
        }

        public Object[] toRow() {
            return new Object[] {name, value.get()};
        }

        public void update(long delta) {
            this.value.addAndGet(delta);
        }

        public void set(long value) {
            this.value.set(value);
        }

        public Metric merge(Metric rhs) {
            assert name.equals(rhs.getName());
            long value = getValue().get() + rhs.getValue().get();
            return new Metric(getName(), value);
        }

        /**
         * Delegate the metric to the other component
         */
        public static class DelegatorMetric extends Metric {

            private Function<Void, Long> delegator;

            public DelegatorMetric(String name, Function<Void, Long> delegator) {
                super(name);
                this.delegator = delegator;
            }

            @Override
            public Object[] toRow() {
                return new Object[] {name, delegator.apply(null)};
            }

            @Override
            public void update(long delta) {
                throw new AssertionError("non-updatable");
            }

            @Override
            public void set(long value) {
                throw new AssertionError("non-updatable");
            }

        }
    }

}
