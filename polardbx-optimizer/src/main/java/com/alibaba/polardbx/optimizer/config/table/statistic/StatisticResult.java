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

package com.alibaba.polardbx.optimizer.config.table.statistic;

import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;

/**
 * statistic result. value&source
 *
 * @author jilong.ljl
 */
public class StatisticResult {
    private Object value;
    private StatisticResultSource source;
    private StatisticTrace trace;

    public static StatisticResult build() {
        return new StatisticResult();
    }

    public static StatisticResult build(StatisticResultSource source) {
        return new StatisticResult().setSource(source);
    }

    public Object getValue() {
        return value;
    }

    public boolean getBooleanValue() {
        return ((Boolean) value).booleanValue();
    }

    public long getLongValue() {
        return ((Number) value).longValue();
    }

    public StatisticResult setValue(Object value, StatisticTrace trace) {
        this.value = value;
        this.trace = trace;
        return this;
    }

    public StatisticResultSource getSource() {
        return source;
    }

    public StatisticResult setSource(StatisticResultSource source) {
        this.source = source;
        return this;
    }

    @Override
    public String toString() {
        if (this.getSource() == StatisticResultSource.NULL) {
            return "empty";
        }
        return value + ":" + source.name();
    }

    public StatisticTrace getTrace() {
        return trace;
    }

    public void setTrace(StatisticTrace trace) {
        this.trace = trace;
    }
}
