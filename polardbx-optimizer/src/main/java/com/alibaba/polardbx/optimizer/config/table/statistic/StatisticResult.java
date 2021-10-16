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

    public final static StatisticResult EMPTY = build().setValue(-1L);

    private Object value;
    private StatisticResultSource source;

    public static StatisticResult build() {
        return new StatisticResult();
    }

    public static StatisticResult build(StatisticResultSource source) {
        return new StatisticResult().setSource(source);
    }

    public Object getValue() {
        return value;
    }

    public long getLongValue() {
        return ((Number) value).longValue();
    }

    public StatisticResult setValue(Object value) {
        this.value = value;
        return this;
    }

    public StatisticResultSource getSource() {
        return source;
    }

    public StatisticResult setSource(StatisticResultSource source) {
        this.source = source;
        return this;
    }
}
