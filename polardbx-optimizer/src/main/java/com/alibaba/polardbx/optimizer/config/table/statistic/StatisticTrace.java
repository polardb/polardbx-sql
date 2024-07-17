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

import com.alibaba.polardbx.common.utils.time.old.DateUtils;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.digest;
import static com.alibaba.polardbx.optimizer.utils.ExplainUtils.getTimeStamp;

/**
 * @author fangwu
 */
public class StatisticTrace {
    /**
     * value digest max size
     */
    public static final int MAX_DIGEST_SIZE = 50;
    public static final int MAX_TRACE_CHILDREN_SIZE = 5;

    // fields
    /**
     * like schema:table:columns
     */
    private final String catalogTarget;

    /**
     * statistic service action, like getFrequency
     */
    private final String action;

    /**
     * statistic cal value
     */
    private final Object value;

    /**
     * statistic cal desc
     */
    private final String desc;

    /**
     * statistic source
     */
    private final StatisticResultSource source;

    /**
     * statistic data last modify time
     */
    private final long lastModifyTime;

    /**
     * tree struct
     */
    private final List<StatisticTrace> childStack = Lists.newArrayList();

    public StatisticTrace(String catalogTarget, String action, Object value, String desc, StatisticResultSource source,
                          long lastModifyTime) {
        this.catalogTarget = catalogTarget;
        this.action = action;
        this.value = value;
        this.desc = desc;
        this.source = source;
        this.lastModifyTime = (source == StatisticResultSource.CACHE_LINE) ? lastModifyTime * 1000 : lastModifyTime;
    }

    public StatisticTrace addChild(StatisticTrace child) {
        if (child == null || childStack.size() >= MAX_TRACE_CHILDREN_SIZE) {
            return this;
        }
        childStack.add(child);
        return this;
    }

    public void addChildren(Collection<StatisticTrace> children) {
        if (childStack.size() >= MAX_TRACE_CHILDREN_SIZE) {
            return;
        }
        // remove null elements
        List<StatisticTrace> l = children.stream().filter(Objects::nonNull).collect(Collectors.toList());

        //cal children adding size
        int toIndex = Math.min(MAX_TRACE_CHILDREN_SIZE - childStack.size(), l.size());

        childStack.addAll(l.subList(0, toIndex));
    }

    public String print() {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        printTrace(null, null, pw, 0);
        sw.flush();
        return sw.toString();
    }

    // private method

    private void printTrace(String parentCatalogTarget, String parentAction, PrintWriter printWriter, int level) {
        println(printWriter, level, "Catalog:" + catalogTarget);
        println(printWriter, level, "Action:" + action);

        // print value
        StringBuilder sb = new StringBuilder();
        digest(value, sb);
        println(printWriter, level, "StatisticValue:" + sb);

        // print desc
        println(printWriter, level, desc);

        // print source
        if (source != StatisticResultSource.NULL) {
            println(printWriter, level, source + ", modify by " + getTimeStamp(new Date(lastModifyTime)));
        }

        for (StatisticTrace st : childStack) {
            st.printTrace(catalogTarget, action, printWriter, level + 1);
        }
    }

    private void println(PrintWriter printWriter, int level, String line) {
        if (StringUtils.isEmpty(line)) {
            return;
        }
        // two space print ahead for each layer
        printWriter.append(new SpaceString(level * 2)).println(line);
    }

    /**
     * cp from org.apache.calcite.avatica.util.Spaces.SpaceString
     * avoid the dependency of calcite package
     */
    private static class SpaceString implements CharSequence {
        private final int length;

        private SpaceString(int length) {
            this.length = length;
        }

        public String toString() {
            return Spaces.of(this.length);
        }

        public int length() {
            return this.length;
        }

        public char charAt(int index) {
            return ' ';
        }

        public @NotNull CharSequence subSequence(int start, int end) {
            return new SpaceString(end - start);
        }
    }
}
