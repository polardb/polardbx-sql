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

package com.alibaba.polardbx.executor.operator.scan.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;

public class RuntimeMetricsImpl implements RuntimeMetrics {
    /**
     * Responsible for allocation and maintenance of metrics counter.
     */
    private MetricRegistry registry;

    /**
     * The unique name of this runtime metrics.
     */
    private final String rootName;

    /**
     * The parent of this runtime metrics.
     */
    private RuntimeMetrics parent;

    /**
     * children metrics.
     */
    private Map<String, RuntimeMetrics> childrenMap;

    /**
     * Mapping from metrics name to inner object of metrics.
     */
    private Map<String, InnerCounter> counterMap;

    public RuntimeMetricsImpl(String rootName) {
        this.registry = new MetricRegistry();
        this.rootName = rootName;

        // concurrency-safe and sorted in lexicographical order
        this.childrenMap = new ConcurrentSkipListMap<>(String::compareTo);
        this.counterMap = new ConcurrentSkipListMap<>(String::compareTo);

        InnerCounter rootCounter = new DerivedCounter(rootName, ProfileUnit.NONE, null, ProfileAccumulatorType.NONE);
        counterMap.put(rootName, rootCounter);
    }

    /**
     * A tree-structure visitor for traversal of metrics counter.
     *
     * @param <T> Type of returned object.
     */
    interface Visitor<T> {
        /**
         * Traversing Real counter node.
         *
         * @param level level of traversal.
         */
        T visitRealCounter(RealCounter realCounter, int level);

        /**
         * Traversing Derived counter node.
         *
         * @param level level of traversal.
         */
        T visitDerivedCounter(DerivedCounter derivedCounter, int level);
    }

    /**
     * The tree node of runtime metrics.
     */
    interface InnerCounter {
        String name();

        ProfileUnit unit();

        InnerCounter parent();

        /**
         * Adaptor for tree node visitor.
         */
        <T> T accept(Visitor<T> visitor, int level);
    }

    /**
     * Only for summarizing the results from children nodes.
     */
    private static class DerivedCounter implements InnerCounter {
        private String name;
        private ProfileUnit unit;
        private InnerCounter parent;
        private List<InnerCounter> children;
        private ProfileAccumulatorType accumulatorType;

        public DerivedCounter(String name, ProfileUnit unit, InnerCounter parent,
                              ProfileAccumulatorType accumulatorType) {
            this.name = name;
            this.unit = unit;
            this.accumulatorType = accumulatorType;
            this.children = new ArrayList<>();
            this.parent = parent;
        }

        public ProfileAccumulatorType getAccumulatorType() {
            return accumulatorType;
        }

        public void addChild(InnerCounter innerCounter) {
            children.add(innerCounter);
        }

        public List<InnerCounter> getChildren() {
            return children;
        }

        public void removeChild(InnerCounter innerCounter) {
            children.remove(innerCounter);
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ProfileUnit unit() {
            return unit;
        }

        @Override
        public InnerCounter parent() {
            return parent;
        }

        @Override
        public <T> T accept(Visitor<T> visitor, int level) {
            return visitor.visitDerivedCounter(this, level);
        }
    }

    /**
     * To collect the metrics during execution.
     */
    private static class RealCounter implements InnerCounter {
        private Counter counter;
        private String name;
        private InnerCounter parent;
        private ProfileUnit unit;

        public RealCounter(Counter counter, String name, ProfileUnit unit, InnerCounter parent) {
            this.counter = counter;
            this.name = name;
            this.unit = unit;
            this.parent = parent;
        }

        public Counter counter() {
            return counter;
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public ProfileUnit unit() {
            return unit;
        }

        @Override
        public InnerCounter parent() {
            return parent;
        }

        @Override
        public <T> T accept(Visitor<T> visitor, int level) {
            return visitor.visitRealCounter(this, level);
        }
    }

    /**
     * To traverse the metrics tree for aggregating the metrics value and storing into results map.
     */
    private static class AccumulatorVisitor implements Visitor<Long> {
        private Map<String, Long> result = new TreeMap<>(String::compareTo);

        public Map<String, Long> getResult() {
            return result;
        }

        @Override
        public Long visitRealCounter(RealCounter realCounter, int level) {
            long count = realCounter.counter().getCount();
            result.put(realCounter.name(), count);
            return count;
        }

        @Override
        public Long visitDerivedCounter(DerivedCounter derivedCounter, int level) {
            ProfileAccumulatorType accumulatorType = derivedCounter.getAccumulatorType();
            switch (accumulatorType) {
            case NONE: {
                for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                    innerCounter.accept(this, level + 1);
                }
                return 0L;
            }
            case MAX: {
                long max = -1L;
                for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                    max = Math.max(max, innerCounter.accept(this, level + 1));
                }
                result.put(derivedCounter.name(), max);
                return max;
            }
            case MIN: {
                long min = Long.MAX_VALUE;
                for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                    min = Math.min(min, innerCounter.accept(this, level + 1));
                }
                result.put(derivedCounter.name(), min);
                return min;
            }
            case AVG: {
                long sum = 0L;
                int n = 0;
                for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                    sum += innerCounter.accept(this, level + 1);
                    n++;
                }
                long avg = sum / n;
                result.put(derivedCounter.name(), avg);
                return avg;
            }
            case SUM:
            default: {
                long sum = 0L;
                for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                    sum += innerCounter.accept(this, level + 1);
                }
                result.put(derivedCounter.name(), sum);
                return sum;
            }
            }

        }
    }

    /**
     * To generate a metrics report from given root in the tree.
     */
    private static class PrintVisitor implements Visitor<Void> {
        private final Map<String, Long> resultMap;
        private final StringBuilder builder;

        private PrintVisitor(Map<String, Long> resultMap) {
            this.resultMap = resultMap;
            this.builder = new StringBuilder();
        }

        public String print() {
            return builder.toString();
        }

        @Override
        public Void visitRealCounter(RealCounter realCounter, int level) {
            while (level-- > 0) {
                builder.append(' ');
                builder.append(' ');
            }
            builder.append(realCounter.name());
            builder.append(' ');
            builder.append(resultMap.get(realCounter.name()));
            builder.append(' ');
            builder.append(realCounter.unit().getUnitStr());
            builder.append('\n');
            return null;
        }

        @Override
        public Void visitDerivedCounter(DerivedCounter derivedCounter, int level) {
            int nextLevel = level + 1;
            while (level-- > 0) {
                builder.append(' ');
                builder.append(' ');
            }
            builder.append(derivedCounter.name());

            if (resultMap.containsKey(derivedCounter.name())) {
                builder.append(' ');
                builder.append(resultMap.get(derivedCounter.name()));
                builder.append(' ');
                builder.append(derivedCounter.unit().getUnitStr());
            }
            builder.append(':');
            builder.append('\n');

            for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                innerCounter.accept(this, nextLevel);
            }
            return null;
        }
    }

    private static class MergeVisitor implements Visitor<Void> {
        private RuntimeMetricsImpl metrics;

        public MergeVisitor(RuntimeMetricsImpl metrics) {
            this.metrics = metrics;
        }

        @Override
        public Void visitRealCounter(RealCounter realCounter, int level) {
            InnerCounter innerCounter;
            if ((innerCounter = metrics.counterMap.get(realCounter.name())) != null
                && innerCounter instanceof RealCounter) {
                long addend = ((RealCounter) innerCounter).counter.getCount();
                realCounter.counter.inc(addend);
            }
            return null;
        }

        @Override
        public Void visitDerivedCounter(DerivedCounter derivedCounter, int level) {
            for (InnerCounter innerCounter : derivedCounter.getChildren()) {
                innerCounter.accept(this, level + 1);
            }
            return null;
        }
    }

    @Override
    public String name() {
        return rootName;
    }

    @Override
    public RuntimeMetrics parent() {
        return parent;
    }

    @Override
    public List<RuntimeMetrics> children() {
        return childrenMap.values().stream().collect(Collectors.toList());
    }

    @Override
    public synchronized void addChild(RuntimeMetrics child) {
        childrenMap.putIfAbsent(child.name(), child);
        ((RuntimeMetricsImpl) child).parent = this;
    }

    @Override
    public synchronized Counter addCounter(String name, String parentName, ProfileUnit unit) {
        // handle parent.
        if (parentName == null || parentName.isEmpty()) {
            parentName = rootName;
        } else {
            Preconditions.checkArgument(counterMap.containsKey(parentName)
                && counterMap.get(parentName) instanceof DerivedCounter);
        }
        DerivedCounter parent = (DerivedCounter) counterMap.get(parentName);

        // find existed counter
        if (counterMap.containsKey(name)) {
            InnerCounter innerCounter = counterMap.get(name);
            Preconditions.checkArgument(innerCounter instanceof RealCounter);
            return ((RealCounter) innerCounter).counter();
        }

        // create and register counter into Metric Registry.
        Counter counter = registry.counter(name);
        InnerCounter innerCounter = new RealCounter(counter, name, unit, parent);
        parent.addChild(innerCounter);

        counterMap.put(name, innerCounter);

        return counter;
    }

    @Override
    public synchronized void addDerivedCounter(String name, String parentName, ProfileUnit unit,
                                               ProfileAccumulatorType accumulatorType) {
        // handle parent.
        if (parentName == null || parentName.isEmpty()) {
            parentName = rootName;
        } else {
            Preconditions.checkArgument(counterMap.containsKey(parentName)
                && counterMap.get(parentName) instanceof DerivedCounter);
        }
        DerivedCounter parent = (DerivedCounter) counterMap.get(parentName);

        // find existed counter
        if (counterMap.containsKey(name)) {
            InnerCounter innerCounter = counterMap.get(name);
            Preconditions.checkArgument(innerCounter instanceof DerivedCounter);
        }

        // create and register counter into Metric Registry.
        InnerCounter innerCounter = new DerivedCounter(name, unit, parent, accumulatorType);
        parent.addChild(innerCounter);

        counterMap.put(name, innerCounter);
    }

    @Override
    public Counter getCounter(String name) {
        // find existed counter
        InnerCounter innerCounter;
        if (counterMap.containsKey(name) &&
            (innerCounter = counterMap.get(name)) instanceof RealCounter) {
            return ((RealCounter) innerCounter).counter();
        }

        return null;
    }

    @Override
    public boolean remove(String name) {
        InnerCounter innerCounter;
        if ((innerCounter = counterMap.get(name)) != null) {
            // remove from children list
            InnerCounter parent = counterMap.get(name).parent();
            Preconditions.checkArgument(parent instanceof DerivedCounter);
            ((DerivedCounter) parent).removeChild(innerCounter);

            // remove from global map.
            return counterMap.remove(name) != null;
        }

        return false;
    }

    @Override
    public String report(String parent) {
        if (parent == null || parent.isEmpty()) {
            parent = rootName;
        }

        Preconditions.checkArgument(counterMap.containsKey(parent),
            String.format("The counter: %s does not exist", parent));

        // for aggregation.
        InnerCounter innerCounter = counterMap.get(parent);
        AccumulatorVisitor accumulatorVisitor = new AccumulatorVisitor();
        innerCounter.accept(accumulatorVisitor, 0);

        // for printing.
        Map<String, Long> result = accumulatorVisitor.getResult();
        PrintVisitor printVisitor = new PrintVisitor(result);
        innerCounter.accept(printVisitor, 0);

        return printVisitor.print();
    }

    @Override
    public void merge(RuntimeMetrics metrics) {
        if (metrics == null || !(metrics instanceof RuntimeMetricsImpl)) {
            return;
        }
        InnerCounter innerCounter = counterMap.get(rootName);
        MergeVisitor visitor = new MergeVisitor((RuntimeMetricsImpl) metrics);
        innerCounter.accept(visitor, 0);
    }
}
