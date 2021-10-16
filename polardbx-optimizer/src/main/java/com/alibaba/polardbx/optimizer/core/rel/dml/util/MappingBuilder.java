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

package com.alibaba.polardbx.optimizer.core.rel.dml.util;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author chenmo.cm
 */
public abstract class MappingBuilder {
    protected final List<String> target;
    protected final Map<String, Integer> targetMap;
    protected final Integer targetSize;
    protected final boolean caseSensitive;

    protected List<Integer> mapping;

    protected MappingBuilder(List<String> target, Map<String, Integer> targetMap, Integer targetSize,
                             boolean caseSensitive) {
        this.target = target;
        this.targetMap = targetMap;
        this.targetSize = targetSize;
        this.caseSensitive = caseSensitive;
        this.mapping = IntStream.range(0, target.size()).boxed().collect(Collectors.toList());
    }

    public static MappingBuilder create(RelDataType rowType) {
        return new ListMappingBuilder(rowType.getFieldNames(), false);
    }

    public static MappingBuilder create(List<String> target) {
        return new ListMappingBuilder(target, false);
    }

    public static MappingBuilder create(List<String> target, boolean caseSensitive) {
        return new ListMappingBuilder(target, caseSensitive);
    }

    public static MappingBuilder create(Map<String, Integer> targetMap, Integer targetSize, boolean caseSensitive) {
        return new MapMappingBuilder(targetMap, targetSize, caseSensitive);
    }

    public abstract MappingBuilder targetOrderedSource(Collection<String> source);

    public abstract MappingBuilder source(List<String> source);

    public Mapping buildMapping() {
        return Mappings.source(mapping, getTargetSize());
    }

    public Mapping buildMapping(int offset) {
        return Mappings
            .source(mapping.stream().map(i -> i + offset).collect(Collectors.toList()), getTargetSize() + offset);
    }

    public List<String> getSource() {
        return this.mapping.stream().map(this.target::get).collect(Collectors.toList());
    }

    public List<String> getTarget() {
        return target;
    }

    public Map<String, Integer> getTargetMap() {
        return targetMap;
    }

    public int getTargetSize() {
        return targetSize;
    }

    public List<Integer> getMapping() {
        return mapping;
    }

    protected boolean isCaseSensitive() {
        return caseSensitive;
    }

    protected Set<String> getSet() {
        return isCaseSensitive() ? new HashSet<>() : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    }

    protected Map<String, Integer> getMap() {
        return getMap(isCaseSensitive());
    }

    protected static Map<String, Integer> getMap(boolean caseSensitive) {
        return caseSensitive ? new HashMap<>() : new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }
}
