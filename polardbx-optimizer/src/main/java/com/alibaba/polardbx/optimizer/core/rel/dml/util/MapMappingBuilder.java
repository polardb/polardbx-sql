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

import org.apache.calcite.linq4j.Ord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author chenmo.cm
 */
public class MapMappingBuilder extends MappingBuilder {
    protected MapMappingBuilder(Map<String, Integer> targetMap, Integer targetSize, boolean caseSensitive) {
        super(new ArrayList<>(targetSize), targetMap, targetSize, caseSensitive);

        IntStream.range(0, targetSize).boxed().forEach(i -> target.add(null));
        targetMap.forEach((k, v) -> target.set(v, k));
    }

    @Override
    public MappingBuilder targetOrderedSource(Collection<String> source) {
        final Set<String> sourceSet = getSet();
        sourceSet.addAll(source);

        this.mapping = Ord.zip(target).stream().filter(Objects::nonNull).filter(o -> sourceSet.contains(o.getValue()))
            .map(Ord::getKey).collect(Collectors.toList());

        return this;
    }

    @Override
    public MappingBuilder source(List<String> source) {
        this.mapping = source.stream().map(targetMap::get).collect(Collectors.toList());

        return this;
    }
}
