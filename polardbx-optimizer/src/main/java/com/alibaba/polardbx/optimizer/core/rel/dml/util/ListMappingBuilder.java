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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class ListMappingBuilder extends MappingBuilder {

    protected ListMappingBuilder(List<String> target, boolean caseSensitive) {
        super(target, getMap(caseSensitive), target.size(), caseSensitive);

        Ord.zip(target).forEach(o -> this.targetMap.put(o.getValue(), o.getKey()));
    }

    @Override
    public MappingBuilder targetOrderedSource(Collection<String> source) {
        final Set<String> sourceSet = getSet();
        sourceSet.addAll(source);

        this.mapping = Ord.zip(target).stream().filter(o -> sourceSet.contains(o.getValue())).map(Ord::getKey)
            .collect(Collectors.toList());

        return this;
    }

    @Override
    public MappingBuilder source(List<String> source) {
        this.mapping = source.stream().map(targetMap::get).collect(Collectors.toList());

        return this;
    }
}
