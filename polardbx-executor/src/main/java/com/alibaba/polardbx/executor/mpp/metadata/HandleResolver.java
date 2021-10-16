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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.metadata;

import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.RemoteSplit;

import javax.inject.Inject;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;

public class HandleResolver {
    private final ConcurrentMap<String, MaterializedHandleResolver> handleResolvers = new ConcurrentHashMap<>();

    @Inject
    public HandleResolver() {
        handleResolvers.put("$scan", new MaterializedHandleResolver(JdbcSplit.class));
        handleResolvers.put("$remote", new MaterializedHandleResolver(RemoteSplit.class));
    }

    public String getId(ConnectorSplit split) {
        return getId(split, MaterializedHandleResolver::getSplitClass);
    }

    public Class<? extends ConnectorSplit> getSplitClass(String id) {
        Class<? extends ConnectorSplit> clazz = resolverFor(id).getSplitClass();
        if (clazz == null) {
            throw new IllegalArgumentException("No resolver for " + id);
        }
        return clazz;
    }

    private MaterializedHandleResolver resolverFor(String id) {
        MaterializedHandleResolver resolver = handleResolvers.get(id);
        checkArgument(resolver != null, "No handle resolver for connector: %s", id);
        return resolver;
    }

    private <T> String getId(T handle, Function<MaterializedHandleResolver, Class<? extends T>> getter) {
        for (Entry<String, MaterializedHandleResolver> entry : handleResolvers.entrySet()) {
            try {
                Class<? extends T> clazz = getter.apply(entry.getValue());
                if (clazz != null && clazz.isInstance(handle)) {
                    //if (getter.apply(entry.getValue()).map(clazz -> clazz.isInstance(handle)).orElse(false)) {
                    return entry.getKey();
                }
            } catch (UnsupportedOperationException ignored) {
            }
        }
        throw new IllegalArgumentException("No connector for handle: " + handle);
    }

    private static class MaterializedHandleResolver {
        private final Class<? extends ConnectorSplit> split;

        public MaterializedHandleResolver(Class<? extends ConnectorSplit> split) {
            this.split = split;
        }

        public Class<? extends ConnectorSplit> getSplitClass() {
            return split;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MaterializedHandleResolver that = (MaterializedHandleResolver) o;
            return Objects.equals(split, that.split);
        }

        @Override
        public int hashCode() {
            return Objects.hash(split);
        }
    }
}
