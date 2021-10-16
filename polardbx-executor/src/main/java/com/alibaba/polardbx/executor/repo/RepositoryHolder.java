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

package com.alibaba.polardbx.executor.repo;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.IRepositoryFactory;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class RepositoryHolder {

    private Map<String, IRepository> repository = new ConcurrentHashMap<>();

    public boolean containsKey(String repoName) {
        return repository.containsKey(repoName);
    }

    public boolean containsValue(IRepository repo) {
        return repository.containsValue(repo);
    }

    public IRepository get(String repoName) {
        return repository.get(repoName);
    }

    public IRepository getOrCreateRepository(Group group, Map<String, String> properties, Map connectionProperties) {
        String repoName = group.getType().name();
        if (get(repoName) != null) {
            return get(repoName);
        }

        synchronized (this) {
            if (get(repoName) == null) {
                IRepositoryFactory factory = getRepoFactory(repoName);
                IRepository repo = factory.buildRepository(group, properties, connectionProperties);
                repo.init();
                this.put(repoName, repo);
            }
        }

        return this.get(repoName);
    }

    private IRepositoryFactory getRepoFactory(String repoName) {
        return ExtensionLoader.load(IRepositoryFactory.class, repoName);
    }

    public IRepository put(String repoName, IRepository repo) {
        return repository.put(repoName, repo);
    }

    public Set<Entry<String, IRepository>> entrySet() {
        return repository.entrySet();
    }

    public Map<String, IRepository> getRepository() {
        return repository;
    }

    public void setRepository(Map<String, IRepository> reponsitory) {
        this.repository = reponsitory;
    }

    public void clear() {
        this.repository.clear();
    }

}
