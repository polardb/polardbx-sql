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

package com.alibaba.polardbx.executor.spi;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.executor.IExecutor;
import com.alibaba.polardbx.executor.repo.RepositoryConfig;

/**
 * 每个存储一个，主入口
 *
 * @author mengshi.sunmengshi 2013-11-27 下午3:59:13
 * @since 5.0.0
 */
public interface IRepository extends Lifecycle {

    /**
     * 获取当前存储引擎的一些配置信息。
     */
    RepositoryConfig getRepoConfig();

    /**
     * cursor实现类
     */
    ICursorFactory getCursorFactory();

    /**
     * 获取对应的 handler构造器
     */
    ICommandHandlerFactory getCommandExecutorFactory();

    /**
     * 获取对应的group {@link IExecutor}
     */
    IGroupExecutor getGroupExecutor(Group group);

    /**
     * invdalidate the group executor when destroy the old group from TopologyHandler.executorMap
     */
    void invalidateGroupExecutor(Group group);

}
