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

package com.alibaba.polardbx.executor;

import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.executor.spi.IRepository;

/**
 * 为TGroupDatasource实现的groupexecutor
 * 因为TGroupDatasource中已经做了主备切换等功能，所以TddlGroupExecutor只是简单的执行sql
 *
 * @author mengshi.sunmengshi 2013-12-6 下午2:39:18
 * @since 5.0.0
 */
public class TddlGroupExecutor extends AbstractGroupExecutor {

    private TGroupDataSource groupDataSource;

    public TddlGroupExecutor(IRepository repo) {
        super(repo);
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    @Override
    protected void doDestroy() {
        if (this.groupDataSource != null) {
            groupDataSource.destroyDataSource();
        }
    }

    @Override
    public TGroupDataSource getDataSource() {
        return groupDataSource;
    }

    public void setGroupDataSource(TGroupDataSource ds) {
        this.groupDataSource = ds;
    }
}
