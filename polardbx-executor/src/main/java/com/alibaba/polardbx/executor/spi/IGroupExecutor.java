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

import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.executor.IExecutor;

/**
 * 用于主备切换等group操作
 *
 * @author mengshi.sunmengshi 2013-12-6 下午1:45:56
 * @since 5.0.0
 */
public interface IGroupExecutor extends IExecutor {

    Group getGroupInfo();

    IDataSource getDataSource();

}
