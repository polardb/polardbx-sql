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

package com.alibaba.polardbx.group.config;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.rpc.compatible.XDataSource;

/**
 * @author 梦实 2017年11月21日 下午6:37:14
 * @since 5.0.0
 */
public interface GroupDataSourceHolder {

    TAtomDataSource getDataSource(MasterSlave masterSlave);

    Pair<Boolean, XDataSource> isChangingLeader(MasterSlave masterSlave);
}
