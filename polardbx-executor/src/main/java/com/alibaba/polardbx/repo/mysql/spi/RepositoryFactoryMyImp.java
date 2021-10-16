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

package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.spi.IRepositoryFactory;

import java.util.Map;

@Activate(name = "MYSQL_JDBC")
public class RepositoryFactoryMyImp implements IRepositoryFactory {

    @Override
    public IRepository buildRepository(Group group, Map repoProperties, Map connectionProperties) {
        MyRepository myRepo = new MyRepository();
        myRepo.setAppName(group.getAppName());
        myRepo.setSchemaName(group.getSchemaName());
        myRepo.init();
        return myRepo;
    }

}
