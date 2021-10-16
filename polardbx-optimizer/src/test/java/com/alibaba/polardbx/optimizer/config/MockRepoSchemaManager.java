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

package com.alibaba.polardbx.optimizer.config;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.extension.Activate;
import com.alibaba.polardbx.optimizer.config.table.RepoSchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;

@Activate(name = "MYSQL_JDBC")
public class MockRepoSchemaManager extends RepoSchemaManager {

    public MockRepoSchemaManager(String schemaName) {
        super(schemaName);
    }

    @Override
    protected void doInit() {
        // DO NOTHGING
    }

    @Override
    protected TableMeta getTable0(String logicalTableName, String actualTableName) {
        TableMeta tableMeta = super.getTable0(logicalTableName, actualTableName);
        if (tableMeta == null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, logicalTableName);
        }
        tableMeta.setSchemaName(this.getTddlRuleManager().getTddlRule().getSchemaName());
        return tableMeta;
    }

}
