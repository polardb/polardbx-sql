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

package com.alibaba.polardbx.optimizer.core.sequence.bean;

import com.alibaba.polardbx.optimizer.core.sequence.ISequence;

/**
 * @author agapple 2014年12月18日 下午6:52:33
 * @since 5.1.17
 */
public abstract class Sequence<RT extends ISequence> implements ISequence<RT> {

    protected static final String STMT_SEPARATOR = ";";

    protected String name;

    protected String schemaName;

    protected String sql;

    @Override
    public String getSql() {
        return this.sql;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public String getSchemaName() {
        return this.schemaName;
    }

}
