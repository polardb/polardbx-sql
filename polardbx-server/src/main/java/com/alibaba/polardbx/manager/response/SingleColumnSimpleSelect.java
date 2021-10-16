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

package com.alibaba.polardbx.manager.response;

/**
 * 单个simple-select语句
 *
 * @author arnkore 2016-12-27 16:56
 */
public abstract class SingleColumnSimpleSelect extends AbstractSimpleSelect {
    protected String origColumnName;

    protected String aliasColumnName;

    public SingleColumnSimpleSelect(String origColumnName, String aliasColumnName) {
        super(1);
        this.origColumnName = origColumnName;
        this.aliasColumnName = aliasColumnName;
    }

    public SingleColumnSimpleSelect(String origColumnName) {
        this(origColumnName, origColumnName);
    }

    public String getOrigColumnName() {
        return origColumnName;
    }

    public String getAliasColumnName() {
        return aliasColumnName;
    }
}
