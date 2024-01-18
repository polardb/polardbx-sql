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

package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class CreateDatabasePreparedData extends DdlPreparedData {
    private String srcSchemaName;
    private String dstSchemaName;

    private List<String> includeTables;
    private List<String> excludeTables;

    boolean like;
    boolean as;
    boolean withLock;
    boolean doCreateTable;

    public CreateDatabasePreparedData(String srcSchemaName, String dstSchemaName, List<String> includeTables,
                                      List<String> excludeTables,
                                      boolean like, boolean as, boolean withLock, boolean doCreateTable) {
        super(null, null);
        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.includeTables = includeTables;
        this.excludeTables = excludeTables;
        this.like = like;
        this.as = as;
        this.withLock = withLock;
        this.doCreateTable = doCreateTable;
    }

    public CreateDatabasePreparedData() {
    }

    public String getSrcSchemaName() {
        return srcSchemaName;
    }

    public void setSrcSchemaName(String srcSchemaName) {
        this.srcSchemaName = srcSchemaName;
    }

    public String getDstSchemaName() {
        return dstSchemaName;
    }

    public void setDstSchemaName(String dstSchemaName) {
        this.dstSchemaName = dstSchemaName;
    }

    public List<String> getIncludeTables() {
        return includeTables;
    }

    public void setIncludeTables(List<String> includeTables) {
        this.includeTables = includeTables;
    }

    public List<String> getExcludeTables() {
        return excludeTables;
    }

    public void setExcludeTables(List<String> excludeTables) {
        this.excludeTables = excludeTables;
    }

    public boolean isLike() {
        return like;
    }

    public void setLike(boolean like) {
        this.like = like;
    }

    public boolean isAs() {
        return as;
    }

    public void setAs(boolean as) {
        this.as = as;
    }

    public boolean isWithLock() {
        return withLock;
    }

    public void setWithLock(boolean withLock) {
        this.withLock = withLock;
    }

    public boolean isDoCreateTable() {
        return doCreateTable;
    }

    public void setDoCreateTable(boolean doCreateTable) {
        this.doCreateTable = doCreateTable;
    }
}
