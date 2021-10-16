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

package com.alibaba.polardbx.config;

import com.alibaba.polardbx.matrix.jdbc.TDataSource;

/**
 * @author xianmao.hexm
 */
public final class SchemaConfig {

    private volatile boolean isDropped = false;

    private final String name;

    private TDataSource ds;

    public SchemaConfig(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public TDataSource getDataSource() {
        return ds;
    }

    public void setDataSource(TDataSource ds) {
        this.ds = ds;
    }

    public boolean isDropped() {
        return isDropped;
    }

    public void setDropped(boolean dropped) {
        isDropped = dropped;
    }

}
