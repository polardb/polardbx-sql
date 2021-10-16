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

package com.alibaba.polardbx.optimizer.parse.bean;

import java.util.List;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
public class SqlParameterizedHash {

    private long sqlHash;
    private String originSql;
    private List<?> parameters;

    public SqlParameterizedHash(long sqlHash, String originSql, List<?> parameters) {
        this.sqlHash = sqlHash;
        this.originSql = originSql;
        this.parameters = parameters;
    }

    public long getSqlHash() {
        return sqlHash;
    }

    public String getOriginSql() {
        return originSql;
    }

    public List<?> getParameters() {
        return parameters;
    }

}
