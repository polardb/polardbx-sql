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

package com.alibaba.polardbx.optimizer.ccl.common;

import lombok.Data;

/**
 * @author busu
 * date: 2020/11/13 3:01 下午
 */
@Data
public class CclPlanCacheKey {

    public CclPlanCacheKey() {

    }

    public CclPlanCacheKey(int hash, String schema, String user, String host) {
        this.hash = hash;
        this.schema = schema;
        this.user = user;
        this.host = host;
    }

    private int hash;
    private String schema;
    private String user;
    private String host;
}
