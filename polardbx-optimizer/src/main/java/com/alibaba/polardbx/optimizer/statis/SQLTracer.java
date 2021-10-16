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

package com.alibaba.polardbx.optimizer.statis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SQLTracer {

    private List<SQLOperation> ops = Collections.synchronizedList(new ArrayList());

    public SQLTracer() {

    }

    @JsonCreator
    public SQLTracer(@JsonProperty("ops") List<SQLOperation> ops) {
        this.ops = ops;
    }

    public void trace(SQLOperation oper) {
        ops.add(oper);
    }

    public void trace(List<SQLOperation> oper) {
        ops.addAll(oper);
    }

    @JsonProperty
    public List<SQLOperation> getOperations() {
        return this.ops;
    }
}
