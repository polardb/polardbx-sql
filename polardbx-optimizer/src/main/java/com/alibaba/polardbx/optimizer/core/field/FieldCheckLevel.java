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

package com.alibaba.polardbx.optimizer.core.field;

/**
 * The check level of storage field.
 * CHECK_FIELD_IGNORE for select statement, and
 * CHECK_FIELD_WARN for insert/update/delete statement.
 */
public enum FieldCheckLevel {
    CHECK_FIELD_IGNORE(0),
    CHECK_FIELD_WARN(1),
    CHECK_FIELD_ERROR_FOR_NULL(2);

    private final int id;

    FieldCheckLevel(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
