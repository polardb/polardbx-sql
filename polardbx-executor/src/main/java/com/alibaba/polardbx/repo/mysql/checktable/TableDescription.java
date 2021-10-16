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

package com.alibaba.polardbx.repo.mysql.checktable;

import java.util.HashMap;
import java.util.Map;

/**
 * 表描述
 *
 * @author arnkore 2017-06-19 17:44
 */
public class TableDescription {

    protected String groupName;
    protected String tableName;
    protected Map<String, FieldDescription> fields = new HashMap<String, FieldDescription>();

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, FieldDescription> getFields() {
        return fields;
    }

    public void setFields(Map<String, FieldDescription> fields) {
        this.fields = fields;
    }
}
