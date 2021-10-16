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
 * @author arnkore 2017-06-19 17:43
 */
public class TableCheckResult {

    private TableDescription tableDesc = null;

    // 标识这个表是否存在
    private boolean isExist = true;

    // 标识表中的所有列的描述是否一致
    private boolean isFieldDescTheSame = true;

    // 标识表中的所有列的数目是否一致
    private boolean isFieldCountTheSame = false;

    // 标识表是否为影子表
    private boolean isShadowTable = false;

    // 保存校验过程中的多出来的非预期列
    private Map<String, FieldDescription> unexpectedFieldDescMaps = new HashMap<String, FieldDescription>();

    // 保存校验过程中的缺乏的预期列
    private Map<String, FieldDescription> missingFieldDescMaps = new HashMap<String, FieldDescription>();

    // 列名字相同，但列定义描述不一致的列
    private Map<String, FieldDescription> incorrectFieldDescMaps = new HashMap<String, FieldDescription>();

    // 所有的不一致列
    private Map<String, FieldDescription> abnormalFieldDescMaps = new HashMap<String, FieldDescription>();

    public TableDescription getTableDesc() {
        return tableDesc;
    }

    public void setTableDesc(TableDescription tableDesc) {
        this.tableDesc = tableDesc;
    }

    public boolean isExist() {
        return isExist;
    }

    public void setExist(boolean isExist) {
        this.isExist = isExist;
    }

    public boolean isFieldDescTheSame() {
        return isFieldDescTheSame;
    }

    public void setFieldDescTheSame(boolean isFieldDescTheSame) {
        this.isFieldDescTheSame = isFieldDescTheSame;
    }

    public boolean isFieldCountTheSame() {
        return isFieldCountTheSame;
    }

    public void setFieldCountTheSame(boolean isFieldCountTheSame) {
        this.isFieldCountTheSame = isFieldCountTheSame;
    }

    public Map<String, FieldDescription> getUnexpectedFieldDescMaps() {
        return unexpectedFieldDescMaps;
    }

    public Map<String, FieldDescription> getMissingFieldDescMaps() {
        return missingFieldDescMaps;
    }

    public Map<String, FieldDescription> getIncorrectFieldDescMaps() {
        return incorrectFieldDescMaps;
    }

    public Map<String, FieldDescription> getAbnormalFieldDescMaps() {
        return abnormalFieldDescMaps;
    }

    public boolean isShadowTable() {
        return isShadowTable;
    }

    public void setShadowTable(boolean shadowTable) {
        isShadowTable = shadowTable;
    }

    public void addUnexpectedFieldDesc(FieldDescription fieldDescription) {
        unexpectedFieldDescMaps.put(fieldDescription.getFieldName(), fieldDescription);
        abnormalFieldDescMaps.put(fieldDescription.getFieldName(), fieldDescription);
    }

    public void addMissingFieldDesc(FieldDescription fieldDescription) {
        missingFieldDescMaps.put(fieldDescription.getFieldName(), fieldDescription);
    }

    public void addIncorrectFieldDescMaps(FieldDescription fieldDescription) {
        incorrectFieldDescMaps.put(fieldDescription.getFieldName(), fieldDescription);
        abnormalFieldDescMaps.put(fieldDescription.getFieldName(), fieldDescription);
    }

}
