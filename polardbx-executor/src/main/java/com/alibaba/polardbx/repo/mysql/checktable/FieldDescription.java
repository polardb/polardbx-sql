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

import org.apache.commons.lang.StringUtils;

/**
 * 列描述
 *
 * @author arnkore 2017-06-19 17:44
 */
public class FieldDescription {

    protected String fieldName;
    protected String fieldType;
    protected String fieldNull;
    protected String fieldKey;
    protected String fieldDefault;
    protected String fieldExtra;
    protected boolean isShadowTable;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldNull() {
        return fieldNull;
    }

    public void setFieldNull(String fieldNull) {
        this.fieldNull = fieldNull;
    }

    public String getFieldKey() {
        return fieldKey;
    }

    public void setFieldKey(String fieldKey) {
        this.fieldKey = fieldKey;
    }

    public String getFieldDefault() {
        return fieldDefault;
    }

    public void setFieldDefault(String fieldDefault) {
        this.fieldDefault = fieldDefault;
    }

    public String getFieldExtra() {
        return fieldExtra;
    }

    public void setFieldExtra(String fieldExtra) {
        this.fieldExtra = fieldExtra;
    }

    public boolean isShadowTable() {
        return isShadowTable;
    }

    public void setShadowTable(boolean shadowTable) {
        isShadowTable = shadowTable;
    }

    public boolean equals(FieldDescription otherDesc) {

        String fieldName = otherDesc.getFieldName();
        String fieldType = otherDesc.getFieldType();
        String fieldNull = otherDesc.getFieldNull();
        String fieldKey = otherDesc.getFieldKey();
        String fieldDefault = otherDesc.getFieldDefault();
        String fieldExtra = otherDesc.getFieldExtra();

        if (!StringUtils.equals(this.fieldName, fieldName)) {
            return false;
        }

        if (!StringUtils.equals(this.fieldType, fieldType)) {
            return false;
        }

        if (!StringUtils.equals(this.fieldNull, fieldNull)) {
            return false;
        }

        if (!isShadowTable && !otherDesc.isShadowTable() && !StringUtils.equals(this.fieldKey, fieldKey)) {
            return false;
        }

        if (!StringUtils.equals(this.fieldDefault, fieldDefault)) {
            return false;
        }

        if (!StringUtils.equals(this.fieldExtra, fieldExtra)) {
            return false;
        }

        return true;
    }
}
