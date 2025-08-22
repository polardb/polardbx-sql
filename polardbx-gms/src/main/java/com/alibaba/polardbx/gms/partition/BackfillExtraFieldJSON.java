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

package com.alibaba.polardbx.gms.partition;

import com.alibaba.fastjson.JSON;

public class BackfillExtraFieldJSON {
    public String progress;
    public String approximateRowCount;
    public String splitLevel;
    public String testCaseName;
    public Boolean logical = false;

    public Boolean getLogical() {
        return logical;
    }

    public void setLogical(Boolean logical) {
        this.logical = logical;
    }

    public BackfillExtraFieldJSON() {
        this.approximateRowCount = "0";
    }

    public static BackfillExtraFieldJSON fromJson(String json) {
        BackfillExtraFieldJSON result = JSON.parseObject(json, BackfillExtraFieldJSON.class);
        if (result == null) {
            return new BackfillExtraFieldJSON();
        }
        return result;
    }

    public static String toJson(BackfillExtraFieldJSON obj) {
        if (obj == null) {
            return "";
        }
        return JSON.toJSONString(obj);
    }

    @Override
    public String toString() {
        return toJson(this);
    }

    public String getApproximateRowCount() {
        return approximateRowCount;
    }

    public String getProgress() {
        return progress;
    }

    public void setApproximateRowCount(String approximateRowCount) {
        this.approximateRowCount = approximateRowCount;
    }

    public void setProgress(String progress) {
        this.progress = progress;
    }

    public String getSplitLevel() {
        return splitLevel;
    }

    public void setSplitLevel(String splitLevel) {
        this.splitLevel = splitLevel;
    }

    public void setTestCaseName(String testCaseName) {
        this.testCaseName = testCaseName;
    }

    public String getTestCaseName() {
        return testCaseName;
    }

    static public boolean isNotEmpty(BackfillExtraFieldJSON extra) {
        if (extra == null) {
            return false;
        }

        return extra.getApproximateRowCount() != null;
    }

}
