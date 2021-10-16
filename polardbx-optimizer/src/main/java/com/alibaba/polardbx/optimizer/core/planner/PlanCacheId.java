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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.utils.TStringUtil;

/**
 * @author chenghui.lch
 * @date 2019/7/21 下午6:19
 */
public class PlanCacheId {

    protected int sqlHashVal = 0;
    protected String originalSql = null;

    public static PlanCacheId createPlanCacheId(String sql) {
        return new PlanCacheId(sql);
    }

    protected PlanCacheId(String sql) {
        this.originalSql = sql;
        this.sqlHashVal = sql.hashCode();
    }

    @Override
    public int hashCode() {
        return sqlHashVal;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof PlanCacheId)) {
            return false;
        }
        if (((PlanCacheId) (obj)).sqlHashVal != this.sqlHashVal) {
            return false;
        }
        if (!TStringUtil.equals(((PlanCacheId) (obj)).originalSql, this.originalSql)) {
            return false;
        }

        return true;
    }

    public int getIdAsInt() {
        return sqlHashVal;
    }

    public String getIdAsHexStr() {
        return TStringUtil.int2FixedLenHexStr(sqlHashVal);
    }

}
