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

package com.alibaba.polardbx.rule;

import org.apache.commons.lang.StringUtils;

import java.util.Objects;
import java.util.Set;

/**
 * Hot-key mapping metadata
 *
 * @author hongxi.chx
 */
public class MappingRule implements Comparable {

    private String dbKeyValue;
    private String tbKeyValue;
    private String db;
    private String tb;
    // if tb is a empty string,tbs is not empty
    private Set<String> tbs;

    public MappingRule() {
    }

    public MappingRule(String dbKeyValue, String tbKeyValue, String db, String tb) {
        this.dbKeyValue = dbKeyValue;
        this.tbKeyValue = tbKeyValue;
        this.db = db;
        this.tb = tb;
    }

    protected void setTbs(Set<String> tbs) {
        if (StringUtils.isNotEmpty(tb)) {
            return;
        }
        if (tbs != null && tbs.size() > 0) {
            return;
        }
        this.tbs = tbs;
    }

    public String getDbKeyValue() {
        return dbKeyValue;
    }

    public void setDbKeyValue(String dbKeyValue) {
        this.dbKeyValue = dbKeyValue;
    }

    public String getTbKeyValue() {
        return tbKeyValue;
    }

    public void setTbKeyValue(String tbKeyValue) {
        this.tbKeyValue = tbKeyValue;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTb() {
        return tb;
    }

    public void setTb(String tb) {
        this.tb = tb;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MappingRule that = (MappingRule) o;
        return Objects.equals(dbKeyValue, that.dbKeyValue) && Objects.equals(db, that.db)
            && Objects.equals(tbKeyValue, that.tbKeyValue) && Objects.equals(tb, that.tb);
    }

    @Override
    public int hashCode() {
        if (tbKeyValue != null) {
            return Objects.hash(dbKeyValue, db, tbKeyValue, tb);
        } else {
            return Objects.hash(dbKeyValue, db);
        }

    }

    @Override
    public int compareTo(Object o) {
        final MappingRule o1 = (MappingRule) o;
        StringBuilder src = new StringBuilder();
        StringBuilder desc = new StringBuilder();
        src.append(this.getDbKeyValue()).append(this.getDb()).append(this.getTbKeyValue()).append(this.getTb());
        desc.append(o1.getDbKeyValue()).append(o1.getDb()).append(o1.getTbKeyValue()).append(o1.getTb());
        return src.toString().compareTo(desc.toString());
    }
}
