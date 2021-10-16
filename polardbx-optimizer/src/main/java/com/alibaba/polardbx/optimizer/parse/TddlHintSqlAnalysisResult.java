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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.druid.sql.parser.ByteString;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于cobar构造的parse结果
 */
public class TddlHintSqlAnalysisResult {

    private ByteString sql;

    private String              tddlHintStr;

    private String              tddlGroupHintStr;

    private String              tddlSimpleHintStr;

    private String              tddlMshaHintStr;

    private int                 tddlHintIndex;
    private int                 tddlHintEndIndex;
    private int                 tddlGroupHintIndex;
    private int                 tddlGroupHintEndIndex;
    private int                 tddlSimpleHintIndex;
    private int                 tddlSimpleHintEndIndex;
    private int                 tddlMshaHintIndex;
    private int                 tddlMshaHintEndIndex;

    public static final String  TDDL_HINT        = "tddlHint";
    public static final String  TDDL_GROUP_HINT  = "tddlGroupHint";
    public static final String  TDDL_MSHA_HINT   = "tddlMshaHint";
    public static final String  TDDL_SIMPLE_HINT = "tddlSimpleHint";
    private Map<String, String> hintResultMap    = new HashMap<String, String>();

    public TddlHintSqlAnalysisResult(ByteString sql, String tddlHintStr, int tddlHintIndex, int tddlHintEndIndex,
                                     String tddlGroupHintStr, int tddlGroupHintIndex, int tddlGroupHintEndIndex,
                                     String tddlSimpleHintStr, int tddlSimpleHintIndex, int tddlSimpleHintEndIndex,
                                     String tddlMshaHintStr, int tddlMshaHintIndex, int tddlMshaHintEndIndex) {
        super();
        this.sql = sql;
        this.tddlHintStr = tddlHintStr;
        this.tddlGroupHintStr = tddlGroupHintStr;
        this.tddlSimpleHintStr = tddlSimpleHintStr;
        this.tddlMshaHintStr = tddlMshaHintStr;

        this.tddlHintIndex = tddlHintIndex;
        this.tddlGroupHintIndex = tddlGroupHintIndex;
        this.tddlSimpleHintIndex = tddlSimpleHintIndex;
        this.tddlMshaHintIndex = tddlMshaHintIndex;

        this.tddlHintEndIndex = tddlHintEndIndex;
        this.tddlGroupHintEndIndex = tddlGroupHintEndIndex;
        this.tddlSimpleHintEndIndex = tddlSimpleHintEndIndex;
        this.tddlMshaHintEndIndex = tddlMshaHintEndIndex;

        this.hintResultMap.put(TDDL_HINT, tddlHintStr);
        this.hintResultMap.put(TDDL_GROUP_HINT, tddlGroupHintStr);
        this.hintResultMap.put(TDDL_MSHA_HINT, tddlMshaHintStr);
        this.hintResultMap.put(TDDL_SIMPLE_HINT, tddlSimpleHintStr);
    }

    public ByteString getSqlRemovedHintStr() {
        int maxEndIdxOfHint = -1;
        if (maxEndIdxOfHint < tddlGroupHintEndIndex) {
            maxEndIdxOfHint = tddlGroupHintEndIndex;
        }

        if (maxEndIdxOfHint < tddlHintEndIndex) {
            maxEndIdxOfHint = tddlHintEndIndex;
        }

        if (maxEndIdxOfHint < tddlSimpleHintEndIndex) {
            maxEndIdxOfHint = tddlSimpleHintEndIndex;
        }

        return sql.slice(maxEndIdxOfHint);
    }

    public String getTddlHintStr() {
        return tddlHintStr;
    }

    public String getTddlGroupHintStr() {
        return tddlGroupHintStr;
    }

    public String getTddlSimpleHintStr() {
        return tddlSimpleHintStr;
    }

    public int getTddlHintIndex() {
        return tddlHintIndex;
    }

    public int getTddlGroupHintIndex() {
        return tddlGroupHintIndex;
    }

    public int getTddlSimpleHintIndex() {
        return tddlSimpleHintIndex;
    }

    public int getTddlHintEndIndex() {
        return tddlHintEndIndex;
    }

    public int getTddlGroupHintEndIndex() {
        return tddlGroupHintEndIndex;
    }

    public int getTddlSimpleHintEndIndex() {
        return tddlSimpleHintEndIndex;
    }


    public String getTddlMshaHintStr() {
        return tddlMshaHintStr;
    }

    public Map<String, String> getHintResultMap() {
        return hintResultMap;
    }
}
