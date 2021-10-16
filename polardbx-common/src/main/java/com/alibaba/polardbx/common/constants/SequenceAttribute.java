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

package com.alibaba.polardbx.common.constants;

import com.alibaba.polardbx.common.utils.TStringUtil;

public class SequenceAttribute {

    public enum Type {
        NA("", ""),
        GROUP("GROUP", "GROUP"),
        SIMPLE("SIMPLE", "SIMPLE"),
        TIME("TIME", "TIME");

        private final String keyword;
        private final String abbreviation;

        Type(String keyword, String abbreviation) {
            this.keyword = keyword;
            this.abbreviation = abbreviation;
        }

        public String getKeyword() {
            return this.keyword;
        }

        public String getAbbreviation() {
            return this.abbreviation;
        }

    }

    public static final String GMS_META_DB_KEY = "polardbx_meta_db";

    public static final int TRUE = 1;
    public static final int FALSE = 0;
    public static final int NA = -1;

    public static final String STR_YES = "Y";
    public static final String STR_NO = "N";
    public static final String STR_NA = "N/A";

    public static final String AUTO_SEQ_PREFIX = "AUTO_SEQ_";

    public static final String NATIVE_AUTO_INC_SYNTAX = "AUTO_INCREMENT";
    public static final String EXT_AUTO_INC_SYNTAX_TYPE = " BY ";
    public static final String EXTENDED_AUTO_INC_SYNTAX = NATIVE_AUTO_INC_SYNTAX + EXT_AUTO_INC_SYNTAX_TYPE;

    public static final String DEFAULT_GROUP_TABLE_NAME = SystemTables.SEQUENCE;
    public static final String DEFAULT_TABLE_NAME = SystemTables.SEQUENCE_OPT;

    public static final String DEFAULT_ID_COLUMN = "id";
    public static final String DEFAULT_TYPE_COLUMN = "type";
    public static final String DEFAULT_NAME_COLUMN = "name";
    public static final String DEFAULT_VALUE_COLUMN = "value";
    public static final String DEFAULT_INCREMENT_BY_COLUMN = "increment_by";
    public static final String DEFAULT_START_WITH_COLUMN = "start_with";
    public static final String DEFAULT_MAX_VALUE_COLUMN = "max_value";
    public static final String DEFAULT_CYCLE_COLUMN = "cycle";
    public static final String DEFAULT_GMT_CREATED_COLUMN = "gmt_created";
    public static final String DEFAULT_GMT_MODIFIED_COLUMN = "gmt_modified";

    public static final String EXT_UNIT_COUNT_COLUMN = "unit_count";
    public static final String EXT_UNIT_INDEX_COLUMN = "unit_index";
    public static final String EXT_INNER_STEP_COLUMN = "inner_step";

    public static final int DEFAULT_RETRY_TIMES = 3;
    public static final boolean DEFAULT_CHECK = false;

    public static final int DEFAULT_INCREMENT_BY = 1;
    public static final long DEFAULT_START_WITH = 1L;
    public static final long DEFAULT_MAX_VALUE = Long.MAX_VALUE;

    public static final int CYCLE = TRUE;
    public static final int NOCYCLE = FALSE;

    public static final int UNDEFINED_UNIT_COUNT = 0;
    public static final int UNDEFINED_UNIT_INDEX = -1;
    public static final int UNDEFINED_INNER_STEP = 0;

    public static final int DEFAULT_UNIT_COUNT = 1;
    public static final int DEFAULT_UNIT_INDEX = 0;
    public static final int DEFAULT_INNER_STEP = 100000;
    public static final int UPPER_LIMIT_UNIT_COUNT = 65536;

    public static final int CACHE_ENABLED = 1 << 7;
    public static final int CACHE_DISABLED = FALSE;

    public static final int TIME_BASED = 1 << 6;

    public static final int GROUP_SEQ_UPDATE_INTERVAL = 60;

    public static final String GROUP_SEQ_NODE = "NODE";
    public static final String GROUP_SEQ_RANGE = "RANGE [ MIN, MAX ]";
    public static final String GROUP_SEQ_MIN_VALUE = "MIN VALUE";

    public static final String NO_AUTO_VALUE_ON_ZERO = "NO_AUTO_VALUE_ON_ZERO";

    public static final boolean getAutoValueOnZero(String sqlMode) {
        return sqlMode == null || !TStringUtil.containsIgnoreCase(sqlMode, NO_AUTO_VALUE_ON_ZERO);
    }
}
