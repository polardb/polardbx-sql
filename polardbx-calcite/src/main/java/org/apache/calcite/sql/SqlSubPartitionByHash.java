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

package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlSubPartitionByHash extends SqlSubPartitionBy {
    protected final boolean key;
    protected final boolean unique;

    public SqlSubPartitionByHash(boolean key, boolean unique,SqlParserPos sqlParserPos) {
        super(sqlParserPos);
        this.key = key;
        this.unique = unique;
    }

    public boolean isKey() {
        return key;
    }
}
