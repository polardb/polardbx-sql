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

package com.alibaba.polardbx.optimizer.hint.operator;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

/**
 * @author chenmo.cm
 */
public class HintCmdIndex extends BaseHintOperator implements HintCmdOperator {
    public final SqlIdentifier tableName;
    public final SqlIdentifier indexName;
    private final SqlIdentifier localIndexName;

    public HintCmdIndex(SqlBasicCall hint, ExecutionContext ec) {
        super(hint, ec);
        // init tableName
        tableName = (SqlIdentifier) this.argMap.get(new HintArgKey("table", 0));
        indexName = (SqlIdentifier) this.argMap.get(new HintArgKey("index", 1));
        localIndexName = (SqlIdentifier) this.argMap.get(new HintArgKey("local_index", 2));
    }

    @Override
    protected List<HintArgKey> getArgKeys() {
        return HintArgKey.INDEX_HINT;
    }

    @Override
    public CmdBean handle(CmdBean current) {
        return current;
    }

    public String tableNameLast() {
        return Optional.ofNullable(tableName).map(SqlIdentifier::getLastName).orElse(null);
    }

    public String indexNameLast() {
        return Optional.ofNullable(indexName).map(SqlIdentifier::getLastName).orElse(null);
    }

    public SqlIdentifier getLocalIndexName() {
        return localIndexName;
    }
}
