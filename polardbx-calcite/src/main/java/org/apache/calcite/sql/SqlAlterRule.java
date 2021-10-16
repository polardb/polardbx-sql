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

import com.alibaba.polardbx.rule.MappingRule;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

/**
 * @author shicai.xsc 2018/9/14 下午3:23
 * @since 5.0.0.0
 */
public class SqlAlterRule extends SqlCreate {

    // ALTER TABLE xx ADD EXTPARTITION (DBPARTITION xxx BY KEY('abc')
    // TBPARTITION yyy BY KEY('abc'));
    // Though the sql starts with "ALTER TABLE", but it is not DDL
    // ，so we use a separate SqlAlterTable instead of SqlAlterRule
    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_RULE);

    private String sourceSql;

    private List<MappingRule> addMappingRules;

    private List<MappingRule> dropMappingRules;

    // -1: not changed, 0: false, 1: true
    private int broadcast = -1;
    private int allowFullTableScan = -1;

    public SqlAlterRule(SqlIdentifier tableName, String sql, SqlParserPos pos) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.sourceSql = sql;
    }

    public List<MappingRule> getAddMappingRules() {
        return this.addMappingRules;
    }

    public void setAddMappingRules(List<MappingRule> addMappingRules) {
        this.addMappingRules = addMappingRules;
    }

    public List<MappingRule> getDropMappingRules() {
        return dropMappingRules;
    }

    public void setDropMappingRules(List<MappingRule> dropMappingRules) {
        this.dropMappingRules = dropMappingRules;
    }

    public int getBroadcast() {
        return broadcast;
    }

    public void setBroadcast(int broadcast) {
        this.broadcast = broadcast;
    }

    public int getAllowFullTableScan() {
        return allowFullTableScan;
    }

    public void setAllowFullTableScan(int allowFullTableScan) {
        this.allowFullTableScan = allowFullTableScan;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("ALTER TABLE");
        name.unparse(writer, leftPrec, rightPrec);
        writer.keyword("SET TBLPROPERTIES");
        writer.sep("(");
        writer.keyword("(RULE.");
        writer.sep(".");
        if (broadcast >= 0) {
            writer.keyword("broadcast");
        } else if (allowFullTableScan >= 0) {
            writer.keyword("allowFullTableScan");
        }
        writer.sep("=");
        if (broadcast == 0 || allowFullTableScan == 0) {
            writer.keyword("false");
        } else if (broadcast == 1 || allowFullTableScan == 1) {
            writer.keyword("true");
        }
        writer.sep(")");
        writer.endList(frame);
    }
}
