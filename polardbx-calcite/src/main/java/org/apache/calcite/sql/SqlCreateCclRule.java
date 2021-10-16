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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author busu
 * date: 2020/10/26 2:55 下午
 */
public class SqlCreateCclRule extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateCclRuleOperator();

    private SqlIdentifier ruleName;

    private SqlIdentifier dbName;

    private SqlIdentifier tableName;

    private SqlUserName userName;

    private SqlIdentifier FOR;

    private SqlNodeList keywords;

    private SqlNodeList templateId;

    private SqlNode query;

    private List<Pair<SqlNode, SqlNode>> with;

    private boolean ifNotExists;

    public SqlCreateCclRule(SqlParserPos pos, boolean ifNotExists, SqlIdentifier ruleName, SqlIdentifier dbName,
                            SqlIdentifier tableName, SqlUserName userName, SqlIdentifier FOR, SqlNodeList keywords,
                            SqlNodeList templateId, List<Pair<SqlNode, SqlNode>> with, SqlNode query) {
        super(pos);
        this.ruleName = ruleName;
        this.dbName = dbName;
        this.tableName = tableName;
        this.userName = userName;
        this.FOR = FOR;
        this.keywords = keywords;
        this.templateId = templateId;
        this.with = with;
        this.ifNotExists = ifNotExists;
        this.query = query;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_CCL_RULE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CREATE CCL_RULE");
        if (ifNotExists) {
            writer.sep("IF NOT EXISTS");
        }
        ruleName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("ON");
        dbName.unparse(writer, leftPrec, 0);
        writer.keyword(".");
        tableName.unparse(writer, 0, 0);
        if (userName != null) {
            writer.keyword("TO");
            userName.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("FOR");
        FOR.unparse(writer, leftPrec, rightPrec);
        if (keywords != null) {
            writer.sep("FILTER BY KEYWORD");
            keywords.unparse(writer, leftPrec, rightPrec);
        }
        if (templateId != null) {
            writer.sep("FILTER BY TEMPLATE");
            templateId.unparse(writer, leftPrec, rightPrec);
        }
        if (query != null) {
            writer.sep("FILTER BY QUERY");
            query.unparse(writer, leftPrec, rightPrec);
        }
        writer.sep("WITH");
        if (with != null) {
            if (!with.isEmpty()) {
                Pair<SqlNode, SqlNode> firstPair = with.get(0);
                firstPair.left.unparse(writer, leftPrec, rightPrec);
                writer.print("=");
                firstPair.right.unparse(writer, leftPrec, rightPrec);
            }
            for (int i = 1; i < with.size(); ++i) {
                writer.print(", ");
                Pair<SqlNode, SqlNode> pair = with.get(i);
                pair.left.unparse(writer, leftPrec, rightPrec);
                writer.print("=");
                pair.right.unparse(writer, leftPrec, rightPrec);
            }
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlCreateCclRuleOperator extends SqlSpecialOperator {
        public SqlCreateCclRuleOperator() {
            super("CREATE_CCL_RULE", SqlKind.CREATE_CCL_RULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("Create_Ccl_Rule_Result",
                    0,
                    columnType)));
        }
    }

    public SqlIdentifier getRuleName() {
        return ruleName;
    }

    public void setRuleName(SqlIdentifier ruleName) {
        this.ruleName = ruleName;
    }

    @Override
    public SqlIdentifier getDbName() {
        return dbName;
    }

    public void setDbName(SqlIdentifier dbName) {
        this.dbName = dbName;
    }

    @Override
    public SqlIdentifier getTableName() {
        return tableName;
    }

    public void setTableName(SqlIdentifier tableName) {
        this.tableName = tableName;
    }

    public SqlUserName getUserName() {
        return userName;
    }

    public void setUserName(SqlUserName userName) {
        this.userName = userName;
    }

    public SqlIdentifier getFOR() {
        return FOR;
    }

    public void setFOR(SqlIdentifier FOR) {
        this.FOR = FOR;
    }

    public SqlNodeList getKeywords() {
        return keywords;
    }

    public void setKeywords(SqlNodeList keywords) {
        this.keywords = keywords;
    }

    public SqlNodeList getTemplateId() {
        return templateId;
    }

    public void setTemplateId(SqlNodeList templateId) {
        this.templateId = templateId;
    }

    public List<Pair<SqlNode, SqlNode>> getWith() {
        return with;
    }

    public void setWith(List<Pair<SqlNode, SqlNode>> with) {
        this.with = with;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public SqlNode getQuery() {
        return query;
    }

    public void setQuery(SqlNode query) {
        this.query = query;
    }

}
