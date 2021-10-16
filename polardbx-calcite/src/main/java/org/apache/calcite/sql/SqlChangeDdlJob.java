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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @version 1.0
 */
public class SqlChangeDdlJob extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlChangeDdlJobOperator();

    private long jobId;
    private boolean skip;
    private boolean add;
    private List<String> groupAndTableNameList = new ArrayList<>();

    public SqlChangeDdlJob(SqlParserPos pos, long jobId, boolean skip, boolean add, List<String> nameList) {
        super(pos);
        this.operands = new ArrayList<>(0);
        this.jobId = jobId;
        this.skip = skip;
        this.add = add;
        this.groupAndTableNameList.addAll(nameList);
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public boolean isAdd() {
        return add;
    }

    public void setAdd(boolean add) {
        this.add = add;
    }

    public List<String> getGroupAndTableNameList() {
        return groupAndTableNameList;
    }

    public void setGroupAndTableNameList(List<String> groupAndTableNameList) {
        this.groupAndTableNameList.clear();
        this.groupAndTableNameList.addAll(groupAndTableNameList);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("CHANGE DDL");
        writer.literal(String.valueOf(jobId));
        if (skip) {
            writer.keyword("SKIP");
        } else if (add) {
            writer.keyword("ADD");
        }
        for (String name : groupAndTableNameList) {
            writer.sep(",");
            writer.literal(name);
        }
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHANGE_DDL_JOB;
    }

    public static class SqlChangeDdlJobOperator extends SqlSpecialOperator {

        public SqlChangeDdlJobOperator() {
            super("CHANGE_DDL_JOB_CACHE", SqlKind.CHANGE_DDL_JOB);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("JOB_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }

}
