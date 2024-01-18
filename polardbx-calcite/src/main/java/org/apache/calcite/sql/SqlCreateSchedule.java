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

import com.google.common.base.Splitter;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author guxu
 */
public class SqlCreateSchedule extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateScheduleOperator();

    private String schemaName;

    private SqlNode tableName;

    private String paramsExpr;
    private String cronExpr;
    private String timeZone;
    private String executorType;

    private boolean ifNotExists;

    private boolean forLocalPartition;
    private boolean forAutoSplitTableGroup;


    public SqlCreateSchedule(SqlParserPos pos,
                             boolean ifNotExists,
                             String schemaName,
                             SqlNode tableName,
                             boolean forLocalPartition,
                             boolean forAutoSplitTableGroup,
                             String paramsExpr,
                             String cronExpr,
                             String timeZone,
                             String executorType) {
        super(pos);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.paramsExpr = paramsExpr;
        this.cronExpr = cronExpr;
        this.timeZone = timeZone;
        this.ifNotExists = ifNotExists;
        this.executorType = executorType;
        this.forLocalPartition = forLocalPartition;
        this.forAutoSplitTableGroup = forAutoSplitTableGroup;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SCHEDULE");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        final String type = forLocalPartition? "LOCAL_PARTITION" : "AUTO_SPLIT_TABLE_GROUP";
        writer.keyword("FOR " + type + " ON");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("PARAMS");
        if(paramsExpr!=null){
            writer.keyword(paramsExpr);
        }
        writer.keyword("CRON");
        writer.keyword(cronExpr);
        if(StringUtils.isNotEmpty(timeZone)){
            writer.keyword("TIMEZONE");
            writer.keyword(timeZone);
        }
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SCHEDULE;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlCreateScheduleOperator extends SqlSpecialOperator {
        public SqlCreateScheduleOperator() {
            super("CREATE_SCHEDULE", SqlKind.CREATE_SCHEDULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SCHEDULE_RESULT",
                    0,
                    columnType)));
        }
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public SqlNode getTableName() {
        return this.tableName;
    }

    @Override
    public void setTableName(final SqlNode tableName) {
        this.tableName = tableName;
    }

    public String getCronExpr() {
        return this.cronExpr;
    }

    public void setCronExpr(final String cronExpr) {
        this.cronExpr = cronExpr;
    }

    public String getTimeZone() {
        return this.timeZone;
    }

    public void setTimeZone(final String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isIfNotExists() {
        return this.ifNotExists;
    }

    public void setIfNotExists(final boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public boolean isForLocalPartition() {
        return forLocalPartition;
    }

    public void setForLocalPartition(boolean forLocalPartition) {
        this.forLocalPartition = forLocalPartition;
    }

    public boolean isForAutoSplitTableGroup() {
        return forAutoSplitTableGroup;
    }

    public void setForAutoSplitTableGroup(boolean forAutoSplitTableGroup) {
        this.forAutoSplitTableGroup = forAutoSplitTableGroup;
    }

    public String getParamsExpr() {
        return paramsExpr;
    }

    public void setParamsExpr(String paramsExpr) {
        this.paramsExpr = paramsExpr;
    }

    public Map<String, String> parseParams(){
        Map<String, String> result = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        if(StringUtils.isEmpty(paramsExpr)){
            return result;
        }
        List<String> kvList = Splitter.on(",").splitToList(paramsExpr);
        for(String kv: kvList){
            List<String> kvPair = Splitter.on("=").splitToList(StringUtils.trim(kv));
            if(kvPair.size()==2){
                result.put(kvPair.get(0), kvPair.get(1));
            }
        }
        return result;
    }

    public String getExecutorType() { return executorType; }

    public void setExecutorType(String executorType) { this.executorType = executorType; }
}
