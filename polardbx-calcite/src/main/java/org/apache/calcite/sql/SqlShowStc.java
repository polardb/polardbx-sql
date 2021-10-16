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

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

/**
 * @author chenmo.cm
 * @date 2018/6/11 下午10:16
 */
public class SqlShowStc extends SqlShow {

    private boolean isFull;

    public SqlShowStc(SqlParserPos pos,
                      List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                      SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean isFull) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.isFull = isFull;
    }

    public void setFull(boolean full) {
        isFull = full;
    }

    public boolean isFull() {
        return isFull;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlShowStcOperator(isFull);
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_STC;
    }

    public static class SqlShowStcOperator extends SqlSpecialOperator {
        final private boolean isFull;

        public SqlShowStcOperator(boolean isFull){
            super("SHOW_STC", SqlKind.SHOW_STC);
            this.isFull = isFull;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("DBNAME"        , 0  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("MYSQLADDR"     , 1  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("APPNAME"       , 2  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("GROUPNAME"     , 3  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ATOMNAME"      , 4  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("READCOUNT"     , 5  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所接收的到读请求数
            columns.add(new RelDataTypeFieldImpl("WRITECOUNT"    , 6  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所接收的到写请求数
            columns.add(new RelDataTypeFieldImpl("TOTALCOUNT"    , 7  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所接收的到总请求数
            columns.add(new RelDataTypeFieldImpl("READTIMECOST"  , 8  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所有读请求的执行用时, 单位: ms毫秒
            columns.add(new RelDataTypeFieldImpl("WRITETIMECOST" , 9  , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所有写请求的执行用时，单位: ms毫秒
            columns.add(new RelDataTypeFieldImpl("TOTALTIMECOST" , 10 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所有总请求的执行用时，单位: ms毫秒
            columns.add(new RelDataTypeFieldImpl("CONNERRCOUNT"  , 11 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所接收的到连接错误数目
            columns.add(new RelDataTypeFieldImpl("SQLERRCOUNT"   , 12 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//从启动到目前为止所接收SQL错误数目
            columns.add(new RelDataTypeFieldImpl("SQLLENGTH"     , 13 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//当前Atom执行的SQL 总长度
            columns.add(new RelDataTypeFieldImpl("ROWS"          , 14 , typeFactory.createSqlType(SqlTypeName.VARCHAR)));//当前Atom反馈的总行数
            if (!isFull) {
                columns.add(
                    new RelDataTypeFieldImpl("START_TIME", 15, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }


            return typeFactory.createStructType(columns);
        }
    }
}
