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
 * @date 2018/6/11 下午10:52
 */
public class SqlShowHtc extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowHtcOperator();

    public SqlShowHtc(SqlParserPos pos,
                      List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                      SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_HTC;
    }

    public static class SqlShowHtcOperator extends SqlSpecialOperator {

        public SqlShowHtcOperator(){
            super("SHOW_HTC", SqlKind.SHOW_HTC);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("CURRENT_TIME" , 0  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //当前系统时间
            columns.add(new RelDataTypeFieldImpl("CPU"          , 1  , typeFactory.createSqlType(SqlTypeName.DOUBLE)));  //当前实例时CPU百分比
            columns.add(new RelDataTypeFieldImpl("LOAD"         , 2  , typeFactory.createSqlType(SqlTypeName.DOUBLE)));  //当前实例时的机器load
            columns.add(new RelDataTypeFieldImpl("FREEMEM"      , 3  , typeFactory.createSqlType(SqlTypeName.DOUBLE)));  //当前机器内存的剩余内存, 单位K
            columns.add(new RelDataTypeFieldImpl("NETIN"        , 4  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //当前机器网络数据接收量，Byte
            columns.add(new RelDataTypeFieldImpl("NETOUT"       , 5  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //当前机器网络数据发出量，Byte
            columns.add(new RelDataTypeFieldImpl("NETIO"        , 6  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //当前机器网络IO，Byte
            columns.add(new RelDataTypeFieldImpl("FULLGCCOUNT"  , 7  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //FULL GC启动以来次数
            columns.add(new RelDataTypeFieldImpl("FULLGCTIME"   , 8  , typeFactory.createSqlType(SqlTypeName.BIGINT)));   //FULL GC启动以来耗时

            return typeFactory.createStructType(columns);
        }
    }
}
