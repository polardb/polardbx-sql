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


import com.alibaba.polardbx.common.cdc.RplConstants;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author shicai.xsc 2021/3/5 13:32
 * @desc
 * @since 5.0.0.0
 */
public class SqlReplicationBase extends SqlDal {

    protected SqlKind sqlKind;
    protected SqlSpecialOperator operator;
    protected String keyWord;
    protected List<Pair<SqlNode, SqlNode>> optionNodes;
    protected SqlNode channelNode;
    protected SqlNode subChannelNode;
    protected Map<String, String> params = new HashMap<>();

    public SqlReplicationBase(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channelNode,
                              SqlNode subChannelNode) {
        super(pos);
        this.optionNodes = options;
        this.channelNode = channelNode;
        this.subChannelNode = subChannelNode;
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getKind() {
        return sqlKind;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(FrameTypeEnum.SELECT);

        writer.keyword(keyWord);
        for (int i = 0; i < optionNodes.size(); i++) {
            if (i > 0) {
                writer.print(", ");
            }
            final Pair<SqlNode, SqlNode> ops = optionNodes.get(i);
            final SqlNode key = ops.getKey();
            key.unparse(writer, leftPrec, rightPrec);
            writer.print("=");
            final SqlNode value = ops.getValue();
            dealOption(key, value);
            if (value instanceof SqlIdentifier) {
                writer.print(value.toString());
            } else {
                value.unparse(writer, leftPrec, rightPrec);
            }
        }
        if (channelNode != null) {
            writer.keyword("FOR");
            writer.keyword(RplConstants.CHANNEL);
            String channel = ((NlsString)((SqlCharStringLiteral)channelNode).value).getValue();
            params.put(RplConstants.CHANNEL, channel);
            writer.print(channel);
        }
        if (subChannelNode != null) {
            writer.keyword("FOR");
            writer.keyword(RplConstants.SUB_CHANNEL);
            String subChannel = ((NlsString)((SqlCharStringLiteral)subChannelNode).value).getValue();
            params.put(RplConstants.SUB_CHANNEL, subChannel);
            writer.print(subChannel);
        }
        writer.endList(selectFrame);
    }

    protected void parseParams(String k, String v) {
    }

    private void dealOption(SqlNode key, SqlNode value) {
        String k = ((NlsString)((SqlCharStringLiteral)key).value).getValue();
        String v;
        if (value instanceof SqlCharStringLiteral) {
            Object vv = ((SqlCharStringLiteral)value).getValue();
            if (vv instanceof NlsString) {
                v = ((NlsString)vv).getValue();
            } else {
                v = vv.toString();
            }
        } else {
            v = value.toString();
        }
        v = v.trim();
        parseParams(k, v.replace("`", ""));
    }

    public static class SqlReplicationOperator extends SqlSpecialOperator {

        public SqlReplicationOperator(SqlKind sqlKind) {
            super(sqlKind.name(), sqlKind);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));

            return typeFactory.createStructType(columns);
        }
    }

    public Map<String, String> getParams() {
        return params;
    }
}

