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
 * @since 5.0.0.0
 */
public class SqlReplicationBase extends SqlDal {

    protected SqlKind sqlKind;
    protected SqlSpecialOperator operator;
    protected String keyWord;
    protected List<Pair<SqlNode, SqlNode>> optionNodes;
    protected SqlNode channelNode;
    protected Map<String, String> params = new HashMap<>();

    public SqlReplicationBase(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options) {
        super(pos);
        this.optionNodes = options;
    }

    public SqlReplicationBase(SqlParserPos pos, List<Pair<SqlNode, SqlNode>> options, SqlNode channel) {
        super(pos);
        this.optionNodes = options;
        this.channelNode = channel;
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
            String channel = ((NlsString) ((SqlCharStringLiteral) channelNode).value).getValue();
            params.put(RplConstants.CHANNEL, channel);
            writer.print(channel);
        }
        writer.endList(selectFrame);
    }

    protected void parseParams(String k, String v) {
    }

    private void dealOption(SqlNode key, SqlNode value) {
        String k = ((NlsString) ((SqlCharStringLiteral) key).value).getValue();
        String v;
        if (value instanceof SqlCharStringLiteral) {
            Object vv = ((SqlCharStringLiteral) value).getValue();
            if (vv instanceof NlsString) {
                v = ((NlsString) vv).getValue();
            } else {
                v = vv.toString();
            }
        } else {
            v = value.toString();
        }
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
