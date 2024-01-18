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

package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntervalUnit;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumberExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLUnaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.LastInsertId;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime.Now;
import com.alibaba.polardbx.optimizer.parse.bean.PreparedParamRef;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.NlsString;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;

public class DrdsParameterizeSqlVisitor extends MySqlOutputVisitor {
    private static final BigInteger MAX_UNSIGNED_INT64 = new BigInteger(Long.toUnsignedString(0xffffffffffffffffL));

    private static final BigInteger MIN_SIGNED_INT64 = new BigInteger(Long.toString(-0x7fffffffffffffffL - 1));

    private boolean hasIn = false;

    public boolean isHasIn() {
        return hasIn;
    }

    private static final BigInteger MAX_SIGNED_INT64 = BigInteger.valueOf(Long.MAX_VALUE);

    public static class UserDefVariable {

        private String name;

        public UserDefVariable(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    public static class SysDefVariable {

        private String name;

        private boolean global;

        public SysDefVariable(String name, boolean global) {
            this.name = name;
            this.global = global;
        }

        public String getName() {
            return name;
        }

        public boolean isGlobal() {
            return global;
        }

    }

    public static class ConstantVariable {

        private String name;

        private Object[] args;

        public ConstantVariable(String name, Object args[]) {
            this.name = name;
            this.args = args;
        }

        public String getName() {
            return name;
        }

        public Object[] getArgs() {
            return args;
        }

        @Override
        public String toString() {
            return "?";
        }
    }

    private ExecutionContext executionContext;

    public DrdsParameterizeSqlVisitor(Appendable appender, boolean parameterized, ExecutionContext executionContext) {
        super(appender, parameterized);
        this.executionContext = executionContext;
        this.isMySQL80 = InstanceVersion.isMYSQL80();
        this.isMySQL80 = InstanceVersion.isMYSQL80();

        /*
         * We only consider session timezone variable, because currently it does not work for PolarDB-X
         * to set a global timezone variable.
         */
        String sessionTimezone;
        if (null != executionContext
            && MapUtils.isNotEmpty(executionContext.getServerVariables())
            && null != (sessionTimezone = (String) executionContext.getServerVariables().get("time_zone"))) {
            if ("SYSTEM".equalsIgnoreCase(sessionTimezone)) {
                sessionTimezone = (String) executionContext.getServerVariables().get("system_time_zone");
                if ("CST".equalsIgnoreCase(sessionTimezone)) {
                    sessionTimezone = "GMT+08:00";
                }
            }

            if (null != sessionTimezone) {
                final String trimmed = sessionTimezone.trim();
                if (trimmed.length() > 0 && ('+' == trimmed.charAt(0) || '-' == trimmed.charAt(0))) {
                    // Convert '+08:00' to 'GMT+08:00'
                    sessionTimezone = "GMT" + trimmed;
                } else if (!sessionTimezone.equals(trimmed)) {
                    sessionTimezone = trimmed;
                }
            }

            this.timezone = sessionTimezone;
        }
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        hasIn = true;
        return super.visit(x);
    }

    @Override
    public boolean visit(MySqlCharExpr x) {
        String mysqlCharset = x.getCharset();
        String mysqlCollate = x.getCollate();
        boolean isHex = x.isHex();

        if (this.parameterized && mysqlCharset != null) {
            print('?');
            incrementReplaceCunt();
            if (this.parameters != null) {
                String text;
                if (CharsetName.of(mysqlCharset) == CharsetName.BINARY) {
                    text = new String(x.getBinary(), StandardCharsets.ISO_8859_1); // store in latin1
                } else {
                    text = x.getText();
                }
                Charset sqlCharset = CharsetName.convertStrToJavaCharset(mysqlCharset);
                if (sqlCharset == null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARSER,
                        "unsupported mysql character set: " + mysqlCharset);
                } else {
                    SqlCollation sqlCollation =
                        new SqlCollation(sqlCharset, mysqlCollate, SqlCollation.Coercibility.EXPLICIT);
                    if (isHex) {
                        CharsetName charsetName = CharsetName.of(sqlCharset);
                        if (charsetName != CharsetName.BINARY) {
                            text = charsetName.toUTF16String(text);
                        }
                    }
                    NlsString value = new NlsString(text, mysqlCharset, sqlCollation);
                    this.parameters.add(value);
                }
            }
            return false;
        } else {
            return super.visit(x);
        }
    }

    @Override
    public boolean visit(SQLVariantRefExpr x) {
        String name = x.getName();

        if (StringUtils.startsWith(name, "@@") || x.isGlobal() || x.isSession()) {
            String varText;
            if (x.isGlobal() || x.isSession()) {
                varText = name;
            } else {
                varText = TStringUtil.substring(name, 2);
            }
            if (parameterized) {
                print('?');
                incrementReplaceCunt();

                String sysVariableName = varText;
                if ("identity".equalsIgnoreCase(varText)) {
                    sysVariableName = LastInsertId.NAME;
                }

                if (this.parameters != null) {
                    this.parameters.add(new SysDefVariable(sysVariableName, x.isGlobal()));
                }
            }
        } else if (StringUtils.startsWith(name, "@")) {
            if (parameterized) {
                String varText = TStringUtil.substring(name, 1);
                if (executionContext != null
                    && executionContext.getUserDefVariables() != null
                    && executionContext.getUserDefVariables().containsKey(varText.toLowerCase())
                    && executionContext.getUserDefVariables().get(varText.toLowerCase()) != null) {
                    print('?');
                    incrementReplaceCunt();

                    if (this.parameters != null) {
                        this.parameters.add(new UserDefVariable(varText));
                    } else {
                        super.visit(x);
                    }
                } else {
                    super.visit(x);
                }
            }
        } else if (StringUtils.equals(name, "?")) {
            visitPreparedParam(x);
        } else {
            super.visit(x);
        }
        return false;
    }

    private void visitPreparedParam(SQLVariantRefExpr x) {
        if (parameters != null) {
            parameters.add(new PreparedParamRef(x.getIndex()));
        }
        print('?');
    }

    @Override
    public boolean visit(SQLSetStatement x) {
        return false;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        if (x.isConnectByRoot()) {
            this.print0(this.ucase ? "CONNECT_BY_ROOT " : "connect_by_root ");
        }

        SQLExpr expr = x.getExpr();
        if (expr instanceof SQLIdentifierExpr) {
            this.print0(((SQLIdentifierExpr) expr).getName());
        } else if (expr instanceof SQLPropertyExpr) {
            this.visit((SQLPropertyExpr) expr);
        } else {
            this.printExpr(expr, this.parameterized);
        }

        String alias = x.getAlias();
        if (alias != null && alias.length() > 0) {
            this.print0(this.ucase ? " AS " : " as ");
            char c0 = alias.charAt(0);
            if (alias.indexOf(' ') != -1 && c0 != '\"' && c0 != '\'' && c0 != '`') {
                this.print('\"');
                this.print0(alias);
                this.print('\"');
            } else {
                this.print0(alias);
            }
        } else if (!(expr instanceof SQLAllColumnExpr)) {
            if (expr instanceof SQLPropertyExpr && ((SQLPropertyExpr) expr).getName().equalsIgnoreCase("*")) {
                return false;
            }
            String aliasNew = null;
            this.print0(this.ucase ? " AS " : " as ");
            if (expr instanceof SQLPropertyExpr) {
                final String name = ((SQLPropertyExpr) expr).getName();
                if (name.length() > 2 && '`' == name.charAt(0) && '`' == name.charAt(name.length() - 1)) {
                    aliasNew = name;
                } else {
                    aliasNew = "`" + SQLUtils.normalizeNoTrim(name) + "`";
                }
            } else if (expr instanceof SQLCharExpr) {
                aliasNew = quoteString(getAliasNew(((SQLCharExpr) expr).getText()));
            } else if (expr instanceof SQLIdentifierExpr) {
                aliasNew = ((SQLIdentifierExpr) expr).getName();
            } else {
                aliasNew = quoteString(SQLUtils.toMySqlString(expr));
            }
            this.print0(aliasNew);
        }

        return false;
    }

    private static final Set<String> METHOD_SKIP_PARAMETERIZE = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    static {
        // Don't parameterize the fractional temporal function.
//        METHOD_SKIP_PARAMETERIZE.add("NOW");
        METHOD_SKIP_PARAMETERIZE.add("CURTIME");
        METHOD_SKIP_PARAMETERIZE.add("CURRENT_TIME");
        METHOD_SKIP_PARAMETERIZE.add("CURRENT_TIMESTAMP");
        METHOD_SKIP_PARAMETERIZE.add("LOCALTIME");
        METHOD_SKIP_PARAMETERIZE.add("LOCALTIMESTAMP");
        METHOD_SKIP_PARAMETERIZE.add("SYSDATE");

        METHOD_SKIP_PARAMETERIZE.add("UTC_DATE");
        METHOD_SKIP_PARAMETERIZE.add("UTC_TIME");
        METHOD_SKIP_PARAMETERIZE.add("UTC_TIMESTAMP");
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        final String function = x.getMethodName();
        final boolean originParameterized = this.parameterized;
        if (METHOD_SKIP_PARAMETERIZE.contains(function)) {
            this.parameterized = false;
        }

        if (parameterized && LastInsertId.NAME.equalsIgnoreCase(function) && x.getArguments().size() == 0) {
            print('?');
            incrementReplaceCunt();

            String sysVariableName = LastInsertId.NAME;

            if (this.parameters != null) {
                this.parameters.add(new SysDefVariable(sysVariableName, false));
            }
            return false;
        }

        if (parameterized && Now.NAME.equalsIgnoreCase(function) && this.parameters != null) {

            String sysVariableName = Now.NAME;

            try {
                if (x.getArguments() == null || x.getArguments().size() == 0) {
                    print("cast(? as datetime)");
                    this.parameters.add(new ConstantVariable(sysVariableName, new Integer[0]));
                } else {
                    print("cast(? as datetime(" + Integer.valueOf(x.getArguments().get(0).toString()) + " ) )");
                    this.parameters.add(new ConstantVariable(sysVariableName,
                        new Integer[] {Integer.valueOf(x.getArguments().get(0).toString())}));
                }
                incrementReplaceCunt();
            } catch (NumberFormatException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS);
            }
            return false;
        }

        try {
            return super.visit(x);
        } finally {
            this.parameterized = originParameterized;
        }
    }

    @Override
    public boolean visit(SQLUnaryExpr x) {
        final boolean originParameterized = this.parameterized;
        if (x.getOperator() == SQLUnaryOperator.Negative
            && (x.getExpr() instanceof SQLIntegerExpr || x.getExpr() instanceof SQLNumberExpr)) {
            // The format like -(int number)
            this.parameterized = false;
        }
        try {
            return super.visit(x);
        } finally {
            this.parameterized = originParameterized;
        }
    }

    // todo HIGH RISK
//    @Override
//    public boolean visit(SQLHexExpr x) {
//        final boolean originParameterized = this.parameterized;
//        this.parameterized = true;
//        try {
//            return super.visit(x);
//        } finally {
//            this.parameterized = originParameterized;
//        }
//    }

    @Override
    public boolean visit(SQLIntervalExpr x) {
        this.print0(this.ucase ? "INTERVAL " : "interval ");
        SQLExpr value = x.getValue();
        this.printExpr(value, false);
        SQLIntervalUnit unit = x.getUnit();
        if (unit != null) {
            this.print(' ');
            this.print0(this.ucase ? unit.name() : unit.name_lcase);
        }

        return false;
    }

    @Override
    protected void printInteger(SQLIntegerExpr x, boolean parameterized) {
        if (parameterized && x.getNumber() instanceof BigInteger) {
            BigInteger number = (BigInteger) x.getNumber();

            // The boundary value of bigint is min value of longlong and max value of ulonglong.
            // otherwise, the big integer number will be recognized as decimal value.
            if (number.compareTo(MAX_UNSIGNED_INT64) > 0 || number.compareTo(MIN_SIGNED_INT64) < 0) {
                BigDecimal decimalNumber = new BigDecimal(number);
                x.setNumber(decimalNumber);
            } else if (number.compareTo(MAX_SIGNED_INT64) <= 0) {
                // for -9223372036854775808 ~ 9223372036854775807, use normal long value.
                x.setNumber(x.getNumber().longValue());
            }
        }

        super.printInteger(x, parameterized);
    }

    private String getAliasNew(String alias) {
        if (alias != null && alias.length() != 0) {
            char first = alias.charAt(0);
            if (first != '"' && first != '\'') {
                return alias;
            } else if (alias.length() == 1) {
                if (alias.charAt(0) == '\'') {
                    return "'\\" + alias.charAt(0) + "'";
                } else {
                    return "\'" + alias.charAt(0) + "\'";
                }
            } else {
                char[] chars = new char[(alias.length() - 2) * 2 + 4];
                int len = 1;

                for (int i = 1; i < alias.length() - 1; ++i) {
                    char ch = alias.charAt(i);
                    if (ch == '\\') {
                        ++i;
                        ch = alias.charAt(i);
                    }
                    chars[len++] = ch;
                }
                chars[0] = alias.charAt(0);
                chars[len++] = alias.charAt(alias.length() - 1);
                return new String(chars, 0, len);
            }
        } else {
            return alias;
        }
    }

    private String quoteString(String x) {
        boolean usingAnsiMode = false;
        int stringLength = x.length();
        StringBuffer buf = new StringBuffer((int) (x.length() * 1.1));
        buf.append('\'');

        for (int i = 0; i < stringLength; ++i) {
            char c = x.charAt(i);
            switch (c) {
            case 0:
                buf.append('\\');
                buf.append('0');
                break;
            case '\n':
                buf.append('\\');
                buf.append('n');
                break;
            case '\r':
                buf.append('\\');
                buf.append('r');
                break;
            case '\t':
                buf.append('\\');
                buf.append('t');
                break;
            case '\\':
                buf.append('\\');
                buf.append('\\');
                break;
            case '\'':
                buf.append('\\');
                buf.append('\'');
                break;
            case '"':
                if (usingAnsiMode) {
                    buf.append('\\');
                }
                buf.append('"');
                break;
            case '\032':
                buf.append('\\');
                buf.append('Z');
                break;
            default:
                buf.append(c);
            }
        }

        buf.append('\'');
        return buf.toString();
    }

    @Override
    public boolean visit(SQLOver x) {
        boolean parameterized = this.parameterized;
        this.parameterized = false;
        boolean visit = super.visit(x);
        this.parameterized = parameterized;
        return visit;
    }
}
