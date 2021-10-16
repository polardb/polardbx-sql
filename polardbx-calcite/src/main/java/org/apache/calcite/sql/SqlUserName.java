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

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang.StringUtils;

/**
 * @author chenmo.cm
 * @date 2018/6/12 下午11:54
 */
public class SqlUserName extends SqlLiteral {
    private static final String DEFAULT_HOST = "%";

    private String user;
    private String host;

    /**
     * Creates a <code>SqlUserName</code>.
     *
     * @param pos
     */
    public SqlUserName(String value, String user, String host, SqlParserPos pos) {
        super(value, SqlTypeName.SYMBOL, pos);
        this.user = user;
        this.host = host;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(user);
        writer.print("@");
        writer.print(host);
    }

    public PolarAccount toPolarAccount() {
        return PolarAccount.newBuilder()
            .setUsername(user)
            .setHost(host)
            .build();
    }

    /**
     * host为空等同于 %
     * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/request-access.html">Request Verification</a>
     */
    public static SqlUserName from(MySqlUserName x) {
        String host = x.getHost() == null ? DEFAULT_HOST : x.getHost();
        return new SqlUserName(String.format("'%s'@'%s'", x.getUserName(), host),
            x.getUserName(), host,
            SqlParserPos.ZERO);
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    @Override
    public String toString() {
        if (StringUtils.isBlank(host)) {
            return user;
        } else {
            return String.format("%s@'%s'", user, host);
        }
    }
}
