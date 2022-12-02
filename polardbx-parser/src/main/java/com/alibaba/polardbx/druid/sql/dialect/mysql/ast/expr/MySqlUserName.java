/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.Collections;
import java.util.List;

public class MySqlUserName extends MySqlExprImpl implements SQLName, Cloneable {
    public static int MAX_USER_NAME_IN_PROCEDURE = 32;
    public static int MAX_HOST_NAME_IN_PROCEDURE = 60;

    private String userName;
    private String host;
    private String identifiedBy;

    private long userNameHashCod64;
    private long hashCode64;

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;

        this.hashCode64 = 0;
        this.userNameHashCod64 = 0;
    }

    public String getNormalizeUserName() {
        return SQLUtils.normalize(userName);
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;

        this.hashCode64 = 0;
        this.userNameHashCod64 = 0;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public String getSimpleName() {
        StringBuilder buf = new StringBuilder();

        if (userName.length() == 0 || userName.charAt(0) != '\'') {
            buf.append('\'');
            buf.append(userName);
            buf.append('\'');
        } else {
            buf.append(userName);
        }

        buf.append('@');

        if (host.length() == 0 || host.charAt(0) != '\'') {
            buf.append('\'');
            buf.append(host);
            buf.append('\'');
        } else {
            buf.append(host);
        }

        if (identifiedBy != null) {
            buf.append(" identifiedBy by ");
            buf.append(identifiedBy);
        }

        return buf.toString();
    }

    public String getIdentifiedBy() {
        return identifiedBy;
    }

    public void setIdentifiedBy(String identifiedBy) {
        this.identifiedBy = identifiedBy;
    }

    public String toString() {
        return getSimpleName();
    }

    public MySqlUserName clone() {
        MySqlUserName x = new MySqlUserName();

        x.userName = userName;
        x.host = host;
        x.identifiedBy = identifiedBy;
        x.hashCode64 = hashCode64;
        x.userNameHashCod64 = userNameHashCod64;

        return x;
    }

    @Override
    public List<SQLObject> getChildren() {
        return Collections.emptyList();
    }

    public long nameHashCode64() {
        if (userNameHashCod64 == 0
            && userName != null) {
            userNameHashCod64 = FnvHash.hashCode64(userName);
        }
        return userNameHashCod64;
    }

    @Override
    public long hashCode64() {
        if (hashCode64 == 0) {
            if (host != null) {
                long hash = FnvHash.hashCode64(host);
                hash ^= '@';
                hash *= 0x100000001b3L;
                hash = FnvHash.hashCode64(hash, userName);

                hashCode64 = hash;
            } else {
                hashCode64 = nameHashCode64();
            }
        }

        return hashCode64;
    }

    @Override
    public SQLColumnDefinition getResolvedColumn() {
        return null;
    }

    // For roles
    public void verifyNoIdentify() {
        if (identifiedBy != null) {
            throw new ParserException(getSimpleName() + " should not contain password!");
        }
    }

    public static MySqlUserName fromIdentifier(SQLIdentifierExpr expr) {
        MySqlUserName mySqlUserName = new MySqlUserName();
        mySqlUserName.setUserName(unquote(expr.getName()));
        return mySqlUserName;
    }

    public static MySqlUserName fromCharExpr(SQLCharExpr expr) {
        MySqlUserName mySqlUserName = new MySqlUserName();
        mySqlUserName.setUserName(unquote(expr.getText()));
        return mySqlUserName;
    }

    public static MySqlUserName fromExpr(SQLExpr expr) {
        if (expr instanceof MySqlUserName) {
            MySqlUserName ret = (MySqlUserName) expr;
            ret.setUserName(unquote(ret.getUserName()));
            return ret;
        } else if (expr instanceof SQLIdentifierExpr) {
            return fromIdentifier((SQLIdentifierExpr) expr);
        } else if (expr instanceof SQLCharExpr) {
            return fromCharExpr((SQLCharExpr) expr);
        } else {
            throw new IllegalArgumentException("Can't convert " + expr + " to MysqlUserName");
        }
    }

    private static String unquote(String part) {
        if (part != null) {
            if (part.length() >= 2 && part.startsWith("'") && part.endsWith("'")) {
                return part.substring(1, part.length() - 1);
            }
        }

        return part;
    }
}
