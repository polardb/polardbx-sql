package com.alibaba.polardbx.qatest.ddl.cdc.util;

import com.alibaba.polardbx.cdc.SQLHelper;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.TDDLHint;
import com.alibaba.polardbx.executor.ddl.ImplicitTableGroupUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;

import static com.alibaba.polardbx.qatest.util.PropertiesUtil.POLARDBX_SERVER_ID_CONF;

/**
 * created by ziyang.lb
 **/
public class CdcTestUtil {

    public static String getServerId4Check(String serverId) {
        if (serverId == null && StringUtils.containsIgnoreCase(PropertiesUtil.getConnectionProperties(),
            POLARDBX_SERVER_ID_CONF)) {
            return "181818";
        }
        return serverId;
    }

    public static void removeDdlIdComments(SQLStatement stmt) {
        String searchSeed = "/*DDL_ID";
        if (stmt.hasBeforeComment()) {
            stmt.getBeforeCommentsDirect().removeIf(c -> org.apache.commons.lang.StringUtils.contains(c, searchSeed));
        }

        if (stmt.getHeadHintsDirect() != null) {
            stmt.getHeadHintsDirect().forEach(hint -> {
                if (hint instanceof TDDLHint) {
                    TDDLHint tddlHint = (TDDLHint) hint;
                    if (tddlHint.hasBeforeComment()) {
                        tddlHint.getBeforeCommentsDirect()
                            .removeIf(c -> org.apache.commons.lang.StringUtils.contains(c, searchSeed));
                    }
                }
            });
        }
    }

    public static String removeImplicitTgSyntax(String sql) {
        SQLStatement statement = SQLHelper.parseSql(sql);
        ImplicitTableGroupUtil.removeImplicitTgSyntax(statement);
        return statement.toString();
    }
}
