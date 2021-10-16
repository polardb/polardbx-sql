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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowGrants;
import org.apache.calcite.sql.SqlUserName;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Show grants handler for polarx mode, compatible with mysql 8.0.
 *
 * @author bairui.lrj
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/show-grants.html">Show Grants</a>
 * @since 5.4.9
 */
public class PolarShowGrantsHandler extends HandlerCommon {

    public PolarShowGrantsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowGrants showGrants = (SqlShowGrants) show.getNativeSqlNode();

        PolarAccount user = showGrants.getUser()
            .map(SqlUserName::toPolarAccount)
            .orElse(executionContext.getPrivilegeContext().getPolarUserInfo().getAccount());

        List<PolarAccount> roles = showGrants.getRoles()
            .stream()
            .map(SqlUserName::toPolarAccount)
            .collect(Collectors.toList());

        ArrayResultCursor cursor = new ArrayResultCursor("Show Grants");
        cursor.addColumn(String.format("Grants for %s", user.getIdentifier()), DataTypes.StringType);

        List<String> grants =
            PolarPrivManager.getInstance().showGrants(
                executionContext.getPrivilegeContext().getPolarUserInfo(),
                executionContext.getPrivilegeContext().getActiveRoles(),
                user,
                roles);
        for (String grant : grants) {
            cursor.addRow(new Object[] {grant});
        }

        return cursor;
    }
}
