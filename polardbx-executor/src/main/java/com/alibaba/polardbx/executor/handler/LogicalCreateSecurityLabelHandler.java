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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.accessor.LBACAccessorUtils;
import com.alibaba.polardbx.gms.lbac.accessor.LBACLabelAccessor;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSecurityLabel;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author pangzhaoxing
 */
public class LogicalCreateSecurityLabelHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateSecurityLabelHandler.class);

    public LogicalCreateSecurityLabelHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlCreateSecurityLabel createLabel =
            (SqlCreateSecurityLabel) ((LogicalDal) logicalPlan).getNativeSqlNode();

        String labelName = createLabel.getLabelName().getSimple().toLowerCase();//全部小写
        String policyName = createLabel.getPolicyName().getSimple().toLowerCase();//全部小写
        String labelContent = createLabel.getLabelContent().getNlsString().getValue().toLowerCase();//全部小写
        LBACSecurityLabel label = LBACAccessorUtils.createSecurityLabel(labelName, policyName, labelContent);
        if (!LBACSecurityManager.getInstance().validateLabel(label)) {
            throw new LBACException("security label is not invalid");
        }

        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            LBACLabelAccessor slAccessor = new LBACLabelAccessor();
            slAccessor.setConnection(connection);
            int affectRow = slAccessor.insert(label);

            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
            return new AffectRowCursor(affectRow);
        } catch (SQLException e) {
            throw new TddlNestableRuntimeException(e);
        }
    }
}
