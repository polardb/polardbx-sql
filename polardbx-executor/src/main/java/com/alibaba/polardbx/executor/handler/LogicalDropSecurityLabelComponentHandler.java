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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropSecurityLabelComponent;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author pangzhaoxing
 */
public class LogicalDropSecurityLabelComponentHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalDropSecurityLabelComponentHandler.class);

    public LogicalDropSecurityLabelComponentHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlDropSecurityLabelComponent dropSecurityLabelComponent =
            (SqlDropSecurityLabelComponent) ((LogicalDal) logicalPlan).getNativeSqlNode();
        List<LBACSecurityLabelComponent> components = new ArrayList<>();
        for (SqlIdentifier identifier : dropSecurityLabelComponent.getComponentNames()) {
            String componentName = identifier.getSimple().toLowerCase();
            LBACSecurityLabelComponent securityLabelComponent =
                LBACSecurityManager.getInstance().getComponent(componentName);

            if (securityLabelComponent == null) {
                continue;
            }

            for (LBACSecurityPolicy policy : LBACSecurityManager.getInstance().getPolicies()) {
                for (String component : policy.getComponentNames()) {
                    if (componentName.equalsIgnoreCase(component)) {
                        throw new LBACException("the component is used by policy, can not be dropped");
                    }
                }
            }
            components.add(securityLabelComponent);
        }

        int affectRow = LBACSecurityManager.getInstance().deleteSecurityLabelComponent(components);
        return new AffectRowCursor(affectRow);
    }
}
