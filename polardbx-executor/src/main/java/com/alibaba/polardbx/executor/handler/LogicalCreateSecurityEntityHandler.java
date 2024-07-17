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
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.accessor.LBACAccessorUtils;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSecurityEntity;

import java.util.Collections;

/**
 * @author pangzhaoxing
 */
public class LogicalCreateSecurityEntityHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalCreateSecurityEntityHandler.class);

    public LogicalCreateSecurityEntityHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlCreateSecurityEntity createSecurityEntity =
            (SqlCreateSecurityEntity) ((LogicalDal) logicalPlan).getNativeSqlNode();

        LBACSecurityEntity.EntityType entityType =
            LBACSecurityEntity.EntityType.valueOf(createSecurityEntity.getEntityType().getSimple().toUpperCase());
        LBACSecurityEntity.EntityKey entityKey =
            LBACAccessorUtils.parseEntityKey(createSecurityEntity.getEntityKey().getSimple(), entityType);
        String attrName = createSecurityEntity.getEntityAttr().getSimple().toLowerCase();
        if (entityType == LBACSecurityEntity.EntityType.TABLE) {
            if (LBACSecurityManager.getInstance().getPolicy(attrName) == null) {
                throw new LBACException("the policy is not exist");
            }
        } else {
            if (LBACSecurityManager.getInstance().getLabel(attrName) == null) {
                throw new LBACException("the label is not exist");
            }
        }

        LBACSecurityEntity securityEntity = new LBACSecurityEntity(entityKey, entityType, attrName);
        int affectRow =
            LBACSecurityManager.getInstance().insertSecurityEntity(Collections.singletonList(securityEntity));
        return new AffectRowCursor(affectRow);

    }

}
