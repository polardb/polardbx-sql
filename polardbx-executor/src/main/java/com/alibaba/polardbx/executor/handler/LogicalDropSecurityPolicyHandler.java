package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropSecurityLabel;
import org.apache.calcite.sql.SqlDropSecurityPolicy;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LogicalDropSecurityPolicyHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalDropSecurityPolicyHandler.class);

    public LogicalDropSecurityPolicyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlDropSecurityPolicy dropSecurityPolicy =
            (SqlDropSecurityPolicy) ((LogicalDal) logicalPlan).getNativeSqlNode();
        List<LBACSecurityPolicy> policies = new ArrayList<>();
        for (SqlIdentifier identifier : dropSecurityPolicy.getPolicyNames()) {
            String policyName = identifier.getSimple().toLowerCase();
            LBACSecurityPolicy securityPolicy = LBACSecurityManager.getInstance().getPolicy(policyName);
            if (securityPolicy == null) {
                continue;
            }

            if (!securityPolicy.getLabelNames().isEmpty()) {
                throw new LBACException("can not drop a policy with labels : " + policyName);
            }
            policies.add(securityPolicy);
        }
        int affectRow = LBACSecurityManager.getInstance().deleteSecurityPolicy(policies);
        return new AffectRowCursor(affectRow);
    }
}
