package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateSecurityPolicy;
import org.apache.calcite.sql.SqlDropSecurityLabel;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LogicalDropSecurityLabelHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalDropSecurityLabelHandler.class);

    public LogicalDropSecurityLabelHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlDropSecurityLabel dropSecurityLabel =
            (SqlDropSecurityLabel) ((LogicalDal) logicalPlan).getNativeSqlNode();
        List<LBACSecurityLabel> labels = new ArrayList<>();
        for (SqlIdentifier identifier : dropSecurityLabel.getLabelNames()) {
            String labelName = identifier.getSimple().toLowerCase();
            LBACSecurityLabel securityLabel = LBACSecurityManager.getInstance().getLabel(labelName);
            if (securityLabel == null) {
                continue;
            }
            labels.add(securityLabel);
        }
        int affectRow = LBACSecurityManager.getInstance().deleteSecurityLabel(labels);
        return new AffectRowCursor(affectRow);
    }
}
