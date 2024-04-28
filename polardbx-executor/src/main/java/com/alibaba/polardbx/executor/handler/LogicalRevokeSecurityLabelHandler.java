package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.lbac.LBACPrivilegeCheckUtils;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDropSecurityLabel;
import org.apache.calcite.sql.SqlRevokeSecurityLabel;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_USER_NOT_EXISTS;

/**
 * @author pangzhaoxing
 */
public class LogicalRevokeSecurityLabelHandler extends HandlerCommon {

    public LogicalRevokeSecurityLabelHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!LBACPrivilegeCheckUtils.isHighPrivilege(executionContext.getPrivilegeContext().getPolarUserInfo())) {
            throw new LBACException("check privilege failed");
        }

        SqlRevokeSecurityLabel revokeLabel =
            (SqlRevokeSecurityLabel) ((LogicalDal) logicalPlan).getNativeSqlNode();

        String policyName = revokeLabel.getPolicyName().getSimple().toLowerCase();
        String accessType = revokeLabel.getAccessType().getSimple().toLowerCase();
        String user = revokeLabel.getUserName().getUser();
        String host = revokeLabel.getUserName().getHost();

        boolean readAccess = false;
        boolean writeAccess = false;
        if ("read".equalsIgnoreCase(accessType)) {
            readAccess = true;
        } else if ("write".equalsIgnoreCase(accessType)) {
            writeAccess = true;
        } else if ("all".equalsIgnoreCase(accessType)) {
            readAccess = true;
            writeAccess = true;
        } else {
            throw new LBACException("unknown access type");
        }

        LBACSecurityPolicy policy = LBACSecurityManager.getInstance().getPolicy(policyName);
        if (policy == null) {
            return new AffectRowCursor(0);
        }
        PolarAccountInfo accountInfo = PolarPrivManager.getInstance().getMatchUser(user, host);
        if (accountInfo == null) {
            return new AffectRowCursor(0);
        }

        List<LBACSecurityEntity> esaList = new ArrayList<>(2);
        if (readAccess) {
            LBACSecurityEntity esa = new LBACSecurityEntity();
            esa.setEntityKey(LBACSecurityEntity.EntityKey.createUserKey(accountInfo.getIdentifier(), policyName));
            esa.setType(LBACSecurityEntity.EntityType.USER_READ);
            esaList.add(esa);
        }
        if (writeAccess) {
            LBACSecurityEntity esa = new LBACSecurityEntity();
            esa.setEntityKey(LBACSecurityEntity.EntityKey.createUserKey(accountInfo.getIdentifier(), policyName));
            esa.setType(LBACSecurityEntity.EntityType.USER_WRITE);
            esaList.add(esa);
        }

        int affectRow = LBACSecurityManager.getInstance().deleteSecurityEntity(esaList);
        return new AffectRowCursor(affectRow);
    }
}
