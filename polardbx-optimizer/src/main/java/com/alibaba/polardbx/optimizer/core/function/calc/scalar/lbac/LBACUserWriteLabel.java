package com.alibaba.polardbx.optimizer.core.function.calc.scalar.lbac;

import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LBACUserWriteLabel extends AbstractScalarFunction {
    public LBACUserWriteLabel(List<DataType> operandTypes,
                                 DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LBAC_USER_WRITE_LABEL"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }
        String policyName = (String) args[0];
        LBACSecurityLabel label = LBACSecurityManager.getInstance()
            .getUserLabel(ec.getPrivilegeContext().getPolarUserInfo().getAccount(),
                policyName, false);
        return label == null ? null : label.getLabelName();
    }
}
