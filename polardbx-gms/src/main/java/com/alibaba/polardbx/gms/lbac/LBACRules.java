package com.alibaba.polardbx.gms.lbac;

import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;

/**
 * @author pangzhaoxing
 */
public interface LBACRules {

    /**
     * 判断在read场景下，c1是否大于c2
     *
     * @param component c1、c2同属的SecurityLabelComponent
     */
    boolean canRead(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2);

    /**
     * 判断在write场景下，c1是否大于c2
     *
     * @param component c1、c2同属的SecurityLabelComponent
     */
    boolean canWrite(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2);

}
