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

package com.alibaba.polardbx.gms.lbac;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.lbac.accessor.LBACEntityAccessor;
import com.alibaba.polardbx.gms.lbac.accessor.LBACComponentAccessor;
import com.alibaba.polardbx.gms.lbac.accessor.LBACLabelAccessor;
import com.alibaba.polardbx.gms.lbac.accessor.LBACPolicyAccessor;
import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;
import com.alibaba.polardbx.gms.lbac.component.ComponentType;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pangzhaoxing
 */
public class LBACSecurityManager extends AbstractLifecycle {

    private static final LBACSecurityManager INSTANCE = new LBACSecurityManager();
    private Map<String, LBACSecurityLabel> labels = new ConcurrentHashMap<>();

    private Map<String, LBACSecurityPolicy> policies = new ConcurrentHashMap<>();

    private Map<String, LBACSecurityLabelComponent> components = new ConcurrentHashMap<>();

    //key format ： schema.table
    private Map<String, Map<String, String>> tableSecurityPolicies = new ConcurrentHashMap<>();

    //key format ： schema.table.column
    private Map<String, Map<String, Map<String, String>>> columnSecurityLabels = new ConcurrentHashMap<>();

    //key format ： user@host.security_policy
    private Map<String, Map<String, String>> userReadSecurityLabels = new ConcurrentHashMap<>();

    //key format ： user@host.security_policy
    private Map<String, Map<String, String>> userWriteSecurityLabels = new ConcurrentHashMap<>();

    private LBACSecurityManager() {
    }

    public static LBACSecurityManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        registerConfigListener();
        loadFromMetaDB();
    }

    protected void loadFromMetaDB() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            LBACComponentAccessor slcAccessor = new LBACComponentAccessor();
            LBACPolicyAccessor spAccessor = new LBACPolicyAccessor();
            LBACLabelAccessor slAccessor = new LBACLabelAccessor();
            LBACEntityAccessor esaAccessor = new LBACEntityAccessor();
            slcAccessor.setConnection(conn);
            spAccessor.setConnection(conn);
            slAccessor.setConnection(conn);
            esaAccessor.setConnection(conn);
            Map<String, LBACSecurityLabelComponent> newComponents = new ConcurrentHashMap<>();
            Map<String, LBACSecurityPolicy> newPolicies = new ConcurrentHashMap<>();
            Map<String, LBACSecurityLabel> newLabels = new ConcurrentHashMap<>();
            Map<String, Map<String, String>> newTableSecurityPolicies = new ConcurrentHashMap<>();
            Map<String, Map<String, Map<String, String>>> newColumnSecurityLabels = new ConcurrentHashMap<>();
            Map<String, Map<String, String>> newUserReadSecurityLabels = new ConcurrentHashMap<>();
            Map<String, Map<String, String>> newUserWriteSecurityLabels = new ConcurrentHashMap<>();

            //按照component、policy、label的顺序加载
            List<LBACSecurityLabelComponent> slcList = slcAccessor.queryAll();
            for (LBACSecurityLabelComponent c : slcList) {
                newComponents.put(c.getComponentName(), c);
            }
            this.components = newComponents;

            List<LBACSecurityPolicy> spList = spAccessor.queryAll();
            for (LBACSecurityPolicy p : spList) {
                newPolicies.put(p.getPolicyName(), p);
            }
            this.policies = newPolicies;

            List<LBACSecurityLabel> slList = slAccessor.queryAll();
            for (LBACSecurityLabel l : slList) {
                newLabels.put(l.getLabelName(), l);
                if (newPolicies.get(l.getPolicyName()) != null) {
                    newPolicies.get(l.getPolicyName()).addLabel(l.getLabelName());
                }
            }
            this.labels = newLabels;

            List<LBACSecurityEntity> esaList = esaAccessor.queryAll();
            for (LBACSecurityEntity a : esaList) {
                LBACSecurityEntity.EntityKey key = a.getEntityKey();
                switch (a.getType()) {
                case TABLE:
                    newTableSecurityPolicies.putIfAbsent(key.getSchema(), new ConcurrentHashMap<>());
                    newTableSecurityPolicies.get(key.getSchema()).put(key.getTable(), a.getSecurityAttr());
                    break;
                case COLUMN:
                    newColumnSecurityLabels.putIfAbsent(key.getSchema(), new ConcurrentHashMap<>());
                    newColumnSecurityLabels.get(key.getSchema()).putIfAbsent(key.getTable(), new ConcurrentHashMap<>());
                    newColumnSecurityLabels.get(key.getSchema()).get(key.getTable())
                        .put(key.getColumn(), a.getSecurityAttr());
                    break;
                case USER_READ:
                    newUserReadSecurityLabels.putIfAbsent(key.getUser(), new ConcurrentHashMap<>());
                    newUserReadSecurityLabels.get(key.getUser()).put(key.getPolicy(), a.getSecurityAttr());
                    break;
                case USER_WRITE:
                    newUserWriteSecurityLabels.putIfAbsent(key.getUser(), new ConcurrentHashMap<>());
                    newUserWriteSecurityLabels.get(key.getUser()).put(key.getPolicy(), a.getSecurityAttr());
                    break;
                default:
                    //ignore
                    //throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_LBAC_SECURITY_COMPONENT_TYPE);
                }
            }
            this.tableSecurityPolicies = newTableSecurityPolicies;
            this.columnSecurityLabels = newColumnSecurityLabels;
            this.userReadSecurityLabels = newUserReadSecurityLabels;
            this.userWriteSecurityLabels = newUserWriteSecurityLabels;
        } catch (Throwable ex) {
            throw GeneralUtil.nestedException(ex);
        }
    }

    public LBACSecurityPolicy getPolicy(String policyName) {
        return policies.get(policyName.toLowerCase());
    }

    public LBACSecurityLabelComponent getComponent(String componentName) {
        return components.get(componentName.toLowerCase());
    }

    public LBACSecurityLabel getLabel(String labelName) {
        return labels.get(labelName.toLowerCase());
    }

    public Collection<LBACSecurityPolicy> getPolicies() {
        return policies.values();
    }

    public LBACSecurityPolicy getTablePolicy(String schema, String table) {
        Map<String, String> map = tableSecurityPolicies.get(schema);
        if (map == null) {
            return null;
        }
        String policyName = map.get(table);
        return policyName == null ? null : getPolicy(policyName);
    }

    public LBACSecurityLabel getColumnLabel(String schema, String table, String column) {
        Map<String, Map<String, String>> map1 = columnSecurityLabels.get(schema);
        if (map1 == null) {
            return null;
        }
        Map<String, String> map2 = map1.get(table);
        if (map2 == null) {
            return null;
        }
        String labelName = map2.get(column);
        return labelName == null ? null : labels.get(labelName);
    }

    public LBACSecurityLabel getUserLabel(PolarAccount account, String policy, boolean read) {
        Map<String, String> map = read ? userReadSecurityLabels.get(account.getIdentifier()) :
            userWriteSecurityLabels.get(account.getIdentifier());
        if (map == null) {
            return null;
        }
        String labelName = map.get(policy);
        return labelName == null ? null : labels.get(labelName);
    }

    public List<LBACSecurityLabel> getUserLabel(PolarAccount account, boolean read) {
        Map<String, String> map = read ? userReadSecurityLabels.get(account.getIdentifier()) :
            userWriteSecurityLabels.get(account.getIdentifier());
        if (map == null) {
            return Collections.emptyList();
        }
        List<LBACSecurityLabel> userLabels = new ArrayList<>();
        for (String labelName : map.values()) {
            userLabels.add(labels.get(labelName));
        }
        return userLabels;
    }

    public boolean validatePolicy(LBACSecurityPolicy policy) {
        for (String s : policy.getComponentNames()) {
            if (getComponent(s) == null) {
                return false;
            }
        }
        return true;
    }

    public boolean validateComponent(LBACSecurityLabelComponent component) {
        return true;
    }

    public boolean validateLabel(LBACSecurityLabel label) {
        LBACSecurityPolicy policy = getPolicy(label.getPolicyName());
        if (policy == null) {
            return false;
        }
        String[] componentNames = policy.getComponentNames();
        ComponentInstance[] instances = label.getComponents();
        if (componentNames.length != instances.length) {
            return false;
        }
        for (int i = 0; i < componentNames.length; i++) {
            LBACSecurityLabelComponent component = getComponent(componentNames[i]);
            if (component == null) {
                return false;
            }
            ComponentInstance instance = instances[i];
            if (component.getType() == ComponentType.ARRAY) {
                if (instance.getTags().size() != 1) {
                    return false;
                }
            }
            for (String tag : instance.getTags()) {
                if (!component.containTag(tag)) {
                    return false;
                }
            }
        }
        return true;
    }

    public int insertSecurityEntity(List<LBACSecurityEntity> securityEntities) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            try {
                LBACEntityAccessor accessor = new LBACEntityAccessor();
                accessor.setConnection(connection);
                connection.setAutoCommit(false);
                int affectRow = 0;
                for (LBACSecurityEntity entity : securityEntities) {
                    affectRow += accessor.insert(entity);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
                connection.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
                return affectRow;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LBACException(e);
        }
    }

    public int deleteSecurityLabelComponent(List<LBACSecurityLabelComponent> securityLabelComponents) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            try {
                LBACComponentAccessor accessor = new LBACComponentAccessor();
                accessor.setConnection(connection);
                connection.setAutoCommit(false);
                int affectRow = 0;
                for (LBACSecurityLabelComponent component : securityLabelComponents) {
                    affectRow += accessor.delete(component);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
                connection.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
                return affectRow;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LBACException(e);
        }
    }

    public int deleteSecurityPolicy(List<LBACSecurityPolicy> securityPolicies) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            try {
                LBACPolicyAccessor accessor = new LBACPolicyAccessor();
                accessor.setConnection(connection);
                connection.setAutoCommit(false);
                int affectRow = 0;
                for (LBACSecurityPolicy policy : securityPolicies) {
                    affectRow += accessor.delete(policy);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
                connection.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
                return affectRow;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LBACException(e);
        }
    }

    public int deleteSecurityLabel(List<LBACSecurityLabel> securityLabels) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            try {
                LBACLabelAccessor accessor = new LBACLabelAccessor();
                accessor.setConnection(connection);
                connection.setAutoCommit(false);
                int affectRow = 0;
                for (LBACSecurityLabel label : securityLabels) {
                    affectRow += accessor.delete(label);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
                connection.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
                return affectRow;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LBACException(e);
        }
    }

    public int deleteSecurityEntity(List<LBACSecurityEntity> securityEntities) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            try {
                LBACEntityAccessor accessor = new LBACEntityAccessor();
                accessor.setConnection(connection);
                connection.setAutoCommit(false);
                int affectRow = 0;
                for (LBACSecurityEntity entity : securityEntities) {
                    affectRow += accessor.deleteByKeyAndType(entity);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getLBACSecurityDataId(), connection);
                connection.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.getLBACSecurityDataId());
                return affectRow;
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new LBACException(e);
        }
    }

    public boolean validateSecurityEntity(LBACSecurityEntity esa, String tablePolicy) {
        switch (esa.getType()) {
        case TABLE:
            return getPolicy(esa.getSecurityAttr()) != null;
        case COLUMN:
        case USER_READ:
        case USER_WRITE:
            LBACSecurityLabel label = getLabel(esa.getSecurityAttr());
            if (label == null) {
                return false;
            }
            return label.getPolicyName().equalsIgnoreCase(tablePolicy);
        }
        return false;
    }

    public List<Pair<String, String>> getAllTableWithPolicy() {
        List<Pair<String, String>> tables = new ArrayList<>();
        for (Map.Entry<String, Map<String, String>> entry : tableSecurityPolicies.entrySet()) {
            String schema = entry.getKey();
            for (String table : entry.getValue().keySet()) {
                tables.add(Pair.of(schema, table));
            }
        }
        return tables;
    }

    private void registerConfigListener() {
        MetaDbConfigManager.getInstance()
            .register(MetaDbDataIdBuilder.getLBACSecurityDataId(), null);
        MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.getLBACSecurityDataId(),
            new LBACSecurityConfigListener());
    }

    protected static class LBACSecurityConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            LBACSecurityManager.getInstance().loadFromMetaDB();
        }
    }

}
