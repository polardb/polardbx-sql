package com.alibaba.polardbx.gms.lbac.accessor;

import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.gms.lbac.component.ArraySecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;
import com.alibaba.polardbx.gms.lbac.component.ComponentType;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.SetSecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.TreeSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * 用于定义label component policy在sql语句以及metadb中的格式
 * <p>
 * 这个类不能使用LBACSecurityManager.getInstance方法，
 * 因为LBACSecurityManager.getInstance的初始化会使用当前类，造成循环初始化
 * 此类生成的component、policy、label的合法性需要外部保证，此类只负责格式正确性!!!
 *
 * @author pangzhaoxing
 */
public class LBACAccessorUtils extends AbstractAccessor {

    private static final String COMPONENT_INSTANCE_CONNECTOR = ":";

    private static final String TAG_CONNECTOR = ",";

    private static final String TREE_COUPLE_CONNECT = ";";

    private static final String COMPONENT_CONNECT = ",";

    public static LBACSecurityEntity loadESA(ResultSet rs) throws SQLException {
        int id = rs.getInt("entity_id");
        LBACSecurityEntity.EntityType type = LBACSecurityEntity.EntityType.valueOf(rs.getString("entity_type"));
        LBACSecurityEntity.EntityKey entityKey = parseEntityKey(rs.getString("entity_key"), type);
        String securityAttr = rs.getString("security_attr");
        return new LBACSecurityEntity(id, entityKey, type, securityAttr);
    }

    public static LBACSecurityLabelComponent loadSLC(ResultSet rs) throws SQLException {
        String componentName = rs.getString("component_name");
        ComponentType type = ComponentType.valueOf(rs.getString("component_type"));
        String componentContent = rs.getString("component_content");
        return createSecurityLabelComponent(componentName, type, componentContent);
    }

    public static LBACSecurityLabelComponent createSecurityLabelComponent(String componentName,
                                                                          ComponentType type,
                                                                          String componentContent) {
        switch (type) {
        case TREE:
            return createTreeSLC(componentName, componentContent);
        case SET:
            return createSetSLC(componentName, componentContent);
        case ARRAY:
            return createArraySLC(componentName, componentContent);
        default:
            throw new LBACException("unknown security component type");
        }
    }

    public static ArraySecurityLabelComponent createArraySLC(String componentName, String componentContent) {
        List<String> tags = Arrays.stream(componentContent.split(TAG_CONNECTOR))
            .map(String::trim)
            .map(String::toLowerCase)
            .collect(Collectors.toList());
        return new ArraySecurityLabelComponent(componentName, tags);
    }

    public static SetSecurityLabelComponent createSetSLC(String componentName, String componentContent) {
        Set<String> tags = Arrays.stream(componentContent.split(TAG_CONNECTOR))
            .map(String::trim)
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
        return new SetSecurityLabelComponent(componentName, tags);
    }

    public static TreeSecurityLabelComponent createTreeSLC(String componentName, String componentContent) {
        List<String> couples = Arrays.stream(componentContent.split(TREE_COUPLE_CONNECT))
            .map(String::trim)
            .collect(Collectors.toList());
        Map<String, TreeSecurityLabelComponent.Node> map = new HashMap<>();
        TreeSecurityLabelComponent.Node root = null;
        for (int i = 0; i < couples.size(); i++) {
            String couple = couples.get(i).substring(1, couples.get(i).length() - 1);//去掉括号
            String[] tags = couple.split(TAG_CONNECTOR);

            String parentTag = tags[0].trim().toLowerCase();
            String childTag = tags[1].trim().toLowerCase();
            map.putIfAbsent(parentTag, new TreeSecurityLabelComponent.Node(parentTag));
            map.putIfAbsent(childTag, new TreeSecurityLabelComponent.Node(childTag));
            map.get(parentTag).addChild(map.get(childTag));
            if (i == 0) {
                root = map.get(parentTag);
            }
        }
        return new TreeSecurityLabelComponent(componentName, root);
    }

    public static LBACSecurityPolicy loadSP(ResultSet rs) throws SQLException {
        String policyName = rs.getString("policy_name");
        String policyComponent = rs.getString("policy_components");
        return createSecurityPolicy(policyName, policyComponent);
    }

    public static LBACSecurityPolicy createSecurityPolicy(String policyName, String policyComponents) {
        String[] componentNames = Arrays.stream(policyComponents.split(COMPONENT_CONNECT))
            .map(String::trim)
            .map(String::toLowerCase)
            .toArray(String[]::new);
        return new LBACSecurityPolicy(policyName, componentNames);
    }

    public static LBACSecurityLabel createSecurityLabel(String labelName, String policyName,
                                                        String labelContent) {
        ComponentInstance[] instances = parseComponentInstance(labelContent);
        return new LBACSecurityLabel(labelName, policyName, instances);
    }

    public static LBACSecurityLabel loadSL(ResultSet rs) throws SQLException {

        String labelName = rs.getString("label_name");
        String policyName = rs.getString("label_policy");
        ComponentInstance[] components = parseComponentInstance(rs.getString("label_content"));
        return new LBACSecurityLabel(labelName, policyName, components);
    }

    private static ComponentInstance[] parseComponentInstance(String labelContent) {
        String[] comInstStrArr = labelContent.split(COMPONENT_INSTANCE_CONNECTOR, -1);//可能存在空的ComponentInstance
        ComponentInstance[] comInstArr = new ComponentInstance[comInstStrArr.length];
        for (int i = 0; i < comInstStrArr.length; i++) {
            String s = comInstStrArr[i];
            if (s.startsWith("(")) {
                s = comInstStrArr[i].substring(1, s.length() - 1);//去掉首尾的括号
            }
            Set<String> tags = s.length() == 0 ? Collections.emptySet() :
                Arrays.stream(s.split(TAG_CONNECTOR)).collect(Collectors.toSet());
            comInstArr[i] = new ComponentInstance(tags);
        }
        return comInstArr;
    }

    public static String getComponentContent(LBACSecurityLabelComponent component) {
        switch (component.getType()) {
        case TREE:
            return getComponentContent((TreeSecurityLabelComponent) component);
        case SET:
            return getComponentContent((SetSecurityLabelComponent) component);
        case ARRAY:
            return getComponentContent((ArraySecurityLabelComponent) component);
        default:
            throw new LBACException("unknown security component type");
        }
    }

    public static String getComponentContent(TreeSecurityLabelComponent component) {
        TreeSecurityLabelComponent.Node root = component.getRoot();
        StringJoiner sj = new StringJoiner(TREE_COUPLE_CONNECT);
        Queue<TreeSecurityLabelComponent.Node> queue = new LinkedList<>();
        queue.offer(root);
        while (!queue.isEmpty()) {
            TreeSecurityLabelComponent.Node poll = queue.poll();
            for (TreeSecurityLabelComponent.Node child : poll.getChildren()) {
                sj.add("(" + poll.getTag() + TAG_CONNECTOR + child.getTag() + ")");
                queue.offer(child);
            }
        }
        return sj.toString();
    }

    public static String getComponentContent(ArraySecurityLabelComponent component) {
        Set<String> allTags = component.getAllTags();
        String[] tags = new String[allTags.size()];
        for (String tag : allTags) {
            tags[component.getTagIndex(tag)] = tag;
        }
        StringJoiner sj = new StringJoiner(TAG_CONNECTOR);
        for (String tag : tags) {
            sj.add(tag);
        }
        return sj.toString();
    }

    public static String getComponentContent(SetSecurityLabelComponent component) {
        Set<String> allTags = component.getAllTags();
        StringJoiner sj = new StringJoiner(TAG_CONNECTOR);
        for (String tag : allTags) {
            sj.add(tag);
        }
        return sj.toString();
    }

    public static String getPolicyComponents(LBACSecurityPolicy policy) {
        StringJoiner sj = new StringJoiner(COMPONENT_CONNECT);
        for (String s : policy.getComponentNames()) {
            sj.add(s);
        }
        return sj.toString();
    }

    public static String getLabelComponent(LBACSecurityLabel label) {
        ComponentInstance[] instances = label.getComponents();
        StringJoiner sj = new StringJoiner(COMPONENT_INSTANCE_CONNECTOR);
        for (int i = 0; i < instances.length; i++) {
            String tags = "(";
            StringJoiner tmpSJ = new StringJoiner(TAG_CONNECTOR);
            for (String tag : instances[i].getTags()) {
                tmpSJ.add(tag);
            }
            tags += tmpSJ + ")";
            sj.add(tags);
        }
        return sj.toString();
    }

    public static String buildTableKey(String schema, String table) {
        return (schema + "." + table).toLowerCase();
    }

    public static String buildColumnKey(String schema, String table, String column) {
        return (schema + "." + table + "." + column).toLowerCase();
    }

    public static String buildUserKey(String user, String securityPolicy) {
        return (user + "." + securityPolicy).toLowerCase();
    }

    public static String buildEntityKey(LBACSecurityEntity.EntityKey key, LBACSecurityEntity.EntityType type) {
        switch (type) {
        case TABLE:
            return buildTableKey(key.getSchema(), key.getTable());
        case COLUMN:
            return buildColumnKey(key.getSchema(), key.getTable(), key.getColumn());
        case USER_WRITE:
        case USER_READ:
            return buildUserKey(key.getUser(), key.getPolicy());
        }
        return null;
    }

    public static LBACSecurityEntity.EntityKey parseEntityKey(String key, LBACSecurityEntity.EntityType type) {
        String[] strings = key.split("\\.");
        switch (type) {
        case TABLE:
            return LBACSecurityEntity.EntityKey.createTableKey(strings[0], strings[1]);
        case COLUMN:
            return LBACSecurityEntity.EntityKey.createColumnKey(strings[0], strings[1], strings[2]);
        case USER_WRITE:
        case USER_READ:
            return LBACSecurityEntity.EntityKey.createUserKey(PolarAccount.fromIdentifier(strings[0]).getIdentifier(),
                strings[1]);
        }
        return null;
    }

    public static void dropUserSecurityAttr(List<PolarAccountInfo> accountInfos, Connection conn) {
        LBACEntityAccessor attrAccessor = new LBACEntityAccessor();
        attrAccessor.setConnection(conn);
        for (PolarAccountInfo user : accountInfos) {
            List<LBACSecurityLabel> userReadLabels =
                LBACSecurityManager.getInstance().getUserLabel(user.getAccount(), true);
            for (LBACSecurityLabel label : userReadLabels) {
                if (label == null) {
                    continue;
                }
                LBACSecurityEntity.EntityKey key =
                    LBACSecurityEntity.EntityKey.createUserKey(user.getIdentifier(), label.getPolicyName());
                attrAccessor.deleteByKeyAndType(key, LBACSecurityEntity.EntityType.USER_READ);
            }
            List<LBACSecurityLabel> userWriteLabels =
                LBACSecurityManager.getInstance().getUserLabel(user.getAccount(), false);
            for (LBACSecurityLabel label : userWriteLabels) {
                if (label == null) {
                    continue;
                }
                LBACSecurityEntity.EntityKey key =
                    LBACSecurityEntity.EntityKey.createUserKey(user.getIdentifier(), label.getPolicyName());
                attrAccessor.deleteByKeyAndType(key, LBACSecurityEntity.EntityType.USER_WRITE);
            }
        }
    }

}
