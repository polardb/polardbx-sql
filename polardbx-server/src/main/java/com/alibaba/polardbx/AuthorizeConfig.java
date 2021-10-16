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

package com.alibaba.polardbx;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.taobao.tddl.common.privilege.EncrptPassword;
import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.GrantedUser;
import com.alibaba.polardbx.common.privilege.Host;
import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import com.taobao.tddl.common.privilege.RevokeParameter;
import com.alibaba.polardbx.config.UserConfig;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.DbPriv;
import com.alibaba.polardbx.common.model.TbPriv;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;

/**
 * 账号和权限相关配置, 目前只能用于DRDS模式下. 需要保证线程安全, 由于账号和授权并不是一个高频场景, 锁的粒度比较粗.
 *
 * @author arnkore 2016-11-28 10:12
 */
public class AuthorizeConfig extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory
        .getLogger(AuthorizeConfig.class);

    private Map<String/* user */, Map<String/* host */, UserConfig>> users = Maps.newHashMap();

    private Map<String/* dbName */, String/* appName */> dbInfos = Maps.newConcurrentMap();

    private ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();

    public AuthorizeConfig() {
    }

    /**
     * 用户是不是存在
     */
    public boolean userExists(String user, String host) {
        ReentrantReadWriteLock.ReadLock readLock = rwlock.readLock();
        readLock.lock();
        try {
            Map<String/* host */, UserConfig> host_ucMap = users.get(user);
            if (host_ucMap == null) {
                return false;
            }
            return host_ucMap.containsKey(host);
        } finally {
            readLock.unlock();
        }
    }

    public boolean userExists(String user) {
        return users != null && users.containsKey(user);
    }

    /**
     * 获取跟user和host匹配的用户, 优先完全匹配, 然后才是正则匹配. 首先判断user是否相等, 不相等直接返回null;
     * 然后判断host是否相等, 相等的话返回此用户, 如果不相等, 接下来寻找首次匹配的host, 返回此次匹配的用户.
     */
    public UserConfig getMatchedUser(String user, String host) {

        ReentrantReadWriteLock.ReadLock readLock = rwlock.readLock();
        readLock.lock();
        try {
            Map<String, UserConfig> host_ucMap = users.get(user);
            if (host_ucMap == null) {
                return null;
            }

            UserConfig _uc = host_ucMap.get(host);
            if (_uc != null) {
                return _uc;
            }

            return getMostAccurateMatching(host_ucMap.values(), host);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 获取最精确匹配的用户
     */
    private UserConfig getMostAccurateMatching(Collection<UserConfig> users, String host) {
        int longestMatchLength = -1;
        UserConfig resultUser = null;
        for (UserConfig uc : users) {
            if (uc.getHost().matches(host)) {
                int commonPrefixLen = getCommonPrefixLength(uc.getHost().getValue(), host);
                if (commonPrefixLen > longestMatchLength) {
                    longestMatchLength = commonPrefixLen;
                    resultUser = uc;
                }
            }
        }

        return resultUser;
    }

    /**
     * 获取公共前缀字符串的长度
     */
    private int getCommonPrefixLength(String host1, String host2) {
        String prefix = StringUtils.getCommonPrefix(new String[] {host1, host2});
        return prefix.length();
    }

    /**
     * 授予某个用户表级权限
     */
    public void addTbPriv(GrantParameter grantParam) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            for (GrantedUser grantedUser : grantParam.getGrantedUsers()) {
                UserConfig uc = getUserConfig(grantedUser.getUser(), grantedUser.getHost());
                if (uc == null) {
                    // 如果用户不存在, 需要创建用户.
                    uc = buildUserConfig(grantedUser, grantParam);
                    addDrdsUserToCache(uc);
                } else {
                    // 如果用户已存在, 且提供了密码, 需要更新密码
                    if (grantedUser.getEncrptPassword() != null) {
                        updatePassword(grantedUser);
                    }
                }
                String database = TStringUtil.normalizePriv(grantParam.getDatabase());
                String table = grantParam.getTable();
                Map<String, Map<String, TbPriv>> schemaTbPrivMap = uc.getSchemaTbPrivs();
                if (schemaTbPrivMap == null) {
                    schemaTbPrivMap = Maps.newHashMap();
                    uc.setSchemaTbPrivs(schemaTbPrivMap);
                }
                Map<String, TbPriv> tbPrivMap = schemaTbPrivMap.get(database);
                if (tbPrivMap == null) {
                    tbPrivMap = Maps.newHashMap();
                    schemaTbPrivMap.put(database, tbPrivMap);
                }
                TbPriv tbPriv = tbPrivMap.get(table);
                if (tbPriv == null) {
                    tbPriv = new TbPriv(database, table);
                    tbPrivMap.put(table, tbPriv);
                }
                PrivilegeUtil.addPrivilegePointsToTbPriv(tbPriv, grantParam);

                // Update schemas
                Set<String> schemas = uc.getSchemas();
                if (schemas == null) {
                    uc.setSchemas(Sets.newHashSet(grantParam.getDatabase()));
                } else if (!schemas.contains(grantParam.getDatabase())) {
                    schemas.add(grantParam.getDatabase());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 授予某个用户库级权限
     */
    public void addDbPriv(GrantParameter grantParam) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            for (GrantedUser grantedUser : grantParam.getGrantedUsers()) {
                UserConfig uc = getUserConfig(grantedUser.getUser(), grantedUser.getHost());
                if (uc == null) {
                    // 如果用户不存在, 需要创建用户.
                    uc = buildUserConfig(grantedUser, grantParam);
                    addDrdsUserToCache(uc);
                } else {
                    // 如果用户已存在, 且提供了密码, 需要更新密码
                    if (grantedUser.getEncrptPassword() != null) {
                        updatePassword(grantedUser);
                    }
                }

                String database = TStringUtil.normalizePriv(grantParam.getDatabase());
                Map<String, DbPriv> dbPrivMap = uc.getSchemaPrivs();
                if (dbPrivMap == null) {
                    dbPrivMap = Maps.newHashMap();
                    uc.setSchemaPrivs(dbPrivMap);
                }
                DbPriv dbPriv = dbPrivMap.get(database);
                if (dbPriv == null) {
                    dbPriv = new DbPriv(database);
                    dbPrivMap.put(database, dbPriv);
                }
                PrivilegeUtil.addPrivilegePointsToDbPriv(dbPriv, grantParam);

                // Update schemas
                Set<String> schemas = uc.getSchemas();
                if (schemas == null) {
                    uc.setSchemas(Sets.newHashSet(grantParam.getDatabase()));
                } else if (!schemas.contains(grantParam.getDatabase())) {
                    schemas.add(grantParam.getDatabase());
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    private UserConfig buildUserConfig(GrantedUser grantedUser, GrantParameter param) {
        UserConfig uc = new UserConfig();
        uc.setName(grantedUser.getUser());
        uc.setHost(new Host(grantedUser.getHost()));
        uc.setEncrptPassword(grantedUser.getEncrptPassword());
        uc.buildNewSchemaInfos(param);
        return uc;
    }

    /**
     * 移除某个用户的表级权限
     */
    public void removeTbPriv(RevokeParameter revokeParam) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            for (GrantedUser grantedUser : revokeParam.getGrantedUsers()) {
                UserConfig userConfig = getUserConfig(grantedUser.getUser(), grantedUser.getHost());
                if (userConfig != null) {
                    String database = TStringUtil.normalizePriv(revokeParam.getDatabase());
                    String table = revokeParam.getTable();
                    Map<String, Map<String, TbPriv>> schemaTbPrivMap = userConfig.getSchemaTbPrivs();
                    if (schemaTbPrivMap != null && schemaTbPrivMap.get(database) != null) {
                        TbPriv tbPriv = schemaTbPrivMap.get(database).get(table);
                        if (tbPriv != null) {
                            if (revokeParam.isRevokeAll()) {
                                schemaTbPrivMap.get(database).remove(table);
                            } else {
                                PrivilegeUtil.removePrivilegePointsFromTbPriv(tbPriv, revokeParam);
                            }
                            continue;
                        }
                    }
                    throw new TddlRuntimeException(ErrorCode.REVOKE_NO_SUCH_PRIVILEGE_EXCEPTION,
                        grantedUser.getUser(),
                        grantedUser.getHost(),
                        table);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 移除某个用户的DB权限
     */
    public void removeDbPriv(RevokeParameter revokeParam) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            for (GrantedUser grantedUser : revokeParam.getGrantedUsers()) {
                UserConfig userConfig = getUserConfig(grantedUser.getUser(), grantedUser.getHost());
                if (userConfig != null) {
                    Map<String, DbPriv> dbPrivMap = userConfig.getSchemaPrivs();
                    String database = TStringUtil.normalizePriv(revokeParam.getDatabase());
                    if (dbPrivMap != null) {
                        if (revokeParam.isRevokeAll()) {
                            dbPrivMap.remove(database);
                        } else {
                            DbPriv dbPriv = dbPrivMap.get(database);
                            PrivilegeUtil.removePrivilegePointsFromDbPriv(dbPriv, revokeParam);
                        }
                    }
                }
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 添加一个用户
     */
    public void addUser(String schema, GrantedUser grantedUser) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            Map<String/* host */, UserConfig> host_ucMap = users.get(grantedUser.getUser());
            if (host_ucMap == null) {
                host_ucMap = Maps.newHashMap();
                users.put(grantedUser.getUser(), host_ucMap);
            }

            UserConfig uc = UserConfig.createUserWithoutPrivs(schema, grantedUser);
            host_ucMap.put(grantedUser.getHost(), uc);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 移除一个用户
     */
    public void removeUser(String user, String host) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            Map<String/* host */, UserConfig> host_ucMap = users.get(user);
            if (host_ucMap == null) {
                return;
            }
            host_ucMap.remove(host);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 修改密码
     */
    public void updatePassword(GrantedUser grantedUser) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            UserConfig uc = getMatchedUser(grantedUser.getUser(), grantedUser.getHost());
            uc.setEncrptPassword(grantedUser.getEncrptPassword());
        } finally {
            writeLock.unlock();
        }
    }

    private void addDrdsUserToCache(UserConfig uc) {
        Map<String/* host */, UserConfig> host_ucMap = users.get(uc.getName());
        if (host_ucMap == null) {
            host_ucMap = Maps.newHashMap();
            users.put(uc.getName(), host_ucMap);
        }
        host_ucMap.put(uc.getHost().getValue(), uc);
    }

    private UserConfig getUserConfig(String user, String host) {
        Map<String/* host */, UserConfig> host_ucMap = users.get(user);
        if (host_ucMap == null) {
            return null;
        }

        return host_ucMap.get(host);
    }

    /**
     * 加载权限相关的信息
     *
     * @param dbInfos 数据库跟appname的对应关系信息
     * @param userInfos 用户密码信息
     * @param userDbInfos 用户跟数据库的对应关系数据
     * @param userDbPrivs 数据库级权限数据
     * @param userDbTbPrivs 表级权限数据
     */
    public void loadInfos(Map<String/* dbName */, String/* appName */> dbInfos,
                          Map<String/* user */, Map<String/* host */, String/*
                           * password
                           */>> userInfos,
                          Map<String/* user */, Map<String/* host */, Set<String/*
                           * dbName
                           */>>> userDbInfos,
                          Map<String/* user */, Map<String/* host */, Map<String/*
                           * dbName
                           */, DbPriv>>> userDbPrivs,
                          Map<String/* user */, Map<String/* host */, Map<String/*
                           * dbName
                           */, Map<String/*
                           * tbName
                           */, TbPriv>>>> userDbTbPrivs) {
        ReentrantReadWriteLock.WriteLock writeLock = rwlock.writeLock();
        writeLock.lock();
        try {
            // 防止推送空的配置
            reloadDbInfos(dbInfos);
            incrLoadUsers(userInfos, userDbInfos, userDbPrivs, userDbTbPrivs);

            // 检查是否存在已经被删除的只读账号(以xxx_ro)结尾，如果存在
            removeReadOnlyUserDeletedIfNeed(userInfos);

        } finally {
            writeLock.unlock();
        }
    }

    protected void removeReadOnlyUserDeletedIfNeed(Map<String/* user */, Map<String/*
     * host
     */, String/*
     * password
     */>> userInfos) {

        if (users != null) {

            for (Map.Entry<String, Map<String/* host */, UserConfig>> userItem : users.entrySet()) {
                String userName = userItem.getKey();
                Map<String/* host */, UserConfig> userConfigMap = userItem.getValue();

                if (userName.endsWith("_ro")) {
                    // 发现当前userName中存在只读用户

                    if (!userInfos.containsKey(userName)) {
                        // 发现当前的最新从Manager传过来的userName中已经不含有这个只读用户
                        // 说明，这个userName很可能被控制台删除了, 所以要清除
                        if (userConfigMap != null && userConfigMap.containsKey(Host.DEFAULT_HOST)) {
                            userConfigMap.remove(Host.DEFAULT_HOST);
                        }
                    }
                }

            }

        }
    }

    /**
     * 从数据库中增量加载db和appname的对应关系到缓存中(这里采用全局替换的方式, 因为这个缓存不存在被server端更新的可能)
     *
     * @param dbInfos 数据库跟appname的对应关系信息
     */
    private void reloadDbInfos(Map<String/* dbName */, String/* appName */> dbInfos) {
        // 防止推送空的配置
        if (dbInfos != null && !dbInfos.isEmpty()) {
            this.dbInfos = dbInfos;
        }
    }

    /**
     * 从数据库中增量加载用户到缓存中(有可能是用户创建了新的db, 或者做了迁移等操作, 需要将增量修改的数据加入到内存中,
     * 这里没有采用全局替换的方式, 而是增量加入到目前的缓存) 根据user_priv表中的所有记录加载UserConfig,
     * 库级权限表和表级权限表中的user,host组合如果无法在用户权限表中找到的话, 说明是垃圾数据.
     *
     * @param userInfos 用户密码信息
     * @param userDbInfos 用户跟数据库的对应关系数据
     * @param userDbPrivs 数据库级权限数据
     * @param userDbTbPrivs 表级权限数据
     */
    private void incrLoadUsers(Map<String/* user */, Map<String/* host */, String/*
     * password
     */>> userInfos,
                               Map<String/* user */, Map<String/* host */, Set<String/*
                                * dbName
                                */>>> userDbInfos,
                               Map<String/* user */, Map<String/* host */, Map<String/*
                                * dbName
                                */, DbPriv>>> userDbPrivs,
                               Map<String/* user */, Map<String/* host */, Map<String/*
                                * dbName
                                */, Map<String/*
                                * tbName
                                */, TbPriv>>>> userDbTbPrivs) {
        Map<String, Map<String, UserConfig>> newUsers = Maps.newHashMap();
        for (String user : userInfos.keySet()) {
            Map<String/* host */, UserConfig> host_ucMap = Maps.newHashMap();
            newUsers.put(user, host_ucMap);

            for (String host : userInfos.get(user).keySet()) {
                // 此时唯一确定一个UserConfig,
                UserConfig uc = new UserConfig();
                uc.setName(user);
                uc.setHost(new Host(host));
                String passwd = userInfos.get(user).get(host);
                if (passwd != null) {
                    uc.setEncrptPassword(new EncrptPassword(passwd, true));
                }
                // 1. 获取该用户的所有数据库
                uc.setSchemas(getUserSchemas(userDbInfos, user, host));
                // 2. 获取相应的库级权限
                uc.setSchemaPrivs(getUserDbPrivs(userDbPrivs, user, host));
                // 3. 获取相应的表级权限
                uc.setSchemaTbPrivs(getUserTbPrivs(userDbTbPrivs, user, host));
                host_ucMap.put(host, uc);
            }
        }

        users = newUsers;
    }

    private <T> T getGenericValInUser(Map<String/* user */, Map<String/* host */, T>> objMap, String user,
                                      String host) {
        if (objMap.containsKey(user)) {
            Map<String/* host */, T> hostObjMap = objMap.get(user);
            if (hostObjMap.containsKey(host)) {
                return hostObjMap.get(host);
            } else {
                for (String _host : hostObjMap.keySet()) {
                    Host h = new Host(_host);
                    if (h.matches(host)) {
                        return hostObjMap.get(host);
                    }
                }
            }
        }

        return null;
    }

    private Set<String> getUserSchemas(Map<String/* user */, Map<String/*
     * host
     */, Set<String/*
     * dbName
     */>>> userDbInfos, String user,
                                       String host) {
        return getGenericValInUser(userDbInfos, user, host);
    }

    private Map<String/* dbName */, DbPriv> getUserDbPrivs(Map<String/* user */, Map<String/*
     * host
     */, Map<String/*
     * dbName
     */, DbPriv>>> userDbPrivs,
                                                           String user, String host) {
        Map<String, DbPriv> tmp = getGenericValInUser(userDbPrivs, user, host);
        Map<String, DbPriv> schemaPrivs = Maps.newHashMap();

        if (tmp != null && tmp.keySet() != null && tmp.keySet().size() > 0) {
            for (String db : tmp.keySet()) {
                schemaPrivs.putIfAbsent(TStringUtil.normalizePriv(db), tmp.get(db));
            }
        }

        return schemaPrivs;
    }

    private Map<String/* dbName */, Map<String/* tbName */, TbPriv>> getUserTbPrivs(Map<String/*
     * user
     */, Map<String/*
     * host
     */, Map<String/*
     * dbName
     */, Map<String/*
     * tbName
     */, TbPriv>>>> userDbTbPrivs,
                                                                                    String user, String host) {
        Map<String, Map<String, TbPriv>> tmpDbTbPrivs = getGenericValInUser(userDbTbPrivs, user, host);
        Map<String, Map<String, TbPriv>> dbTbPrivs = Maps.newHashMap();

        if (tmpDbTbPrivs != null && tmpDbTbPrivs.keySet() != null && tmpDbTbPrivs.keySet().size() > 0) {
            for (String db : tmpDbTbPrivs.keySet()) {
                Map<String, TbPriv> tmpTbPrivs = tmpDbTbPrivs.get(db);

                if (tmpTbPrivs != null && tmpTbPrivs.keySet() != null && tmpTbPrivs.size() > 0) {
                    Map<String, TbPriv> tbPrivs = Maps.newHashMap();
                    for (String tb : tmpTbPrivs.keySet()) {
                        tbPrivs.putIfAbsent(TStringUtil.normalizePriv(tb), tmpTbPrivs.get(tb));
                    }
                    dbTbPrivs.putIfAbsent(TStringUtil.normalizePriv(db), tbPrivs);
                }
            }
        }

        return dbTbPrivs;
    }

    /**
     * 判断用户是否是系统账号
     */
    public boolean isSystemAccount(String user) {
        return isAdminAccount(user) || isReadOnlyAccount(user);
    }

    /**
     * 判断用户是否为管理员账号
     */
    public boolean isAdminAccount(String user) {
        ReentrantReadWriteLock.ReadLock readLock = rwlock.readLock();
        readLock.lock();
        try {
            return dbInfos.get(user) != null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * 判断是否为只读账号
     */
    public boolean isReadOnlyAccount(String user) {
        int ix = StringUtils.lastIndexOfIgnoreCase(user, "_RO");
        if (ix == -1) {
            return false;
        }

        String adminUser = user.substring(0, ix);
        return StringUtils.endsWithIgnoreCase(user, "_RO") && isAdminAccount(adminUser);
    }
}
