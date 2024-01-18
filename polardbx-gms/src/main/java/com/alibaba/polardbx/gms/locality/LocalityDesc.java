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

package com.alibaba.polardbx.gms.locality;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoRecord;
import lombok.Value;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Descriptor of locality
 * Format:
 * single datanode: dn=dn1
 * multi datanodes: dn=[dn1,dn2]
 *
 * @author moyi
 * @since 2021/03
 */
public class LocalityDesc {

    //TODO: locality="dn=dn1,dn2;balance_single_table=on/off;storage_group=g1;"
    //TODO: locality="hash_range_sequential_placement=on;"
    public static final String DN_PREFIX = "dn=";

    public static final String BALANCE_PREFIX = "balance_single_table=";
    //TODO: locality="dn=dn1,dn2;balance_single_table=on/off;storage_groups=g1;"

    public static final String BALANCE_SINGLE_TABLE_ON = "balance_single_table=on";

    private static final String SEQUENTIAL_PLACEMENT_PREFIX = "hash_range_sequential_placement=";

    public static final String STORAGE_POOL_PREFIX = "storage_pools=";

    private static final String PRIMARY_STORAGE_POOL_PREFIX = "primary_storage_pool=";

    private String primaryStoragePoolName;

    // list of datanode instance
    private Set<String> dnSet;

    private Set<String> fullDnSet;

    private List<String> dnList;

    private List<String> fullDnList;

    public Boolean getBalanceSingleTable() {
        return balanceSingleTable;
    }

    public void setBalanceSingleTable(Boolean balanceSingleTable) {
        this.balanceSingleTable = balanceSingleTable;
    }

    private Boolean balanceSingleTable;

    private Boolean hashRangeSequentialPlacement;

    private List<String> storagePoolNames = new ArrayList<>();

    private String primaryDnId;

    public LocalityDesc() {
        this.dnSet = new HashSet<>();
        this.dnList = new ArrayList<>();
        this.fullDnSet = new HashSet<>();
        this.fullDnList = new ArrayList<>();
        this.balanceSingleTable = false;
        this.hashRangeSequentialPlacement = false;
    }

    public LocalityDesc(List<String> dnList) {
        this.dnSet = dnList.stream().sorted().collect(Collectors.toSet());
        this.dnList = dnSet.stream().collect(Collectors.toList());
        this.fullDnSet = dnList.stream().sorted().collect(Collectors.toSet());
        this.fullDnList = fullDnSet.stream().collect(Collectors.toList());
        this.balanceSingleTable = false;
        this.hashRangeSequentialPlacement = false;
    }

    public LocalityDesc(Set<String> dnSet) {
        this.dnSet = dnSet;
        this.dnList = dnSet.stream().collect(Collectors.toList());
        this.fullDnSet = dnSet;
        this.fullDnList = fullDnList.stream().collect(Collectors.toList());
        this.balanceSingleTable = false;
        this.hashRangeSequentialPlacement = false;
    }

    public Boolean hasStoragePoolDefinition() {
        return !this.storagePoolNames.isEmpty();
    }

    public static LocalityDesc parse(String str) {
        LocalityDesc result = new LocalityDesc();
        if (str == null) {
            return result;
        }
        if (str.endsWith("'") && str.startsWith("'")) {
            str = str.substring(1, str.length() - 1);
        }
        str = StringUtils.trim(str);
        str = str.toLowerCase();
        if (str.startsWith("'") && str.endsWith("'") && str.length() >= 2) {
            str = str.substring(1, str.length() - 1);
        }

        // json
        if (str.startsWith("{")) {
            LocalityJSON json = JSON.parseObject(str, LocalityJSON.class);
            str = json.getLocality();
        }

        if (TStringUtil.isBlank(str)) {
            return result;
        }

        List<String> options = Arrays.asList(str.split(";"));
        for (int i = 0; i < options.size(); i++) {
            String option = options.get(i);
            if (option.startsWith(DN_PREFIX)) {
                String dnString = StringUtils.trim(StringUtils.removeStart(option, DN_PREFIX));
                if (!dnString.isEmpty()) {
                    String[] dns = StringUtils.trim(StringUtils.removeStart(option, DN_PREFIX)).split(",");
                    Set<String> dnSet = Arrays.asList(dns).stream().map(String::trim).collect(Collectors.toSet());
                    result.setDnSet(dnSet);
                    result.setFullDnSet(dnSet);
                }
            } else if (option.startsWith(BALANCE_PREFIX)) {
                Map<String, Boolean> flagMap = new HashMap<String, Boolean>() {{
                    put("true", true);
                    put("false", false);
                    put("on", true);
                    put("off", false);
                }};
                String balanceFlag = StringUtils.trim(StringUtils.removeStart(option, BALANCE_PREFIX));
                if (flagMap.containsKey(balanceFlag)) {
                    result.balanceSingleTable = flagMap.get(balanceFlag);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                        "invalid locality: '%s', balance_single_table option value illegal '%s', must be 'true' or 'false'",
                        str, balanceFlag));
                }
            } else if (option.startsWith(SEQUENTIAL_PLACEMENT_PREFIX)) {
                Map<String, Boolean> flagMap = new HashMap<String, Boolean>() {{
                    put("true", true);
                    put("false", false);
                    put("on", true);
                    put("off", false);
                }};
                String sequentialPlacement =
                    StringUtils.trim(StringUtils.removeStart(option, SEQUENTIAL_PLACEMENT_PREFIX));
                if (flagMap.containsKey(sequentialPlacement)) {
                    result.hashRangeSequentialPlacement = flagMap.get(sequentialPlacement);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS, String.format(
                        "invalid locality: '%s', sequential_placement option value illegal '%s', must be 'true' or 'false'",
                        str, sequentialPlacement));
                }
            } else if (option.startsWith(STORAGE_POOL_PREFIX)) {
                StoragePoolSpecParser storagePoolSpecParser = new StoragePoolSpecParser(option);
                String[] storagePoolNames = storagePoolSpecParser.getStoragePoolNames();
                String primaryStoragePool = storagePoolSpecParser.getPrimaryStoragePool();
                if (storagePoolNames != null && storagePoolNames.length > 0) {
                    result.setStoragePoolNames(Arrays.stream(storagePoolNames).collect(Collectors.toList()));
                    if (StringUtils.isEmpty(primaryStoragePool)) {
                        primaryStoragePool = storagePoolNames[0];
                    }
                    result.setPrimaryStoragePoolName(primaryStoragePool);
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                    String.format("invalid locality: '%s', must start with '%s' or '%s' or '%s' or be empty string.",
                        str, DN_PREFIX, BALANCE_PREFIX, STORAGE_POOL_PREFIX));
            }
        }
        return result;
    }

    public LocalityDesc removeStoragePool(Set<String> storagePools) {
        this.storagePoolNames.removeAll(storagePools);
        return this;
    }

    public List<String> getDnList() {
        return this.dnList;
    }

    public void setFullDnSet(Set<String> dnSet) {
        this.fullDnSet = dnSet;
    }

    public Set<String> getFullDnSet() {
        return fullDnSet;
    }

    public String getPrimaryDnId() {
        return primaryDnId;
    }

    public void setPrimaryDnId(String dnInst) {
        this.primaryDnId = dnInst;
    }

    public List<String> getFullDnList() {
        return fullDnList;
    }

    public void setFullDnList(List<String> dnList) {
        this.fullDnList = dnList;
    }

    public void setDnList(List<String> dnList) {
        this.dnList = dnList;
    }

    public Set<String> getDnSet() {
        return this.dnSet;
    }

    public void setStoragePoolNames(List<String> storagePoolNames) {
        this.storagePoolNames = storagePoolNames;
    }

    public List<String> getStoragePoolNames() {
        return this.storagePoolNames;
    }

    public String getPrimaryStoragePoolName() {
        return this.primaryStoragePoolName;
    }

    public void setPrimaryStoragePoolName(String primaryStoragePoolName) {
        this.primaryStoragePoolName = primaryStoragePoolName;
    }

    public void setDnSet(Set<String> dnSet) {
        this.dnSet = dnSet;
        this.dnList = dnSet.stream().collect(Collectors.toList());
    }

    public Boolean hasBalanceSingleTable() {
        return balanceSingleTable;
    }

    public Boolean getHashRangeSequentialPlacement() {
        return hashRangeSequentialPlacement;
    }

    //localityDesc 1 >= localityDesc 2
    public boolean compactiableWith(LocalityDesc localityDesc) {
        if (dnSet == null || dnSet.isEmpty()) {
            return true;
        } else if (localityDesc.holdEmptyDnList()) {
            return true;
        } else {
            return dnSet.containsAll(localityDesc.dnSet);
        }
    }

    public boolean fullCompactiableWith(LocalityDesc localityDesc) {
        if (dnSet == null || dnSet.isEmpty()) {
            return true;
        } else if (localityDesc.holdEmptyDnList()) {
            return true;
        } else {
            return fullDnSet.containsAll(localityDesc.fullDnSet);
        }
    }

    // default dn Set.
    public boolean matchStorageInstance(String storage) {
        return this.dnSet == null || this.dnSet.isEmpty() || this.dnSet.contains(storage);
    }

    public boolean fullMatchStorageInstance(String storage) {
        return this.fullDnSet == null || this.fullDnSet.isEmpty() || this.fullDnSet.contains(storage);
    }

    /**
     * Choose a group according to locality
     */
    public Optional<GroupDetailInfoRecord> chooseGroup(List<GroupDetailInfoRecord> groups) {
        return groups.stream()
            .sorted(Comparator.comparing(x -> x.groupName))
            .filter(x -> this.dnSet.contains(x.storageInstId))
            .findFirst();
    }

    public boolean isEmpty() {
        return this.toString() == null || this.toString().isEmpty();
    }

    public boolean holdEmptyDnList() {
        return this.dnSet == null || this.dnSet.isEmpty();
    }

    public boolean holdEmptyLocality() {
        return (this.dnSet == null || this.dnSet.isEmpty()) && this.storagePoolNames.isEmpty();
    }

    public String getDnString() {
        String result = "";
        if (!this.holdEmptyDnList()) {
            result += "dn=" + StringUtils.join(this.dnList, ",");
        }
        return result;
    }

    public Boolean match(LocalityDesc localityDesc) {
        return this.getDnString().equals(localityDesc.getDnString());
    }

    @Override
    public String toString() {
        String result = "";
        List<String> options = new ArrayList<>();
        if (this.balanceSingleTable) {
            options.add("balance_single_table=on");
        }
        if (this.hashRangeSequentialPlacement) {
            options.add("hash_range_sequential_placement=on");
        }
        if (!storagePoolNames.isEmpty()) {
            options.add("storage_pools=" + "\"" + StringUtils.join(this.storagePoolNames, ",") + "\"");
            options.add("primary_storage_pool=" + "\"" + this.primaryStoragePoolName + "\"");
        } else if (!this.holdEmptyDnList()) {
            options.add("dn=" + StringUtils.join(this.dnSet, ","));
        }
        if (!options.isEmpty()) {
            result = StringUtils.join(options, ",");
        }
        return result;
    }

    public String showCreate() {
        return "/* LOCALITY='" + this.toString() + "' */";
    }

    public static class StoragePoolSpecParser {
        String spec;

        String[] storagePoolNames;

        String primaryStoragePoolName;

        StoragePoolSpecParser(String spec) {
            this.spec = spec;
            parse();
        }

        void parse() {

//            String specPattern = STORAGE_POOL_PREFIX + "'([^']+)'" + "(," + PRIMARY_STORAGE_POOL_PREFIX + "'([^']+)'"  + ")?";
            String totalPattern =
                STORAGE_POOL_PREFIX + "(.+?['\"])" + "(," + PRIMARY_STORAGE_POOL_PREFIX + "(.*['\"])" + ")?";
            Pattern pattern = Pattern.compile(totalPattern);

            Matcher matcher = pattern.matcher(spec);
            if (matcher.matches()) {
                String storagePoolList = matcher.group(1);
                String primaryStoragePool = matcher.group(3);
                storagePoolNames = StringUtils.strip(storagePoolList, "'\"").split(",");
                if (!StringUtils.isEmpty(primaryStoragePool)) {
                    primaryStoragePoolName = StringUtils.strip(primaryStoragePool, "'\"");
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_INVALID,
                    "invalid locality specification for storage pool : " + spec);
            }
        }

        public String[] getStoragePoolNames() {
            return storagePoolNames;
        }

        public String getPrimaryStoragePool() {
            return primaryStoragePoolName;
        }

    }

    /**
     * JSON serialization: {locality: 'dn=dn1'}
     */
    @Value
    static class LocalityJSON {
        public String locality;

        @JSONCreator
        public LocalityJSON(String locality) {
            this.locality = locality;
        }
    }
}
