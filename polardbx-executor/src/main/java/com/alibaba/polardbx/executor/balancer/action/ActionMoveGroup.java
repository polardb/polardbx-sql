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

package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * Action that move group between storage nodes
 *
 * @author moyi
 * @since 2021/05
 */
public class ActionMoveGroup implements BalanceAction, Comparable<ActionMoveGroup> {

    private static final Logger LOG = SQLRecorderLogger.ddlLogger;

    private String schema;
    private List<String> sourceGroups;
    private String target;

    /**
     * Run move database in debug mode.
     */
    private boolean debug = false;

    @JSONCreator
    public ActionMoveGroup(String schema, List<String> sourceGroups, String target, boolean debug) {
        this.schema = schema;
        this.sourceGroups = sourceGroups;
        this.target = target;
        this.debug = debug;
    }

    public ActionMoveGroup(String schema, List<String> sourceGroups, String target) {
        this(schema, sourceGroups, target, false);
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    @Override
    public String getName() {
        return "MoveGroup";
    }

    @Override
    public String getStep() {
        return genMoveGroupSql(StringUtils.join(sourceGroups, ","));
    }

    public List<String> getSourceGroups() {
        return this.sourceGroups;
    }

    private String genMoveGroupSql(String sourceGroup) {
        String hint = "";
        if (this.debug) {
            List<String> params = Lists.newArrayList(
                ConnectionParams.SCALE_OUT_DEBUG.getName() + "=true",
                ConnectionParams.SHARE_STORAGE_MODE.getName() + "=true"
            );
            hint = String.format("/*+TDDL:CMD_EXTRA(%s)*/", StringUtils.join(params, ","));
        }
        return String.format("move database %s %s to '%s'", hint, sourceGroup, this.target);
    }

    @Override
    public ExecutableDdlJob toDdlJob(ExecutionContext ec) {
        String groups = StringUtils.join(getSourceGroups(), ",");
        String sql = genMoveGroupSql(groups);
        if (this.debug) {
            ParamManager.setBooleanVal(ec.getParamManager().getProps(),
                ConnectionParams.SHARE_STORAGE_MODE, true, false);
            ParamManager.setVal(ec.getParamManager().getProps(),
                ConnectionParams.SCALE_OUT_DEBUG, "true", false);
        }
        return ActionUtils.convertToDelegatorJob(ec, schema, sql);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ActionMoveGroup)) {
            return false;
        }
        ActionMoveGroup moveGroup = (ActionMoveGroup) o;
        return Objects.equals(sourceGroups, moveGroup.sourceGroups) && Objects
            .equals(target, moveGroup.target);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceGroups, target);
    }

    @Override
    public String toString() {
        return "ActionMoveGroup{" +
            "sourceGroups=" + sourceGroups +
            ", target='" + target + '\'' +
            ", debug=" + debug +
            '}';
    }

    @Override
    public int compareTo(ActionMoveGroup o) {
        int res = schema.compareTo(o.schema);
        if (res != 0) {
            return res;
        }
        res = target.compareTo(o.target);
        if (res != 0) {
            return res;
        }
        for (int i = 0; i < Math.min(sourceGroups.size(), o.sourceGroups.size()); i++) {
            res = sourceGroups.get(i).compareTo(o.sourceGroups.get(i));
            if (res != 0) {
                return res;
            }
        }
        return Integer.compare(sourceGroups.size(), o.sourceGroups.size());
    }
}
