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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

public class TaskId {
    private final static TaskId EMPTY_TASKID = new TaskId("-1", -1, -1);

    public static TaskId getEmptyTask() {
        return EMPTY_TASKID;
    }

    private final String queryId;
    private final StageId stageId;

    @JsonCreator
    public static TaskId valueOf(String taskId) {
        return new TaskId(taskId);
    }

    private final String fullId;

    /*
    	@JsonCreator
	public TaskId(@JsonProperty("fullId") String fullId,
				  @JsonProperty("schema") String schema) {
		this(fullId);
		this.queryId.setSchema(schema);
	}
     */

    public TaskId(String queryId, int stageId, int id) {
        this(new StageId(queryId, stageId), id);
    }

    public TaskId(StageId stageId, int id) {
        this.fullId = stageId.getQueryId() + "." + stageId.getId() + "." + id;
        this.stageId = stageId;
        this.queryId = stageId.getQueryId();
    }

    public TaskId(String fullId) {
        this.fullId = fullId;
        int firstPos = fullId.indexOf(".");
        int lastPos = fullId.lastIndexOf(".");
        String qId = fullId.substring(0, firstPos);
        String sId = fullId.substring(firstPos + 1, lastPos);
        this.queryId = qId;
        this.stageId = new StageId(queryId, Integer.parseInt(sId));
    }

    public String getQueryId() {
        return queryId;
    }

    public StageId getStageId() {
        return stageId;
    }

    public int getId() {
        int lastPos = fullId.lastIndexOf(".");
        return Integer.parseInt(fullId.substring(lastPos + 1, fullId.length()));
    }

    @Override
    @JsonValue
    public String toString() {
        return fullId;
    }

    @Override
    public int hashCode() {
        return 31 + fullId.hashCode();
        //return Objects.hash(fullId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskId other = (TaskId) obj;
        return Objects.equals(this.fullId, other.fullId);
    }
}
