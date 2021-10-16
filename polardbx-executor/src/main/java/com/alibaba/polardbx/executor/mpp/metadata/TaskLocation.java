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

package com.alibaba.polardbx.executor.mpp.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.gms.node.NodeServer;
import com.alibaba.polardbx.util.MoreObjects;

import java.net.URI;

import static com.alibaba.polardbx.executor.mpp.execution.TaskId.getEmptyTask;
import static com.alibaba.polardbx.gms.node.NodeServer.EMPTY_NODE_SERVER;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;

public class TaskLocation {
    private final static TaskLocation EMPTY_TASK_LOCATION = new TaskLocation(EMPTY_NODE_SERVER, getEmptyTask(), -1);

    public static TaskLocation getEmptyTaskLocation() {
        return EMPTY_TASK_LOCATION;
    }

    private NodeServer nodeServer;
    private TaskId taskId;
    private URI uri;
    private int bufferId = -1;

    @JsonCreator
    public TaskLocation(@JsonProperty("nodeServer") NodeServer nodeServer,
                        @JsonProperty("taskId") TaskId taskId,
                        @JsonProperty("bufferId") int bufferId) {
        this.nodeServer = nodeServer;
        this.taskId = taskId;
        this.bufferId = bufferId;
    }

    public TaskLocation(NodeServer nodeServer,
                        TaskId taskId) {
        this.nodeServer = nodeServer;
        this.taskId = taskId;
    }

    @VisibleForTesting
    public void setUriForTest(URI uriForTest) {
        this.uri = uriForTest;
    }

    @JsonProperty
    public NodeServer getNodeServer() {
        return nodeServer;
    }

    @JsonProperty
    public TaskId getTaskId() {
        return taskId;
    }

    public void setTaskId(TaskId taskId) {
        this.taskId = taskId;
    }

    @JsonProperty
    public int getBufferId() {
        return bufferId;
    }

    public void setBufferId(int bufferId) {
        this.bufferId = bufferId;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("nodeServer", nodeServer)
            .add("taskId", taskId)
            .add("bufferId", bufferId)
            .toString();
    }

    @Override
    public int hashCode() {
        int result = getNodeServer() != null ? getNodeServer().hashCode() : 0;
        result = 31 * result + (getTaskId() != null ? getTaskId().hashCode() : 0);
        result = 31 * result + getBufferId();
        return result;
    }

    @Override
    public boolean equals(Object ob) {
        if (ob == this) {
            return true;
        }
        if (!(ob instanceof TaskLocation) || ob == null) {
            return false;
        }
        TaskLocation that = (TaskLocation) ob;
        return taskId.equals(that.getTaskId())
            && nodeServer.equals(that.getNodeServer())
            && bufferId == that.getBufferId();
    }

    public boolean isLocal() {
        return ServiceProvider.getInstance().getServer().getLocalNode().getNodeServer().equals(this.nodeServer);
    }

    public URI getUri() {
        if (uri != null) {
            return uri;
        }
        return makeUri();
    }

    public synchronized URI makeUri() {
        if (uri == null) {
            if (bufferId > -1) {
                uri = uriBuilderFrom(nodeServer.getTaskURI())
                    .appendPath(taskId.toString())
                    .appendPath("results")
                    .appendPath(String.valueOf(bufferId))
                    .build();
            } else {
                uri = uriBuilderFrom(nodeServer.getTaskURI())
                    .appendPath(taskId.toString())
                    .build();
            }
        }
        return uri;
    }
}
