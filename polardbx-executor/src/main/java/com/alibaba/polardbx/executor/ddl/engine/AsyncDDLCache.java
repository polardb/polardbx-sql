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

package com.alibaba.polardbx.executor.ddl.engine;

import com.alibaba.polardbx.common.ddl.Attribute;
import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.ddl.Job.JobType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddIndex;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnique;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.ddl.sync.JobResponse.Response;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.AsyncDDLContext;
import org.apache.calcite.sql.SqlAlterTableDropIndex;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.executor.ddl.engine.AsyncDDLJobBase.SEPARATOR_COMMON;

public class AsyncDDLCache {

    // { schemaName, TGroupDataSource }
    private static final Map<String, DataSource> dataSources = new ConcurrentHashMap<>();

    // { schemaName, available }
    private static final Set<String> activeSchemas = ConcurrentHashMap.newKeySet();

    // { schemaName, { jobId, <responseType, responseContent> } }
    private static final Map<String, Map<Long, Response>> responses = new ConcurrentHashMap<>();

    // { schemaName, { parentJobId, { subJobId, sub-job } } }
    private static final Map<String, Map<Long, Map<Long, Job>>> leftSubJobs = new ConcurrentHashMap<>();

    // { schemaName, { jobId, [ groupKey:physicalTableName ] } }
    private static final Map<String, Map<Long, Set<String>>> objectsDone = new ConcurrentHashMap<>();

    // { schemaName, { jobId, Job } }
    private static final Map<String, Map<Long, Job>> ongoingJobs = new ConcurrentHashMap<>();

    // { schemaName, { <objectSchema, objectName>, Job } }
    private static final Map<String, Map<Pair<String, String>, Job>> fencedObjects = new ConcurrentHashMap<>();

    static void initAll(String schemaName, DataSource dataSource) {
        schemaName = schemaName.toLowerCase();
        addDataSource(schemaName, dataSource);
        activeSchemas.add(schemaName);
        responses.put(schemaName, new ConcurrentHashMap<>());
        leftSubJobs.put(schemaName, new ConcurrentHashMap<>());
        objectsDone.put(schemaName, new ConcurrentHashMap<>());
        ongoingJobs.put(schemaName, new ConcurrentHashMap<>());
        fencedObjects.put(schemaName, new ConcurrentHashMap<>());
    }

    static void destroyAll(String schemaName) {
        schemaName = schemaName.toLowerCase();
        removeDataSource(schemaName);
        activeSchemas.remove(schemaName);
        responses.remove(schemaName);
        leftSubJobs.remove(schemaName);
        objectsDone.remove(schemaName);
        ongoingJobs.remove(schemaName);
        fencedObjects.remove(schemaName);
    }

    public static Map<String, DataSource> getDataSources() {
        return dataSources;
    }

    public static DataSource getDataSource(String schemaName) {
        return dataSources.get(schemaName.toLowerCase());
    }

    static void addDataSource(String schemaName, DataSource dataSource) {
        dataSources.put(schemaName.toLowerCase(), dataSource);
    }

    static String removeDataSource(String schemaName) {
        String unitName = null;
        DataSource dataSource = dataSources.remove(schemaName.toLowerCase());
        return unitName;
    }

    public static boolean isSchemaAvailable(String schemaName) {
        return activeSchemas.contains(schemaName.toLowerCase());
    }

    public static void addResponse(String schemaName, Long jobId, Response response) {
        responses.get(schemaName.toLowerCase()).put(jobId, response);
    }

    static Response getResponse(String schemaName, Long jobId) {
        return responses.get(schemaName.toLowerCase()).get(jobId);
    }

    public static void removeResponse(String schemaName, Long jobId) {
        responses.get(schemaName.toLowerCase()).remove(jobId);
    }

    public static void removeResponses(String schemaName, List<Long> jobIds) {
        if (jobIds != null && !jobIds.isEmpty()) {
            for (Long jobId : jobIds) {
                removeResponse(schemaName, jobId);
            }
        }
    }

    public static void removeResponses(String schemaName) {
        responses.get(schemaName.toLowerCase()).clear();
    }

    public static Collection<Job> getLeftSubJobs(String schemaName) {
        Set<Job> allLeftSubJobs = new HashSet<>();
        Collection<Map<Long, Job>> leftSubJobSets = leftSubJobs.get(schemaName.toLowerCase()).values();
        for (Map<Long, Job> leftSubJobSet : leftSubJobSets) {
            allLeftSubJobs.addAll(leftSubJobSet.values());
        }
        return allLeftSubJobs;
    }

    static Map<Long, Job> getLeftSubJobs(String schemaName, Long parentJobId) {
        return leftSubJobs.get(schemaName.toLowerCase()).get(parentJobId);
    }

    static void addLeftSubJob(String schemaName, Job subJob) {
        Map<Long, Job> subJobs = leftSubJobs.get(schemaName.toLowerCase()).get(subJob.getParentId());
        if (subJobs == null) {
            subJobs = new ConcurrentHashMap<>();
            leftSubJobs.get(schemaName.toLowerCase()).put(subJob.getParentId(), subJobs);
        }
        subJobs.put(subJob.getId(), subJob);
        // Cache objects done as well.
        AsyncDDLCache.addObjectsDone(schemaName, subJob);
    }

    public static void removeLeftSubJobs(String schemaName, List<Long> subJobIds) {
        if (subJobIds != null && !subJobIds.isEmpty()) {
            for (Long subJobId : subJobIds) {
                removeLeftSubJob(schemaName, subJobId);
            }
        }
    }

    public static void removeLeftSubJob(String schemaName, Long subJobId) {
        removeLeftSubJobs(schemaName, subJobId);
        removeObjectsDone(schemaName, subJobId);
    }

    public static void removeLeftSubJobs(String schemaName) {
        leftSubJobs.get(schemaName.toLowerCase()).clear();
        objectsDone.get(schemaName.toLowerCase()).clear();
    }

    private static void removeLeftSubJobs(String schemaName, Long subJobId) {
        Collection<Map<Long, Job>> leftSubJobSets = leftSubJobs.get(schemaName.toLowerCase()).values();
        for (Map<Long, Job> leftSubJobSet : leftSubJobSets) {
            Long key = null;
            for (Entry<Long, Job> leftSubJob : leftSubJobSet.entrySet()) {
                if (subJobId == leftSubJob.getValue().getId()) {
                    key = leftSubJob.getKey();
                    break;
                }
            }
            if (key != null) {
                leftSubJobSet.remove(key);
            }
        }
    }

    static void reloadObjectsDone(String schemaName, Job job) {
        removeObjectsDone(schemaName.toLowerCase(), job.getId());
        addObjectsDone(schemaName.toLowerCase(), job);
    }

    static Set<String> getObjectsDone(String schemaName, Long jobId) {
        return objectsDone.get(schemaName.toLowerCase()).get(jobId);
    }

    static void addObjectsDone(String schemaName, Job job) {
        addObjectsDone(schemaName, job.getId(), job.getPhysicalObjectDone());
    }

    public static void removeObjectsDone(String schemaName, List<Long> jobIds) {
        if (jobIds != null && !jobIds.isEmpty()) {
            for (Long jobId : jobIds) {
                removeObjectsDone(schemaName, jobId);
            }
        }
    }

    static void addObjectsDone(String schemaName, long jobId, String objectDone) {
        if (TStringUtil.isNotEmpty(objectDone)) {
            String[] objects = objectDone.split(SEPARATOR_COMMON);
            Set<String> objectSet = new HashSet<>(objects.length);
            for (String object : objects) {
                objectSet.add(object);
            }
            objectsDone.get(schemaName.toLowerCase()).put(jobId, objectSet);
        }
    }

    private static void removeObjectsDone(String schemaName, Long jobId) {
        objectsDone.get(schemaName.toLowerCase()).remove(jobId);
    }

    public static boolean isJobCancelled(String schemaName, Long jobId) {
        return !isJobOngoing(schemaName, jobId);
    }

    static boolean isJobOngoing(String schemaName, Long jobId) {
        return ongoingJobs.get(schemaName.toLowerCase()).containsKey(jobId);
    }

    public static boolean isObjectOngoing(String schemaName, String objectSchema, String objectName) {
        Job job = getOngoingObject(schemaName, objectSchema, objectName);
        return job != null;
    }

    public static Job getOngoingObject(String schemaName, String objectSchema, String objectName) {
        if (ongoingJobs.get(schemaName.toLowerCase()) == null) {
            return null;
        }
        for (Job job : ongoingJobs.get(schemaName.toLowerCase()).values()) {
            if (TStringUtil.equalsIgnoreCase(objectSchema, job.getObjectSchema())
                && TStringUtil.equalsIgnoreCase(objectName, job.getObjectName())) {
                return job;
            }
        }
        return null;
    }

    public static Collection<Job> getOngoingJobs(String schemaName) {
        return ongoingJobs.get(schemaName.toLowerCase()).values();
    }

    public static void addOngoingJob(String schemaName, Job job) {
        ongoingJobs.get(schemaName.toLowerCase()).put(job.getId(), job);
    }

    public static void removeOngoingJob(String schemaName, Long jobId) {
        ongoingJobs.get(schemaName.toLowerCase()).remove(jobId);
    }

    public static void removeOngoingJobs(String schemaName, List<Long> jobIds) {
        if (jobIds != null && !jobIds.isEmpty()) {
            for (Long jobId : jobIds) {
                removeOngoingJob(schemaName, jobId);
            }
        }
    }

    public static void removeOngoingJobs(String schemaName) {
        ongoingJobs.get(schemaName.toLowerCase()).clear();
    }

    public static Collection<Job> getFencedObjects(String schemaName) {
        Map<Pair<String, String>, Job> fencedSchemaObjects = fencedObjects.get(schemaName.toLowerCase());
        return fencedSchemaObjects != null ? fencedSchemaObjects.values() : null;
    }

    public static void addFencedObject(String schemaName, Job job) {
        Pair<String, String> objectInfo = new Pair<>(job.getObjectSchema(), job.getObjectName());
        Map<Pair<String, String>, Job> fencedSchemaObjects = fencedObjects.get(schemaName.toLowerCase());
        if (fencedSchemaObjects != null) {
            fencedSchemaObjects.put(objectInfo, job);
        }
    }

    static JobType getFencedJobType(String schemaName, String objectSchema, String objectName) {
        Job fencedJob = getFencedJob(schemaName, objectSchema, objectName);
        if (fencedJob != null) {
            switch (fencedJob.getType()) {
            case CREATE_GLOBAL_INDEX:
            case DROP_GLOBAL_INDEX:
                if (AsyncDDLContext.isParentJob(fencedJob)) {
                    // Special dealing with compound job.
                    // Compound job is primary table, which should not fenced.
                    fencedJob = null;
                }
                if (fencedJob != null) {
                    // Or add drop local index on primary or clustered.
                    final List<SQLStatement> stmts =
                        SQLUtils.parseStatements(fencedJob.getDdlStmt(), JdbcConstants.MYSQL);
                    if (1 == stmts.size()) {
                        final String indexName;
                        if (stmts.get(0) instanceof SQLCreateIndexStatement) {
                            indexName = SQLUtils.normalizeNoTrim(
                                ((SQLCreateIndexStatement) stmts.get(0)).getIndexDefinition().getName()
                                    .getSimpleName());
                        } else if (stmts.get(0) instanceof SQLDropIndexStatement) {
                            indexName = SQLUtils
                                .normalizeNoTrim(((SQLDropIndexStatement) stmts.get(0)).getIndexName().getSimpleName());
                        } else if (stmts.get(0) instanceof SQLAlterTableStatement) {
                            final SQLAlterTableStatement statement = (SQLAlterTableStatement) stmts.get(0);
                            if (1 == statement.getItems().size()) {
                                if (statement.getItems().get(0) instanceof SQLAlterTableAddIndex) {
                                    indexName = SQLUtils.normalizeNoTrim(
                                        ((SQLAlterTableAddIndex) statement.getItems().get(0)).getIndexDefinition()
                                            .getName().getSimpleName());
                                } else if (statement.getItems().get(0) instanceof SQLAlterTableAddConstraint &&
                                    ((SQLAlterTableAddConstraint) statement.getItems().get(0))
                                        .getConstraint() instanceof SQLUnique) {
                                    indexName = SQLUtils.normalizeNoTrim(
                                        ((SQLUnique) ((SQLAlterTableAddConstraint) statement.getItems().get(0))
                                            .getConstraint()).getIndexDefinition().getName().getSimpleName());
                                } else if (statement.getItems().get(0) instanceof SqlAlterTableDropIndex) {
                                    indexName = SQLUtils.normalizeNoTrim(
                                        ((SqlAlterTableDropIndex) statement.getItems().get(0)).getIndexName()
                                            .getLastName());
                                } else {
                                    indexName = null;
                                }
                            } else {
                                indexName = null;
                            }
                        } else {
                            indexName = null;
                        }
                        if (indexName != null && !indexName.equalsIgnoreCase(fencedJob.getObjectName())) {
                            // Get index name and this is not for creating or dropping GSI table.
                            fencedJob = null;
                        }
                    }
                }
                break;
            default:
                break;
            }
        }
        return fencedJob != null ? fencedJob.getType() : JobType.UNSUPPORTED;
    }

    public static Job getFencedJob(String schemaName, String objectSchema, String objectName) {
        Pair<String, String> objectInfo = new Pair<>(objectSchema, objectName);
        Map<Pair<String, String>, Job> fencedSchemaObjects = fencedObjects.get(schemaName.toLowerCase());
        return fencedSchemaObjects != null ? fencedObjects.get(schemaName.toLowerCase()).get(objectInfo) : null;
    }

    public static boolean isObjectFenced(String schemaName, String objectSchema, String objectName) {
        return getFencedJobType(schemaName, objectSchema, objectName) != JobType.UNSUPPORTED;
    }

    public static void removeFencedObject(String schemaName, Job job) {
        Pair<String, String> objectInfo = new Pair<>(job.getObjectSchema(), job.getObjectName());
        Map<Pair<String, String>, Job> fencedSchemaObjects = fencedObjects.get(schemaName.toLowerCase());
        if (fencedSchemaObjects != null) {
            fencedSchemaObjects.remove(objectInfo);
        }
    }

    public static void removeFencedObjects(String schemaName, List<Long> jobIds) {
        if (jobIds != null && !jobIds.isEmpty()) {
            List<Pair<String, String>> objectInfos = new ArrayList<>();
            Map<Pair<String, String>, Job> fencedJobs = fencedObjects.get(schemaName.toLowerCase());
            for (Long jobId : jobIds) {
                for (Entry<Pair<String, String>, Job> fencedJob : fencedJobs.entrySet()) {
                    if (fencedJob.getValue().getId() == jobId.longValue()) {
                        objectInfos.add(fencedJob.getKey());
                        break;
                    }
                }
            }
            for (Pair<String, String> objectInfo : objectInfos) {
                fencedJobs.remove(objectInfo);
            }
        }
    }

    public static void removeFencedObjects(String schemaName) {
        // Remove fenced objects for the jobs that are not ongoing, e.g. left
        // caused by pending.
        List<Pair<String, String>> objectInfos = new ArrayList<>();
        Map<Pair<String, String>, Job> fencedJobs = fencedObjects.get(schemaName.toLowerCase());
        for (Entry<Pair<String, String>, Job> fencedJob : fencedJobs.entrySet()) {
            if (!ongoingJobs.get(schemaName.toLowerCase()).keySet().contains(fencedJob.getValue().getId())) {
                objectInfos.add(fencedJob.getKey());
            }
        }
        for (Pair<String, String> objectInfo : objectInfos) {
            fencedJobs.remove(objectInfo);
        }
    }

    public static int getLogicalParallelism(AsyncDDLContext asyncDDLContext) {
        int parallelism = asyncDDLContext.getParamManager().getInt(ConnectionParams.LOGICAL_DDL_PARALLELISM);
        if (parallelism < Attribute.MIN_LOGICAL_DDL_PARALLELISM
            || parallelism > Attribute.MAX_LOGICAL_DDL_PARALLELISM) {
            parallelism = Attribute.DEFAULT_LOGICAL_DDL_PARALLELISM;
        }
        return parallelism;
    }

}
