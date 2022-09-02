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

package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
@Ignore("Test modify the default values,and effect other test cases")
public class SetVariablesTest extends ReadBaseTestCase {

    @Test
    public void setVariablesSingsleTest() throws Exception {
        enableSetGlobal();
        String sql = "SET GLOBAL auto_increment_offset = 2648";
//        int count1 = getCount();
        try {

            execute(tddlConnection, "SET GLOBAL auto_increment_offset = 2648");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("is a GLOBAL variable"));
        }
        JdbcUtil.executeSuccess(tddlConnection, "SET session auto_increment_offset = 2649");
        int count2 = getCount();
        Assert.assertTrue(count2 == 2649);
//        Assert.assertNotEquals(count1, count2);

    }

    private void setGlobalVariableSingleTest(String variableName, String variableValue) throws Exception {
        setVariableValue(variableName, variableValue, true);
        TimeUnit.MINUTES.sleep(1);
        Assert.assertEquals(variableValue, getDnVariableValue(variableName, true));
    }

    private void batchSetGlobalVariableTest(List<AssignmentItem> variableAssignmentList, boolean shouldCheckOnDn)
        throws Exception {
        if (shouldCheckOnDn) {
            variableAssignmentList =
                variableAssignmentList.stream().filter(this::testIfDnVariableCanBeSet).collect(
                    Collectors.toList());
        }
        batchSetVariableValue(variableAssignmentList);
        TimeUnit.MINUTES.sleep(2);
        for (AssignmentItem item : variableAssignmentList) {
//            System.out.println("Testing variable: " + item.variableName);
            Assert.assertEquals(item.variableValue.replaceAll("\"", "").toLowerCase(Locale.ROOT),
                (shouldCheckOnDn ? getDnVariableValue(item.variableName, item.isGlobal) :
                    getVariableValue(item.variableName, item.isGlobal)).toLowerCase(Locale.ROOT));
        }
    }

    private void setSessionVariableSingleTest(String variableName, String variableValue) throws Exception {
        setVariableValue(variableName, variableValue, false);
        Assert.assertEquals(variableValue, getVariableValue(variableName, false));
    }

    @Test
    public void setCnGlobalVariableTest() throws Exception {

        enableSetGlobal();
        Random random = new Random();
        List<AssignmentItem> variableAssignmentList = new LinkedList<>();
        // Warning: do no set this
//        variableAssignmentList.add(new AssignmentItem("advise_type", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("allow_add_gsi", "true", true));
        variableAssignmentList.add(new AssignmentItem("allow_alter_gsi_indirectly", "false", true));
        variableAssignmentList.add(new AssignmentItem("allow_drop_database_in_scaleout_phase", "false", true));
        variableAssignmentList.add(new AssignmentItem("allow_drop_or_modify_part_unique_with_gsi", "false", true));
        variableAssignmentList.add(new AssignmentItem("allow_full_table_scan", "false", true));
        variableAssignmentList.add(new AssignmentItem("allow_loose_alter_column_with_gsi", "false", true));
        variableAssignmentList.add(new AssignmentItem("allow_simple_sequence", "false", true));
        variableAssignmentList.add(new AssignmentItem("always_rebuild_plan", "false", true));
        variableAssignmentList.add(new AssignmentItem("analyze_table_speed_limitation", "500000", true));
        variableAssignmentList.add(new AssignmentItem("auto_analyze_all_column_table_limit", "10000", true));
        variableAssignmentList.add(new AssignmentItem("auto_analyze_period_in_hours", "168", true));
        variableAssignmentList.add(new AssignmentItem("auto_analyze_table_sleep_mills", "1", true));
        variableAssignmentList.add(new AssignmentItem("auto_partition", "false", true));
        variableAssignmentList.add(new AssignmentItem("auto_partition_partitions", "8", true));
        variableAssignmentList.add(new AssignmentItem("automatic_ddl_job_recovery", "false", true));
        variableAssignmentList.add(new AssignmentItem("background_statistic_collection_end_time", "\"05:00\"", true));
        variableAssignmentList.add(new AssignmentItem("background_statistic_collection_expire_time", "720", true));
        variableAssignmentList.add(new AssignmentItem("background_statistic_collection_period", "720", true));
        variableAssignmentList.add(new AssignmentItem("background_statistic_collection_start_time", "\"02:00\"", true));
        variableAssignmentList.add(new AssignmentItem("balancer_max_partition_size", "536870912", true));
//        variableAssignmentList.add(new AssignmentItem("balancer_window", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("batch_insert_chunk_size", "200", true));
        variableAssignmentList.add(new AssignmentItem("batch_insert_policy", "\"SPLIT\"", true));
        variableAssignmentList.add(new AssignmentItem("binlog_rows_query_log_events", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("block_concurrent", "false", true));
        variableAssignmentList.add(new AssignmentItem("bloom_filter_broadcast_num", "20", true));
        variableAssignmentList.add(new AssignmentItem("bloom_filter_guess_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("bloom_filter_max_size", "2097152", true));
        variableAssignmentList.add(new AssignmentItem("bloom_filter_min_size", "1000", true));
        variableAssignmentList.add(new AssignmentItem("bloom_filter_ratio", "0.5", true));
        variableAssignmentList.add(new AssignmentItem("broadcast_dml", "false", true));
        variableAssignmentList.add(new AssignmentItem("calculate_actual_shard_count_for_cost", "true", true));
        variableAssignmentList.add(new AssignmentItem("cbo_agg_join_transpose_limit", "1", true));
        variableAssignmentList.add(new AssignmentItem("cbo_bushy_tree_join_limit", "3", true));
        variableAssignmentList.add(new AssignmentItem("cbo_join_tablelookup_transpose_limit", "1", true));
        variableAssignmentList.add(new AssignmentItem("cbo_left_deep_tree_join_limit", "7", true));
        variableAssignmentList.add(new AssignmentItem("cbo_start_up_cost_join_limit", "5", true));
        variableAssignmentList.add(new AssignmentItem("cbo_too_many_join_limit", "14", true));
        variableAssignmentList.add(new AssignmentItem("cbo_zig_zag_tree_join_limit", "5", true));
        variableAssignmentList.add(new AssignmentItem("cdc_startup_mode", "1", true));
        variableAssignmentList.add(new AssignmentItem("choose_broadcast_write", "true", true));
        variableAssignmentList.add(new AssignmentItem("choose_streaming", "false", true));
        variableAssignmentList.add(new AssignmentItem("chunk_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("cold_hot_limit_count", "0", true));
        variableAssignmentList.add(new AssignmentItem("collect_sql_error_info", "false", true));
        variableAssignmentList.add(new AssignmentItem("complex_dml_with_trx", "false", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_block_timeout", "5000", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_idle_timeout", "60", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_max_pool_size", "60", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_max_wait_thread_count", "0", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_min_pool_size", "5", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_properties",
            "\"connectTimeout=5000;characterEncoding=utf8;autoReconnect=true;failOverReadOnly=false;socketTimeout=12000;rewriteBatchedStatements=true;useServerPrepStmts=false;useSSL=false\"",
            true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_auth", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_auto_commit_optimize", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_checker", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_chunk_result", "true", true));
//        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_config", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_direct_write", "false", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_feedback", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_flag", "0", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_max_client_per_inst", "32", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_max_packet_size", "67108864", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_max_pooled_session_per_inst", "512", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_max_session_per_client", "1024", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_message_timestamp", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_meta_db_port", "0", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_min_pooled_session_per_inst", "32", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_pipe_buffer_size", "268435456", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_plan_cache", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_pure_async_mpp", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_query_token", "10000", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_session_aging_time", "600000", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_slow_thresh", "1000", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_storage_db_port", "0", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_trx_leak_check", "false", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_xplan", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_xplan_expend_star", "true", true));
        variableAssignmentList.add(new AssignmentItem("conn_pool_xproto_xplan_table_scan", "false", true));
        variableAssignmentList.add(new AssignmentItem("database_parallelism", "0", true));
        variableAssignmentList.add(new AssignmentItem("db_priv", "-1", true));
        variableAssignmentList.add(new AssignmentItem("ddl_engine_debug", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("ddl_job_idle_waiting_time", "10000", true));
        variableAssignmentList.add(new AssignmentItem("ddl_job_request_timeout", "900000", true));
        variableAssignmentList.add(new AssignmentItem("ddl_on_gsi", "false", true));
        variableAssignmentList.add(new AssignmentItem("deadlock_detection_interval", "1000", true));
        variableAssignmentList.add(new AssignmentItem("distributed_trx_required", "false", true));
        variableAssignmentList.add(new AssignmentItem("dml_execution_strategy", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("dml_on_gsi", "false", true));
        variableAssignmentList.add(new AssignmentItem("dml_push_duplicate_check", "true", true));
        variableAssignmentList.add(new AssignmentItem("dml_return_ignored_count", "false", true));
        variableAssignmentList.add(new AssignmentItem("dml_skip_crucial_err_check", "false", true));
        variableAssignmentList.add(new AssignmentItem("dml_skip_duplicate_check_for_pk", "true", true));
        variableAssignmentList.add(new AssignmentItem("dml_skip_trivial_update", "true", true));
        variableAssignmentList.add(new AssignmentItem("dml_use_returning", "true", true));
        variableAssignmentList.add(new AssignmentItem("drds_transaction_policy", "XA", true));
        variableAssignmentList.add(new AssignmentItem("enable_agg_pruning", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_alter_shard_key", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_async_ddl", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_aware_learner_delay", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_aware_learner_load", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_background_statistic_collection", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_balancer", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_bka_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_bka_pruning", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_branch_and_bound_optimization", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_broadcast_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_broadcast_random_read", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_cbo_group_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_cbo_push_agg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_cbo_push_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_compatible_datetime_rounddown", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_compatible_timestamp_rounddown", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_complex_dml_cross_db", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_consistent_replica_read", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_cross_view_optimize", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_ddl", "TRUE", true));
        variableAssignmentList.add(new AssignmentItem("enable_deadlock_detection", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_derive_trait", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_direct_plan", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_driving_stream_scan", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_expand_distinctagg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_expression_vectorization", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_hash_agg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_hash_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_hll", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_htap", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_in_sub_query_for_dml", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_index_selection", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_index_skyline", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_innodb_btree_sampling", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_join_clustering", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_join_clustering_avoid_cross_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_logical_info_schema_query", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_login_audit_config", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_master_mpp", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_materialized_semi_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_mdl", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_merge_index", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_modify_sharding_column", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_mpp", "FALSE", true));
        variableAssignmentList.add(new AssignmentItem("enable_mysql_hash_join", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_mysql_semi_hash_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_nl_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_outer_join_reorder", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_parameter_plan", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_parameterized_sql_log", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_partial_agg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_partition_management", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_partition_pruning", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_pass_through_trait", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_post_planner", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_push_agg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_push_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_push_project", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_push_runtime_filter_scan", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_push_sort", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_random_phy_table_name", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_recyclebin", "FALSE", true));
        variableAssignmentList.add(new AssignmentItem("enable_runtime_filter", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_runtime_filter_into_build_side", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_scale_out_all_phy_dml_log", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_scale_out_feature", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_scale_out_group_phy_dml_log", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_select_into_outfile", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_semi_bka_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_semi_hash_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_semi_join_reorder", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_semi_nl_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_semi_sort_merge_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_shuffle_by_partial_key", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_simplify_trace_sql", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_sort_agg", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_sort_join_transpose", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_sort_merge_join", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_spill", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_spill_output", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_spm", "FALSE", true));
        variableAssignmentList.add(new AssignmentItem("enable_spm_background_task", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_start_up_cost", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_statistic_feedback", "true", true));
        variableAssignmentList.add(new AssignmentItem("enable_temp_table_join", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_trx_read_conn_reuse", "false", true));
        variableAssignmentList.add(new AssignmentItem("enable_trx_single_shard_optimization", "true", true));
        variableAssignmentList.add(new AssignmentItem("executor_mode", "NONE", true));
        variableAssignmentList.add(new AssignmentItem("explain_logicalview", "false", true));
        variableAssignmentList.add(new AssignmentItem("explain_output_format", "TEXT", true));
        variableAssignmentList.add(new AssignmentItem("explain_x_plan", "false", true));
        variableAssignmentList.add(new AssignmentItem("feedback_workload_ap_threshold", "2147483647", true));
        variableAssignmentList.add(new AssignmentItem("feedback_workload_tp_threshold", "-1", true));
        variableAssignmentList.add(new AssignmentItem("fetch_size", "0", true));
        variableAssignmentList.add(new AssignmentItem("first_then_concurrent_policy", "false", true));
        variableAssignmentList.add(new AssignmentItem("forbid_apply_cache", "false", true));
        variableAssignmentList.add(new AssignmentItem("forbid_execute_dml_all", "FALSE", true));
        variableAssignmentList.add(new AssignmentItem("forbid_outer_driver_hash_join", "false", true));
        variableAssignmentList.add(new AssignmentItem("force_apply_cache", "TRUE", true));
        variableAssignmentList.add(new AssignmentItem("force_ddl_on_legacy_engine", "false", true));
//        variableAssignmentList.add(new AssignmentItem("force_disable_runtime_filter_columns", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("force_enable_runtime_filter_columns", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("force_outer_driver_hash_join", "false", true));
        variableAssignmentList.add(new AssignmentItem("general_dynamic_speed_limitation", "-1", true));
        variableAssignmentList.add(new AssignmentItem("get_tso_timeout", "5000", true));
        variableAssignmentList.add(new AssignmentItem("group_concurrent_block", "true", true));
        variableAssignmentList.add(new AssignmentItem("gsi_backfill_batch_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("gsi_backfill_parallelism", "-1", true));
//        variableAssignmentList.add(new AssignmentItem("gsi_backfill_position_mark", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("gsi_backfill_speed_limitation", "150000", true));
        variableAssignmentList.add(new AssignmentItem("gsi_backfill_speed_min", "10000", true));
        variableAssignmentList.add(new AssignmentItem("gsi_check_after_creation", "true", true));
        variableAssignmentList.add(new AssignmentItem("gsi_check_batch_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("gsi_check_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("gsi_check_speed_limitation", "150000", true));
        variableAssignmentList.add(new AssignmentItem("gsi_check_speed_min", "10000", true));
        variableAssignmentList.add(new AssignmentItem("gsi_concurrent_write", "false", true));
        variableAssignmentList.add(new AssignmentItem("gsi_concurrent_write_optimize", "true", true));
//        variableAssignmentList.add(new AssignmentItem("gsi_debug", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("gsi_default_current_timestamp", "true", true));
        variableAssignmentList.add(new AssignmentItem("gsi_early_fail_number", "1024", true));
//        variableAssignmentList.add(new AssignmentItem("gsi_final_status_debug", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("gsi_ignore_restriction", "false", true));
        variableAssignmentList.add(new AssignmentItem("gsi_on_update_current_timestamp", "true", true));
        variableAssignmentList.add(new AssignmentItem("histogram_bucket_size", "64", true));
        variableAssignmentList.add(new AssignmentItem("histogram_max_sample_size", "10000", true));
        variableAssignmentList.add(new AssignmentItem("hybrid_hash_join_bucket_num", "4", true));
        variableAssignmentList.add(new AssignmentItem("in_sub_query_threshold", "2", true));
        variableAssignmentList.add(new AssignmentItem("info_schema_query_stat_by_group", "false", true));
        variableAssignmentList.add(new AssignmentItem("insert_select_batch_size", "1000", true));
        variableAssignmentList.add(new AssignmentItem("insert_select_limit", "1000000", true));
        variableAssignmentList.add(new AssignmentItem("join_block_size", "300", true));
        variableAssignmentList.add(new AssignmentItem("join_clustering_condition_propagation_limit", "7", true));
        variableAssignmentList.add(new AssignmentItem("kill_close_stream", "true", true));
        variableAssignmentList.add(new AssignmentItem("load_data_batch_insert_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("load_data_cache_buffer_size", "62914560", true));
        variableAssignmentList.add(new AssignmentItem("load_data_handle_empty_char", "DEFAULT_VALUE_MODE", true));
        variableAssignmentList.add(new AssignmentItem("load_data_ignore_is_simple_insert", "true", true));
        variableAssignmentList.add(new AssignmentItem("load_data_use_batch_mode", "true", true));
        variableAssignmentList.add(new AssignmentItem("logical_ddl_parallelism", "1", true));
        variableAssignmentList.add(new AssignmentItem("lookup_in_value_limit", "300", true));
        variableAssignmentList.add(new AssignmentItem("lookup_join_block_size_per_shard", "50", true));
        variableAssignmentList.add(new AssignmentItem("lookup_join_max_batch_size", "6400", true));
        variableAssignmentList.add(new AssignmentItem("lookup_join_parallelism_factor", "4", true));
        variableAssignmentList.add(new AssignmentItem("master_read_weight", "-1", true));
        variableAssignmentList.add(new AssignmentItem("materialized_items_limit", "20000", true));
//        variableAssignmentList.add(new AssignmentItem("max_allowed_packet", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("max_batch_insert_sql_length", "256", true));
        variableAssignmentList.add(new AssignmentItem("max_cache_params", "10000", true));
        variableAssignmentList.add(new AssignmentItem("max_execute_memory", "200", true));
        variableAssignmentList.add(new AssignmentItem("max_parameterized_sql_log_length", "5000", true));
        variableAssignmentList.add(new AssignmentItem("max_partition_column_count", "3", true));
        variableAssignmentList.add(new AssignmentItem("max_physical_partition_count", "8192", true));
        variableAssignmentList.add(new AssignmentItem("max_table_partitions_per_db", "128", true));
        variableAssignmentList.add(new AssignmentItem("max_trx_duration", "28800", true));
        variableAssignmentList.add(new AssignmentItem("max_update_num_in_gsi", "10000", true));
        variableAssignmentList.add(new AssignmentItem("merge_concurrent", "false", true));
        variableAssignmentList.add(new AssignmentItem("merge_ddl_concurrent", "false", true));
        variableAssignmentList.add(new AssignmentItem("merge_ddl_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("merge_sort_buffer_size", "2097152", true));
        variableAssignmentList.add(new AssignmentItem("merge_union", "true", true));
        variableAssignmentList.add(new AssignmentItem("merge_union_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("minor_tolerance_value", "500", true));
        variableAssignmentList.add(new AssignmentItem("mpp_elapsed_query_threshold_mills", "600000", true));
        variableAssignmentList.add(new AssignmentItem("mpp_join_broadcast_num", "100", true));
        variableAssignmentList.add(new AssignmentItem("mpp_max_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("mpp_metric_level", "3", true));
        variableAssignmentList.add(new AssignmentItem("mpp_min_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("mpp_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("mpp_parallelism_auto_enable", "false", true));
        variableAssignmentList.add(new AssignmentItem("mpp_print_elapsed_query_enabled", "false", true));
        variableAssignmentList.add(new AssignmentItem("mpp_query_max_run_time", "86400000", true));
        variableAssignmentList.add(new AssignmentItem("mpp_query_need_reserve", "false", true));
        variableAssignmentList.add(new AssignmentItem("mpp_query_phased_exec_schedule_enable", "false", true));
        variableAssignmentList.add(new AssignmentItem("mpp_query_rows_per_partition", "5000", true));
        variableAssignmentList.add(new AssignmentItem("mpp_rpc_local_enabled", "true", true));
        variableAssignmentList.add(new AssignmentItem("mpp_schedule_max_splits_per_node", "0", true));
        variableAssignmentList.add(new AssignmentItem("mpp_tablescan_connection_strategy", "0", true));
        variableAssignmentList.add(new AssignmentItem("mpp_task_local_buffer_enabled", "true", true));
        variableAssignmentList.add(new AssignmentItem("mpp_task_local_max_buffer_size", "8000000", true));
        variableAssignmentList.add(new AssignmentItem("mpp_task_output_max_buffer_size", "32000000", true));
        variableAssignmentList.add(new AssignmentItem("mysql_join_reorder_exhaustive_depth", "4", true));
        variableAssignmentList.add(new AssignmentItem("num_of_job_schedulers", "64", true));
        variableAssignmentList.add(new AssignmentItem("parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("parametric_similarity_algo", "COSINE", true));
        variableAssignmentList.add(new AssignmentItem("partial_agg_bucket_threshold", "64", true));
        variableAssignmentList.add(new AssignmentItem("partial_agg_selectivity_threshold", "0.2", true));
        variableAssignmentList.add(new AssignmentItem("per_query_memory_limit", "-1", true));
        variableAssignmentList.add(new AssignmentItem("physical_ddl_mdl_waiting_timeout", "15", true));
        variableAssignmentList.add(new AssignmentItem("plan_cache", "true", true));
        variableAssignmentList.add(new AssignmentItem("plan_externalize_test", "TRUE", true));
        variableAssignmentList.add(new AssignmentItem("polardbx_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("polardbx_slave_instance_first", "true", true));
        variableAssignmentList.add(new AssignmentItem("prefetch_shards", "-1", true));
        variableAssignmentList.add(new AssignmentItem("pure_async_ddl_mode", "false", true));
        variableAssignmentList.add(new AssignmentItem("purge_trans_before", "604801", true));
        variableAssignmentList.add(new AssignmentItem("purge_trans_interval", "86401", true));
        variableAssignmentList.add(new AssignmentItem("purge_trans_start_time", "\"00:00-01:00\"", true));
        variableAssignmentList.add(new AssignmentItem("push_agg_input_row_count_threshold", "10000", true));
        variableAssignmentList.add(new AssignmentItem("push_policy", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("rbo_heuristic_join_reorder_limit", "8", true));
        variableAssignmentList.add(new AssignmentItem("reload_scale_out_status_debug", "false", true));
//        variableAssignmentList.add(new AssignmentItem("repartition_enable_rebuild_gsi", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("repartition_force_gsi_name", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("repartition_skip_cleanup", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("repartition_skip_cutover", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("replicate_filter_to_primary", "true", true));
        variableAssignmentList.add(new AssignmentItem("resume_scan_step_size", "512", true));
        variableAssignmentList.add(new AssignmentItem("runtime_filter_fpp", "0.03", true));
        variableAssignmentList.add(new AssignmentItem("runtime_filter_probe_min_row_count", "10000000", true));
        variableAssignmentList.add(new AssignmentItem("sample_percentage", "-1.0", true));
//        variableAssignmentList.add(new AssignmentItem("scale_out_debug", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("scale_out_debug_wait_time_in_wo", "0", true));
        variableAssignmentList
            .add(new AssignmentItem("scale_out_drop_database_after_switch_datasource", "false", true));
//        variableAssignmentList.add(new AssignmentItem("scale_out_final_db_status_debug", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("scale_out_final_table_status_debug", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("scale_out_write_debug", "\"\"", true));
//        variableAssignmentList.add(new AssignmentItem("scale_out_write_performance_test", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_backfill_batch_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_backfill_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_backfill_speed_limitation", "300000", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_backfill_speed_min", "10000", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_check_after_backfill", "true", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_check_batch_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_check_parallelism", "-1", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_check_speed_limitation", "150000", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_check_speed_min", "100000", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_dml_pushdown_batch_limit", "32", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_dml_pushdown_optimization", "true", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_early_fail_number", "1024", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_logicaltable_parallelism", "4", true));
        variableAssignmentList.add(new AssignmentItem("scaleout_task_retry_time", "3", true));
        variableAssignmentList.add(new AssignmentItem("segmented", "false", true));
        variableAssignmentList.add(new AssignmentItem("segmented_count", "0", true));
        variableAssignmentList.add(new AssignmentItem("select_into_outfile_buffer_size", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("sequential_concurrent_policy", "false", true));
        variableAssignmentList.add(new AssignmentItem("share_storage_mode", "false", true));
        variableAssignmentList.add(new AssignmentItem("show_hash_partitions_by_range", "false", true));
        variableAssignmentList.add(new AssignmentItem("show_table_group_name", "false", true));
        variableAssignmentList.add(new AssignmentItem("show_tables_cache", "false", true));
        variableAssignmentList.add(new AssignmentItem("show_tables_from_rule_only", "false", true));
        variableAssignmentList.add(new AssignmentItem("skip_readonly_check", "false", true));
        variableAssignmentList.add(new AssignmentItem("slow_sql_time", "1000", true));
        variableAssignmentList.add(new AssignmentItem("socket_timeout", "900000", true));
        variableAssignmentList.add(new AssignmentItem("spill_output_max_buffer_size", "32000000", true));
        variableAssignmentList.add(new AssignmentItem("spm_evolution_rate", "1.0", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_accepted_plan_size_per_baseline", "8", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_baseline_info_sql_length", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_baseline_size", "500", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_plan_info_error_count", "16", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_plan_info_plan_length", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_unaccepted_plan_evolution_times", "5", true));
        variableAssignmentList.add(new AssignmentItem("spm_max_unaccepted_plan_size_per_baseline", "3", true));
        variableAssignmentList.add(new AssignmentItem("statistic_collector_from_rule", "true", true));
        variableAssignmentList.add(new AssignmentItem("statistic_sample_rate", "-1.0", true));
        variableAssignmentList.add(new AssignmentItem("storage_check_on_gsi", "true", true));
        variableAssignmentList.add(new AssignmentItem("storage_supports_bloom_filter", "false", true));
        variableAssignmentList.add(new AssignmentItem("support_read_follower_strategy", "DEFAULT", true));
        variableAssignmentList.add(new AssignmentItem("switch_group_only", "false", true));
//        variableAssignmentList.add(new AssignmentItem("tablegroup_debug", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("topn_min_num", "3", true));
        variableAssignmentList.add(new AssignmentItem("topn_size", "15", true));
        variableAssignmentList.add(new AssignmentItem("truncate_table_with_gsi", "false", true));
//        variableAssignmentList.add(new AssignmentItem("trx_class_required", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("tso_heartbeat_interval", "60000", true));
        variableAssignmentList.add(new AssignmentItem("tso_omit_global_tx_log", "false", true));
        variableAssignmentList.add(new AssignmentItem("update_delete_select_batch_size", "1000", true));
        variableAssignmentList.add(new AssignmentItem("update_delete_select_limit", "1000000", true));
        variableAssignmentList.add(new AssignmentItem("using_rds_result_skip", "false", true));
        variableAssignmentList.add(new AssignmentItem("variable_expire_time", "300000", true));
        variableAssignmentList.add(new AssignmentItem("wait_bloom_filter_timeout_ms", "60000", true));
        variableAssignmentList.add(new AssignmentItem("wait_runtime_filter_for_scan", "true", true));
        variableAssignmentList.add(new AssignmentItem("window_func_optimize", "true", true));
        variableAssignmentList.add(new AssignmentItem("window_func_reorder_join", "false", true));
        variableAssignmentList.add(new AssignmentItem("window_func_subquery_condition", "false", true));
        variableAssignmentList.add(new AssignmentItem("workload_io_threshold", "15000", true));
        variableAssignmentList.add(new AssignmentItem("workload_type", "NULL", true));
        variableAssignmentList.add(new AssignmentItem("xa_recover_interval", "5", true));
        variableAssignmentList.add(new AssignmentItem("xproto_max_dn_concurrent", "500", true));
        variableAssignmentList.add(new AssignmentItem("xproto_max_dn_wait_connection", "32", true));
        batchSetGlobalVariableTest(variableAssignmentList, false);
    }

    @Test
    public void setDnGlobalVariableTest() throws Exception {
        enableSetGlobal();

        List<AssignmentItem> variableAssignmentList = new LinkedList<>();
//        variableAssignmentList.add(new AssignmentItem("appliedindex_force_delay", "0", true));
//        variableAssignmentList.add(new AssignmentItem("async_commit", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("async_commit_check_interval", "2", true));
//        variableAssignmentList.add(new AssignmentItem("auto_generate_certs", "ON", true));
//        variableAssignmentList.add(new AssignmentItem("auto_increment_increment", "1", true));
        variableAssignmentList.add(new AssignmentItem("auto_increment_offset", "2649", true));
//        variableAssignmentList.add(new AssignmentItem("autocommit", "ON", true));
        variableAssignmentList.add(new AssignmentItem("automatic_sp_privileges", "ON", true));
//        variableAssignmentList.add(new AssignmentItem("avoid_temporal_upgrade", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("back_log", "900", true));
//        variableAssignmentList.add(new AssignmentItem("basedir", "\"/u01/xcluster_current/\"", true));
        variableAssignmentList.add(new AssignmentItem("big_tables", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("bind_address", "*", true));
        variableAssignmentList.add(new AssignmentItem("binlog_cache_size", "32768", true));
        variableAssignmentList.add(new AssignmentItem("binlog_checksum", "CRC32", true));
        variableAssignmentList.add(new AssignmentItem("binlog_direct_non_transactional_updates", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_error_action", "ABORT_SERVER", true));
        variableAssignmentList.add(new AssignmentItem("binlog_format", "ROW", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_group_commit_sync_delay", "0", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_group_commit_sync_no_delay_count", "0", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_gtid_simple_recovery", "ON", true));
        variableAssignmentList.add(new AssignmentItem("binlog_max_flush_queue_time", "0", true));
        variableAssignmentList.add(new AssignmentItem("binlog_order_commits", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_row_event_max_size", "8192", true));
        variableAssignmentList.add(new AssignmentItem("binlog_row_image", "FULL", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_rows_query_key_content", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_rows_query_log_events", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_save_prepared_xa_on_rotate", "ON", true));
        variableAssignmentList.add(new AssignmentItem("binlog_stmt_cache_size", "32768", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_transaction_dependency_history_size", "25000", true));
//        variableAssignmentList.add(new AssignmentItem("binlog_transaction_dependency_tracking", "COMMIT_ORDER", true));
        variableAssignmentList.add(new AssignmentItem("block_encryption_mode", "\"aes-128-ecb\"", true));
//        variableAssignmentList.add(new AssignmentItem("boost_pk_access", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("bulk_insert_buffer_size", "8388608", true));
//        variableAssignmentList.add(new AssignmentItem("bulk_insert_ddl", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("character_set_client", "utf8", true));
        variableAssignmentList.add(new AssignmentItem("character_set_connection", "utf8", true));
//        variableAssignmentList.add(new AssignmentItem("character_set_database", "utf8", true));
//        variableAssignmentList.add(new AssignmentItem("character_set_filesystem", "binary", true));
        variableAssignmentList.add(new AssignmentItem("character_set_results", "utf8", true));
        variableAssignmentList.add(new AssignmentItem("character_set_server", "utf8", true));
//        variableAssignmentList.add(new AssignmentItem("character_set_system", "utf8", true));
//        variableAssignmentList
//            .add(new AssignmentItem("character_sets_dir", "/u01/xcluster_current/share/charsets/", true));
        variableAssignmentList.add(new AssignmentItem("check_proxy_users", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("cluster_id", "0", true));
        variableAssignmentList.add(new AssignmentItem("collation_connection", "utf8_general_ci", true));
//        variableAssignmentList.add(new AssignmentItem("collation_database", "utf8_general_ci", true));
        variableAssignmentList.add(new AssignmentItem("collation_server", "utf8_general_ci", true));
//        variableAssignmentList.add(new AssignmentItem("commit_lock_done_count", "1", true));
//        variableAssignmentList.add(new AssignmentItem("commit_pos_watcher", "ON", true));
//        variableAssignmentList.add(new AssignmentItem("commit_pos_watcher_interval", "1000000", true));
        variableAssignmentList.add(new AssignmentItem("completion_type", "NO_CHAIN", true));
        variableAssignmentList.add(new AssignmentItem("concurrent_insert", "AUTO", true));
        variableAssignmentList.add(new AssignmentItem("connect_timeout", "10", true));
        variableAssignmentList.add(new AssignmentItem("consensus_auto_leader_transfer", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_auto_leader_transfer_check_seconds", "60", true));
        variableAssignmentList.add(new AssignmentItem("consensus_auto_reset_match_index", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_check_commit_index_interval", "1000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_check_large_event", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_checksum", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_configure_change_timeout", "60000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_disable_election", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_disable_fifo_cache", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_dump_mode", "APPLIED", true));
        variableAssignmentList.add(new AssignmentItem("consensus_dynamic_easyindex", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_easy_pool_size", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_election_timeout", "5000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_flow_control", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("consensus_force_promote", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_force_recovery", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_force_sync_epoch_diff", "0", true));
        variableAssignmentList.add(new AssignmentItem("consensus_heartbeat_thread_cnt", "0", true));
        variableAssignmentList.add(new AssignmentItem("consensus_io_thread_cnt", "8", true));
        variableAssignmentList.add(new AssignmentItem("consensus_large_batch_ratio", "50", true));
        variableAssignmentList.add(new AssignmentItem("consensus_large_event_limit", "1073741824", true));
        variableAssignmentList.add(new AssignmentItem("consensus_large_event_split_size", "2097152", true));
        variableAssignmentList.add(new AssignmentItem("consensus_large_trx", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_large_trx_split_size", "2097152", true));
        variableAssignmentList.add(new AssignmentItem("consensus_leader_stop_apply", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_leader_stop_apply_time", "0", true));
        variableAssignmentList.add(new AssignmentItem("consensus_learner_heartbeat", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_learner_pipelining", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_learner_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("consensus_log_cache_size", "67108864", true));
        variableAssignmentList.add(new AssignmentItem("consensus_log_level", "LOG_ERROR", true));
        variableAssignmentList.add(new AssignmentItem("consensus_max_delay_index", "50000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_max_log_size", "20971520", true));
        variableAssignmentList.add(new AssignmentItem("consensus_max_packet_size", "131072", true));
        variableAssignmentList.add(new AssignmentItem("consensus_max_pipelining_entry_size", "2097152", true));
        variableAssignmentList.add(new AssignmentItem("consensus_min_delay_index", "5000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_msg_compress_option", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("consensus_new_follower_threshold", "10000", true));
        variableAssignmentList.add(new AssignmentItem("consensus_old_compact_mode", "ON", true));
        variableAssignmentList.add(new AssignmentItem("consensus_optimistic_heartbeat", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_pipelining_timeout", "1", true));
        variableAssignmentList.add(new AssignmentItem("consensus_prefetch_cache_size", "67108864", true));
        variableAssignmentList.add(new AssignmentItem("consensus_prefetch_fast_fetch", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_prefetch_wakeup_ratio", "2", true));
        variableAssignmentList.add(new AssignmentItem("consensus_prefetch_window_size", "10", true));
        variableAssignmentList.add(new AssignmentItem("consensus_replicate_with_cache_log", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("consensus_send_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("consensus_sync_follower_meta_interva", "1", true));
        variableAssignmentList.add(new AssignmentItem("consensus_worker_thread_cnt", "8", true));
        variableAssignmentList.add(new AssignmentItem("consensuslog_revise", "ON", true));
        variableAssignmentList.add(new AssignmentItem("core_file", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("datadir", "/u01/my3306/data/", true));
//        variableAssignmentList.add(new AssignmentItem("date_format", "%Y-%m-%d", true));
//        variableAssignmentList.add(new AssignmentItem("datetime_format", "%Y-%m-%d %H:%i:%s", true));
//        variableAssignmentList.add(new AssignmentItem("dbfs_data_home_dir", "./", true));
        variableAssignmentList.add(new AssignmentItem("default_authentication_plugin", "mysql_native_password", true));
        variableAssignmentList.add(new AssignmentItem("default_password_lifetime", "0", true));
        variableAssignmentList.add(new AssignmentItem("default_storage_engine", "InnoDB", true));
        variableAssignmentList.add(new AssignmentItem("default_tmp_storage_engine", "InnoDB", true));
        variableAssignmentList.add(new AssignmentItem("default_week_format", "0", true));
        variableAssignmentList.add(new AssignmentItem("delay_key_write", "ON", true));
        variableAssignmentList.add(new AssignmentItem("delayed_insert_limit", "100", true));
        variableAssignmentList.add(new AssignmentItem("delayed_insert_timeout", "300", true));
        variableAssignmentList.add(new AssignmentItem("delayed_queue_size", "1000", true));
        variableAssignmentList.add(new AssignmentItem("disable_drc_learner", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("disabled_storage_engines", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("disconnect_on_expired_password", "ON", true));
        variableAssignmentList.add(new AssignmentItem("div_precision_increment", "4", true));
        variableAssignmentList.add(new AssignmentItem("enable_appliedindex_checker", "ON", true));
        variableAssignmentList.add(new AssignmentItem("enable_binlog_fadvise", "ON", true));
        variableAssignmentList.add(new AssignmentItem("enable_gts", "ON", true));
        variableAssignmentList.add(new AssignmentItem("end_markers_in_json", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("enforce_gtid_consistency", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("eq_range_index_dive_limit", "200", true));
        variableAssignmentList.add(new AssignmentItem("event_scheduler", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("expire_logs_days", "0", true));
        variableAssignmentList.add(new AssignmentItem("explicit_defaults_for_timestamp", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("extra_max_connections", "151", true));
        variableAssignmentList.add(new AssignmentItem("extra_port", "0", true));
        variableAssignmentList.add(new AssignmentItem("favor_non_spatial_index", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("flush", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("flush_time", "0", true));
        variableAssignmentList.add(new AssignmentItem("force_revise", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("foreign_key_checks", "ON", true));
//        variableAssignmentList.add(new AssignmentItem("ft_boolean_syntax", "+ -><()~*:\" \"&|", true));
        variableAssignmentList.add(new AssignmentItem("ft_max_word_len", "84", true));
        variableAssignmentList.add(new AssignmentItem("ft_min_word_len", "4", true));
        variableAssignmentList.add(new AssignmentItem("ft_query_expansion_limit", "20", true));
        variableAssignmentList.add(new AssignmentItem("ft_stopword_file", "\"(built-in)\"", true));
        variableAssignmentList.add(new AssignmentItem("general_log", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("general_log_file", "/u01/my3306/data/83d3f9c266eb.log", true));
        variableAssignmentList.add(new AssignmentItem("gis_polygon_compatibility_format", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("group_concat_max_len", "1024", true));
//        variableAssignmentList
//            .add(new AssignmentItem("gtid_executed", /"\"309ccea0-9139-11eb-9763-0242c0a80502:1-6781\"", true));
        variableAssignmentList.add(new AssignmentItem("gtid_executed_compression_period", "1000", true));
//        variableAssignmentList.add(new AssignmentItem("gtid_mode", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("gtid_owned", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("gtid_purged", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("gts_lease", "5000", true));
        variableAssignmentList.add(new AssignmentItem("gu_recycle_interval", "2", true));
        variableAssignmentList.add(new AssignmentItem("gu_recycle_max_fetch_count", "1", true));
        variableAssignmentList.add(new AssignmentItem("gu_recycle_try_lock_count", "3", true));
        variableAssignmentList.add(new AssignmentItem("have_compress", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_crypt", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_dynamic_loading", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_geometry", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_openssl", "DISABLED", true));
        variableAssignmentList.add(new AssignmentItem("have_profiling", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_query_cache", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_rtree_keys", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_ssl", "DISABLED", true));
        variableAssignmentList.add(new AssignmentItem("have_statement_timeout", "YES", true));
        variableAssignmentList.add(new AssignmentItem("have_symlink", "YES", true));
        variableAssignmentList.add(new AssignmentItem("host_cache_size", "853", true));
        variableAssignmentList.add(new AssignmentItem("hostname", "83d3f9c266eb", true));
        variableAssignmentList.add(new AssignmentItem("hotspot", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_fast_insert_dup", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_for_autocommit", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_gu_lock_wait_timeout", "100000000", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_gu_timedlock", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_hash_lock_retry_interval", "100", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_hash_lock_retry_time", "0", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_lock_type", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("hotspot_update_max_wait_time", "100", true));
        variableAssignmentList.add(new AssignmentItem("ignore_builtin_innodb", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("ignore_db_dirs", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("implicit_primary_key", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("index_hint_warning", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("index_visible_enable", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("init_connect", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("init_file", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("init_slave", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_adaptive_flushing", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_adaptive_flushing_lwm", "10", true));
        variableAssignmentList.add(new AssignmentItem("innodb_adaptive_hash_index", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_adaptive_hash_index_parts", "8", true));
        variableAssignmentList.add(new AssignmentItem("innodb_adaptive_max_sleep_delay", "150000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_api_bk_commit_interval", "5", true));
        variableAssignmentList.add(new AssignmentItem("innodb_api_disable_rowlock", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_api_enable_binlog", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_api_enable_mdl", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_api_trx_level", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_autoextend_increment", "64", true));
        variableAssignmentList.add(new AssignmentItem("innodb_autoinc_lock_mode", "2", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_chunk_size", "134217728", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_dump_at_shutdown", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_dump_now", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_dump_pct", "25", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_fast_init", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_fast_init_final_size", "134217728", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_fast_init_increment", "134217728", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_fast_init_interval", "5", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_filename", "ib_buffer_pool", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_instances", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_load_abort", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_load_at_startup", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_load_now", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_buffer_pool_size", "2147483648", true));
        variableAssignmentList.add(new AssignmentItem("innodb_change_buffer_max_size", "25", true));
        variableAssignmentList.add(new AssignmentItem("innodb_change_buffering", "all", true));
        variableAssignmentList.add(new AssignmentItem("innodb_checksum_algorithm", "crc32", true));
        variableAssignmentList.add(new AssignmentItem("innodb_checksums", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_cmp_per_index_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_compression_convert_length", "256", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_compression_level", "6", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_zip_mem_use_heap", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_zip_threshold", "96", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_zlib_strategy", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_column_zlib_wrap", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_commit_concurrency", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_commit_seq", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("innodb_compression_failure_threshold_pct", "5", true));
        variableAssignmentList.add(new AssignmentItem("innodb_compression_level", "6", true));
        variableAssignmentList.add(new AssignmentItem("innodb_compression_pad_pct_max", "50", true));
        variableAssignmentList.add(new AssignmentItem("innodb_concurrency_tickets", "5000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_concurrency_tickets_hotspot", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_csn_mode", "persist", true));
//        variableAssignmentList.add(new AssignmentItem("innodb_data_file_path", "ibdata1:12M:autoextend", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge_all_at_shutdown", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge_dir", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge_immediate", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge_interval", "100", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_file_purge_max_size", "512", true));
        variableAssignmentList.add(new AssignmentItem("innodb_data_home_dir", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_deadlock_detect", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_default_row_format", "dynamic", true));
        variableAssignmentList.add(new AssignmentItem("innodb_disable_sort_file_cache", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_doublewrite", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_encrypt_algorithm", "AES_256_CBC", true));
        variableAssignmentList.add(new AssignmentItem("innodb_fast_shutdown", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_file_format", "Barracuda", true));
        variableAssignmentList.add(new AssignmentItem("innodb_file_format_check", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_file_format_max", "Barracuda", true));
        variableAssignmentList.add(new AssignmentItem("innodb_file_per_table", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_fill_factor", "100", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flush_log_at_timeout", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flush_log_at_trx_commit", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flush_method", "O_DIRECT", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flush_neighbors", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flush_sync", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_flushing_avg_loops", "30", true));
        variableAssignmentList.add(new AssignmentItem("innodb_force_load_corrupted", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_force_recovery", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_aux_table", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_cache_size", "8000000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_enable_diag_print", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_enable_stopword", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_max_token_size", "84", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_min_token_size", "3", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_num_word_optimize", "2000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_result_cache_limit", "2000000000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_server_stopword_table", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_sort_pll_degree", "2", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_total_cache_size", "640000000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_ft_user_stopword_table", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_geom_covering_index", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_heartbeat_seq", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_hotspot_kill_lock_holder", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_hotspot_lock_wait_timeout", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_idle_flush_pct", "5", true));
        variableAssignmentList.add(new AssignmentItem("innodb_init_io_limit", "4294967296", true));
        variableAssignmentList.add(new AssignmentItem("innodb_init_io_limit_normal", "9223372036853727232", true));
        variableAssignmentList.add(new AssignmentItem("innodb_io_capacity", "20000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_io_capacity_max", "40000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_kill_idle_transaction", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_large_prefix", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_lock_wait_timeout", "50", true));
        variableAssignmentList.add(new AssignmentItem("innodb_locks_unsafe_for_binlog", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_buffer_size", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_checksums", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_compressed_pages", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_file_size", "50331648", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_files_in_group", "2", true));
//        variableAssignmentList.add(new AssignmentItem("innodb_log_group_home_dir", "./", true));
        variableAssignmentList.add(new AssignmentItem("innodb_log_write_ahead_size", "8192", true));
        variableAssignmentList.add(new AssignmentItem("innodb_lru_scan_depth", "1024", true));
        variableAssignmentList.add(new AssignmentItem("innodb_max_dirty_pages_pct", "75.000000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_max_dirty_pages_pct_lwm", "0.000000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_max_purge_lag", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_max_purge_lag_delay", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_max_undo_log_size", "1073741824", true));
        variableAssignmentList.add(new AssignmentItem("innodb_monitor_disable", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_monitor_enable", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_monitor_reset", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_monitor_reset_all", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_numa_interleave", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_old_blocks_pct", "37", true));
        variableAssignmentList.add(new AssignmentItem("innodb_old_blocks_time", "1000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_online_alter_log_max_size", "134217728", true));
        variableAssignmentList.add(new AssignmentItem("innodb_open_files", "3000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_optimize_fulltext_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_page_cleaners", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_page_size", "16384", true));
        variableAssignmentList.add(new AssignmentItem("innodb_prepare_wait_timeout", "5000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_print_all_deadlocks", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_print_data_file_purge_process", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_purge_batch_size", "300", true));
        variableAssignmentList.add(new AssignmentItem("innodb_purge_history", "600000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_purge_rseg_truncate_frequency", "128", true));
        variableAssignmentList.add(new AssignmentItem("innodb_purge_threads", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_quick_check_table_lock", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_random_read_ahead", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_read_ahead_threshold", "56", true));
        variableAssignmentList.add(new AssignmentItem("innodb_read_io_threads", "4", true));
        variableAssignmentList.add(new AssignmentItem("innodb_read_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_replication_delay", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_rollback_on_timeout", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_rollback_segments", "128", true));
        variableAssignmentList.add(new AssignmentItem("innodb_show_verbose_deadlock", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_snapshot_seq", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("innodb_sort_buffer_size", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("innodb_spin_wait_delay", "9", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_auto_recalc", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_intrinsic_table", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_method", "nulls_equal", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_on_metadata", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_persistent", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_persistent_sample_pages", "20", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_sample_pages", "8", true));
        variableAssignmentList.add(new AssignmentItem("innodb_stats_transient_sample_pages", "8", true));
        variableAssignmentList.add(new AssignmentItem("innodb_status_output", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_status_output_locks", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_strict_concurrency", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_strict_mode", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_support_xa", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_sync_array_size", "1", true));
        variableAssignmentList.add(new AssignmentItem("innodb_sync_spin_loops", "30", true));
        variableAssignmentList.add(new AssignmentItem("innodb_table_data_free_enhance", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_table_locks", "ON", true));
        variableAssignmentList.add(new AssignmentItem("innodb_temp_data_file_path", "\"ibtmp1:12M:autoextend\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_thread_concurrency", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_thread_extra_concurrency", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_thread_sleep_delay", "10000", true));
        variableAssignmentList.add(new AssignmentItem("innodb_tmpdir", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_transaction_group", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_truncate_algorithm", "in_place", true));
        variableAssignmentList.add(new AssignmentItem("innodb_undo_directory", "\"./\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_undo_log_truncate", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_undo_logs", "128", true));
        variableAssignmentList.add(new AssignmentItem("innodb_undo_tablespaces", "0", true));
        variableAssignmentList.add(new AssignmentItem("innodb_use_native_aio", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("innodb_version", "\"5.7.14\"", true));
        variableAssignmentList.add(new AssignmentItem("innodb_write_io_threads", "4", true));
        variableAssignmentList.add(new AssignmentItem("interactive_timeout", "28800", true));
        variableAssignmentList.add(new AssignmentItem("internal_tmp_disk_storage_engine", "InnoDB", true));
        variableAssignmentList.add(new AssignmentItem("io_state", "ON", true));
        variableAssignmentList.add(new AssignmentItem("io_state_check_interval", "5", true));
        variableAssignmentList.add(new AssignmentItem("io_state_delay_threshold", "500000", true));
        variableAssignmentList.add(new AssignmentItem("io_state_mode", "ALL", true));
        variableAssignmentList.add(new AssignmentItem("io_state_myfs", "ON", true));
        variableAssignmentList.add(new AssignmentItem("io_state_polarfs", "ON", true));
        variableAssignmentList.add(new AssignmentItem("io_state_retry_times", "6", true));
        variableAssignmentList.add(new AssignmentItem("join_buffer_size", "262144", true));
        variableAssignmentList.add(new AssignmentItem("json_new_double_format", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("keep_files_on_create", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("key_buffer_size", "8388608", true));
        variableAssignmentList.add(new AssignmentItem("key_cache_age_threshold", "300", true));
        variableAssignmentList.add(new AssignmentItem("key_cache_block_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("key_cache_division_limit", "100", true));
        variableAssignmentList.add(new AssignmentItem("keyring_operations", "ON", true));
        variableAssignmentList.add(new AssignmentItem("kill_idle_transaction", "0", true));
        variableAssignmentList.add(new AssignmentItem("large_files_support", "ON", true));
        variableAssignmentList.add(new AssignmentItem("large_page_size", "0", true));
        variableAssignmentList.add(new AssignmentItem("large_pages", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("lc_messages", "en_US", true));
//        variableAssignmentList.add(new AssignmentItem("lc_messages_dir", "/u01/xcluster_current/share/", true));
        variableAssignmentList.add(new AssignmentItem("lc_time_names", "en_US", true));
        variableAssignmentList.add(new AssignmentItem("license", "GPL", true));
        variableAssignmentList.add(new AssignmentItem("local_infile", "ON", true));
        variableAssignmentList.add(new AssignmentItem("lock_instance_mode", "LOCK_NON", true));
        variableAssignmentList.add(new AssignmentItem("lock_wait_timeout", "31536000", true));
        variableAssignmentList.add(new AssignmentItem("locked_in_memory", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_bin", "ON", true));
//        variableAssignmentList.add(new AssignmentItem("log_bin_basename", "/u01/my3306/data/master-bin", true));
//        variableAssignmentList.add(new AssignmentItem("log_bin_index", "/u01/my3306/data/master-bin.index", true));
        variableAssignmentList.add(new AssignmentItem("log_bin_trust_function_creators", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_bin_use_v1_row_events", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_builtin_as_identified_by_password", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("log_error", "/u01/my3306/log/mysql.err", true));
        variableAssignmentList.add(new AssignmentItem("log_error_verbosity", "3", true));
        variableAssignmentList.add(new AssignmentItem("log_output", "FILE", true));
        variableAssignmentList.add(new AssignmentItem("log_queries_not_using_indexes", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_slave_updates", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_slow_admin_statements", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_slow_slave_statements", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_slow_verbosity", "\"microtime,query_plan,innodb\"", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_buffer_size", "134217728", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_fifo", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_info", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_info_index", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_info_mock_fifo", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_info_port", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_sql_info_tables", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_statements_unsafe_for_binlog", "ON", true));
        variableAssignmentList.add(new AssignmentItem("log_syslog", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("log_syslog_facility", "daemon", true));
        variableAssignmentList.add(new AssignmentItem("log_syslog_include_pid", "ON", true));
        variableAssignmentList.add(new AssignmentItem("log_syslog_tag", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("log_throttle_queries_not_using_indexes", "0", true));
        variableAssignmentList.add(new AssignmentItem("log_timestamps", "UTC", true));
        variableAssignmentList.add(new AssignmentItem("log_warnings", "2", true));
        variableAssignmentList.add(new AssignmentItem("long_query_time", "10.000000", true));
        variableAssignmentList.add(new AssignmentItem("low_priority_updates", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("lower_case_file_system", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("lower_case_table_names", "1", true));
        variableAssignmentList.add(new AssignmentItem("maintain_user_list", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("master_info_repository", "FILE", true));
        variableAssignmentList.add(new AssignmentItem("master_verify_checksum", "OFF", true));
//        variableAssignmentList.add(new AssignmentItem("max_allowed_packet", "41940992", true));
        variableAssignmentList.add(new AssignmentItem("max_binlog_cache_size", "18446744073709547520", true));
        variableAssignmentList.add(new AssignmentItem("max_binlog_size", "1073741824", true));
        variableAssignmentList.add(new AssignmentItem("max_binlog_stmt_cache_size", "18446744073709547520", true));
        variableAssignmentList.add(new AssignmentItem("max_connect_errors", "10000", true));
//        variableAssignmentList.add(new AssignmentItem("max_connections", "5000", true));
        variableAssignmentList.add(new AssignmentItem("max_delayed_threads", "20", true));
        variableAssignmentList.add(new AssignmentItem("max_digest_length", "1024", true));
        variableAssignmentList.add(new AssignmentItem("max_error_count", "64", true));
        variableAssignmentList.add(new AssignmentItem("max_execution_time", "0", true));
        variableAssignmentList.add(new AssignmentItem("max_heap_table_size", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("max_insert_delayed_threads", "20", true));
        variableAssignmentList.add(new AssignmentItem("max_join_size", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("max_length_for_sort_data", "1024", true));
        variableAssignmentList.add(new AssignmentItem("max_points_in_geometry", "65536", true));
        variableAssignmentList.add(new AssignmentItem("max_prepared_stmt_count", "16382", true));
        variableAssignmentList.add(new AssignmentItem("max_relay_log_size", "0", true));
        variableAssignmentList.add(new AssignmentItem("max_seeks_for_key", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("max_sort_length", "1024", true));
        variableAssignmentList.add(new AssignmentItem("max_sp_recursion_depth", "0", true));
        variableAssignmentList.add(new AssignmentItem("max_tmp_tables", "32", true));
//        variableAssignmentList.add(new AssignmentItem("max_user_connections", "0", true));
        variableAssignmentList.add(new AssignmentItem("max_write_lock_count", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("maximum_protection", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("metadata_locks_cache_size", "1024", true));
        variableAssignmentList.add(new AssignmentItem("metadata_locks_hash_instances", "8", true));
        variableAssignmentList.add(new AssignmentItem("min_examined_row_limit", "0", true));
        variableAssignmentList.add(new AssignmentItem("multi_range_count", "256", true));
        variableAssignmentList.add(new AssignmentItem("myisam_data_pointer_size", "6", true));
        variableAssignmentList.add(new AssignmentItem("myisam_max_sort_file_size", "9223372036853727232", true));
        variableAssignmentList.add(new AssignmentItem("myisam_mmap_size", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("myisam_recover_options", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("myisam_repair_threads", "1", true));
        variableAssignmentList.add(new AssignmentItem("myisam_sort_buffer_size", "8388608", true));
        variableAssignmentList.add(new AssignmentItem("myisam_stats_method", "nulls_unequal", true));
        variableAssignmentList.add(new AssignmentItem("myisam_use_mmap", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("mysql_native_password_proxy_users", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("net_buffer_length", "16384", true));
        variableAssignmentList.add(new AssignmentItem("net_read_timeout", "30", true));
        variableAssignmentList.add(new AssignmentItem("net_retry_count", "10", true));
        variableAssignmentList.add(new AssignmentItem("net_write_timeout", "60", true));
        variableAssignmentList.add(new AssignmentItem("new", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("ngram_token_size", "2", true));
        variableAssignmentList.add(new AssignmentItem("offline_mode", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("old", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("old_alter_table", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("old_passwords", "0", true));
        variableAssignmentList.add(new AssignmentItem("old_show_timestamp", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("open_files_limit", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("opt_indexstat", "ON", true));
        variableAssignmentList.add(new AssignmentItem("opt_rds_audit_log_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("opt_tablestat", "ON", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_prune_level", "1", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_search_depth", "62", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_switch",
            "\"index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,duplicateweedout=on,subquery_materialization_cost_based=on,use_index_extensions=on,condition_fanout_filter=on,derived_merge=on\"",
            true));
        variableAssignmentList.add(new AssignmentItem("optimizer_trace", "\"enabled=off,one_line=off\"", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_trace_features",
            "\"greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on\"", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_trace_limit", "1", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_trace_max_mem_size", "16384", true));
        variableAssignmentList.add(new AssignmentItem("optimizer_trace_offset", "-1", true));
        variableAssignmentList.add(new AssignmentItem("parser_max_mem_size", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("performance_point_dbug_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("performance_point_enabled", "ON", true));
        variableAssignmentList.add(new AssignmentItem("performance_point_iostat_interval", "2", true));
        variableAssignmentList.add(new AssignmentItem("performance_point_iostat_volume_size", "10000", true));
        variableAssignmentList.add(new AssignmentItem("performance_point_lock_rwlock_enabled", "ON", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema", "ON", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_accounts_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_digests_size", "10000", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_events_stages_history_long_size", "10000", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_events_stages_history_size", "10", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_events_statements_history_long_size", "10000", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_events_statements_history_size", "10", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_events_transactions_history_long_size", "10000", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_events_transactions_history_size", "10", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_events_waits_history_long_size", "10000", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_events_waits_history_size", "10", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_hosts_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_cond_classes", "80", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_cond_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_digest_length", "1024", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_file_classes", "80", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_file_handles", "32768", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_file_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_index_stat", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_memory_classes", "322", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_metadata_locks", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_mutex_classes", "200", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_mutex_instances", "-1", true));
        variableAssignmentList
            .add(new AssignmentItem("performance_schema_max_prepared_statements_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_program_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_rwlock_classes", "50", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_rwlock_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_socket_classes", "10", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_socket_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_sql_text_length", "1024", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_stage_classes", "150", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_statement_classes", "244", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_statement_stack", "10", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_table_handles", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_table_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_table_lock_stat", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_thread_classes", "50", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_max_thread_instances", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_session_connect_attrs_size", "512", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_setup_actors_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_setup_objects_size", "-1", true));
        variableAssignmentList.add(new AssignmentItem("performance_schema_users_size", "-1", true));
//        variableAssignmentList.add(new AssignmentItem("pid_file", "/u01/my3306/data/83d3f9c266eb.pid", true));
//        variableAssignmentList.add(new AssignmentItem("plugin_dir", "/u01/xcluster_current/lib/plugin/", true));
        variableAssignmentList.add(new AssignmentItem("polarfs_cache_size", "268435456", true));
        variableAssignmentList.add(new AssignmentItem("polarfs_cluster", "disk", true));
        variableAssignmentList.add(new AssignmentItem("polarfs_data_home_dir", "\"./\"", true));
        variableAssignmentList.add(new AssignmentItem("polarfs_hostid", "1", true));
        variableAssignmentList.add(new AssignmentItem("polarfs_pbdname", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_cache", "1", true));
        variableAssignmentList.add(new AssignmentItem("polarx_connect_timeout", "30", true));
        variableAssignmentList.add(new AssignmentItem("polarx_idle_worker_thread_timeout", "60", true));
        variableAssignmentList.add(new AssignmentItem("polarx_max_allowed_packet", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("polarx_max_connections", "5000", true));
        variableAssignmentList.add(new AssignmentItem("polarx_min_worker_threads", "2", true));
        variableAssignmentList.add(new AssignmentItem("polarx_plan_cache_capacity", "320", true));
        variableAssignmentList.add(new AssignmentItem("polarx_plan_cache_free_queue_threshold", "1000", true));
        variableAssignmentList.add(new AssignmentItem("polarx_plan_cache_hot_count", "10", true));
        variableAssignmentList.add(new AssignmentItem("polarx_port", "31306", true));
        variableAssignmentList.add(new AssignmentItem("polarx_query_cache_capacity", "320", true));
        variableAssignmentList.add(new AssignmentItem("polarx_query_cache_free_queue_threshold", "1000", true));
        variableAssignmentList.add(new AssignmentItem("polarx_query_cache_hot_count", "10", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_ca", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_capath", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_cert", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_cipher", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_crl", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_crlpath", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("polarx_ssl_key", "\"\"", true));
        variableAssignmentList
            .add(new AssignmentItem("polarx_udf_function_list", "\"bloomfilter,hyperloglog,hllndv\"", true));
        variableAssignmentList.add(new AssignmentItem("port", "3306", true));
        variableAssignmentList.add(new AssignmentItem("preload_buffer_size", "32768", true));
        variableAssignmentList.add(new AssignmentItem("profiling", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("profiling_history_size", "15", true));
        variableAssignmentList.add(new AssignmentItem("protocol_meta_optimize", "ON", true));
        variableAssignmentList.add(new AssignmentItem("protocol_version", "10", true));
        variableAssignmentList.add(new AssignmentItem("query_alloc_block_size", "8192", true));
        variableAssignmentList.add(new AssignmentItem("query_cache_limit", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("query_cache_min_res_unit", "4096", true));
        variableAssignmentList.add(new AssignmentItem("query_cache_size", "1048576", true));
        variableAssignmentList.add(new AssignmentItem("query_cache_type", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("query_cache_wlock_invalidate", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("query_prealloc_size", "8192", true));
        variableAssignmentList.add(new AssignmentItem("range_alloc_block_size", "4096", true));
        variableAssignmentList.add(new AssignmentItem("range_optimizer_max_mem_size", "8388608", true));
        variableAssignmentList.add(new AssignmentItem("rbr_exec_mode", "STRICT", true));
        variableAssignmentList.add(new AssignmentItem("rds_active_memory_profiling", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_last_log_row", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_buffer_size", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_cached_method", "ON", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_dir", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_file", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_reserve_all", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_row", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_log_version", "MYSQL_V0", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_max_sql_size", "2048", true));
        variableAssignmentList.add(new AssignmentItem("rds_audit_row_limit", "1000000", true));
        variableAssignmentList.add(new AssignmentItem("rds_check_core_file_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_filter_key_cmp_in_order", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_force_memory_to_innodb", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_force_myisam_to_innodb", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_global_access", "5", true));
        variableAssignmentList.add(new AssignmentItem("rds_is_dump_thread", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_kill_connections", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_proxy_user_list", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_push_gtid_default_interval", "10", true));
        variableAssignmentList.add(new AssignmentItem("rds_push_index_default_interval", "10", true));
        variableAssignmentList.add(new AssignmentItem("rds_release_date", "20210330", true));
        variableAssignmentList.add(new AssignmentItem("rds_reserved_connections", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_reset_all_filter", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_result_skip_counter", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_set_connection_id_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rds_sql_delete_filter", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_sql_insert_filter", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_sql_select_filter", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_sql_update_filter", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_trx_changes_idle_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_trx_idle_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_trx_readonly_idle_timeout", "0", true));
        variableAssignmentList.add(new AssignmentItem("rds_user_with_kill_option", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rds_version", "14", true));
        variableAssignmentList.add(new AssignmentItem("read_buffer_size", "131072", true));
        variableAssignmentList.add(new AssignmentItem("read_lsn", "0", true));
//        variableAssignmentList.add(new AssignmentItem("read_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("read_rnd_buffer_size", "262144", true));
        variableAssignmentList.add(new AssignmentItem("relay_log", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_basename", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_index", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_info_file", "\"relay-log.info\"", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_info_repository", "FILE", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_purge", "ON", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_recovery", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("relay_log_space_limit", "0", true));
        variableAssignmentList.add(new AssignmentItem("replica_read_timeout", "3000", true));
        variableAssignmentList.add(new AssignmentItem("report_host", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("report_password", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("report_port", "3306", true));
        variableAssignmentList.add(new AssignmentItem("report_user", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("require_secure_transport", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("reset_all_sql_hints", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("reset_consensus_prefetch_cache", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rotate_log_table_last_name", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("rpl_async_slave_protect", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rpl_receive_buffer_difftime", "5", true));
        variableAssignmentList.add(new AssignmentItem("rpl_receive_buffer_size", "0", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_timeout", "10000", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_trace_level", "32", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_wait_for_slave_count", "1", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_wait_no_slave", "ON", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_master_wait_point", "AFTER_SYNC", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_slave_enabled", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("rpl_semi_sync_slave_trace_level", "32", true));
        variableAssignmentList.add(new AssignmentItem("rpl_stop_slave_timeout", "31536000", true));
        variableAssignmentList.add(new AssignmentItem("secure_auth", "ON", true));
        variableAssignmentList.add(new AssignmentItem("secure_file_priv", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("server_id", "1", true));
        variableAssignmentList.add(new AssignmentItem("server_id_bits", "32", true));
        variableAssignmentList.add(new AssignmentItem("server_uuid", "\"309ccea0-9139-11eb-9763-0242c0a80502\"", true));
        variableAssignmentList.add(new AssignmentItem("session_track_gtids", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("session_track_index", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("session_track_schema", "ON", true));
        variableAssignmentList.add(new AssignmentItem("session_track_state_change", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("session_track_system_variables",
            "\"time_zone,autocommit,character_set_client,character_set_results,character_set_connection\"", true));
        variableAssignmentList.add(new AssignmentItem("session_track_transaction_info", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sha256_password_auto_generate_rsa_keys", "ON", true));
        variableAssignmentList.add(new AssignmentItem("sha256_password_private_key_path", "\"private_key.pem\"", true));
        variableAssignmentList.add(new AssignmentItem("sha256_password_proxy_users", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sha256_password_public_key_path", "\"public_key.pem\"", true));
        variableAssignmentList.add(new AssignmentItem("show_compatibility_56", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("show_ipk_info", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("show_ipk_user_list", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("show_old_temporals", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("skip_external_locking", "ON", true));
        variableAssignmentList.add(new AssignmentItem("skip_name_resolve", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("skip_networking", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("skip_show_database", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("slave_allow_batching", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("slave_checkpoint_group", "512", true));
        variableAssignmentList.add(new AssignmentItem("slave_checkpoint_period", "300", true));
        variableAssignmentList.add(new AssignmentItem("slave_compressed_protocol", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("slave_exec_mode", "STRICT", true));
//        variableAssignmentList.add(new AssignmentItem("slave_load_tmpdir", "/u01/my3306/tmp", true));
        variableAssignmentList.add(new AssignmentItem("slave_max_allowed_packet", "1073741824", true));
        variableAssignmentList.add(new AssignmentItem("slave_net_timeout", "60", true));
        variableAssignmentList.add(new AssignmentItem("slave_parallel_type", "DATABASE", true));
        variableAssignmentList.add(new AssignmentItem("slave_parallel_workers", "0", true));
        variableAssignmentList.add(new AssignmentItem("slave_pending_jobs_size_max", "16777216", true));
        variableAssignmentList.add(new AssignmentItem("slave_pr_mode", "SCHEMA", true));
        variableAssignmentList.add(new AssignmentItem("slave_preserve_commit_order", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("slave_read_no_lock", "OFF", true));
        variableAssignmentList
            .add(new AssignmentItem("slave_rows_search_algorithms", "\"TABLE_SCAN,INDEX_SCAN\"", true));
        variableAssignmentList.add(new AssignmentItem("slave_skip_errors", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("slave_sql_verify_checksum", "ON", true));
        variableAssignmentList.add(new AssignmentItem("slave_transaction_retries", "10", true));
        variableAssignmentList.add(new AssignmentItem("slave_type_conversions", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("slow_launch_time", "2", true));
        variableAssignmentList.add(new AssignmentItem("slow_query_log", "OFF", true));
//        variableAssignmentList
//            .add(new AssignmentItem("slow_query_log_file", "/u01/my3306/data/83d3f9c266eb-slow.log", true));
//        variableAssignmentList.add(new AssignmentItem("socket", "/u01/my3306/run/mysql.sock", true));
        variableAssignmentList.add(new AssignmentItem("sort_buffer_size", "262144", true));
        variableAssignmentList.add(new AssignmentItem("sql_auto_is_null", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sql_big_selects", "ON", true));
        variableAssignmentList.add(new AssignmentItem("sql_buffer_result", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sql_hints", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("sql_log_off", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sql_mode",
            "\"STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION\"", true));
        variableAssignmentList.add(new AssignmentItem("sql_notes", "ON", true));
        variableAssignmentList.add(new AssignmentItem("sql_priority_filter", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("sql_quote_show_create", "ON", true));
        variableAssignmentList.add(new AssignmentItem("sql_safe_updates", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sql_select_limit", "18446744073709551615", true));
        variableAssignmentList.add(new AssignmentItem("sql_slave_skip_counter", "0", true));
        variableAssignmentList.add(new AssignmentItem("sql_warnings", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("ssl_ca", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_capath", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_cert", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_cipher", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_crl", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_crlpath", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("ssl_key", "\"\"", true));
        variableAssignmentList.add(new AssignmentItem("stored_program_cache", "256", true));
        variableAssignmentList.add(new AssignmentItem("super_read_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("sync_binlog", "1", true));
        variableAssignmentList.add(new AssignmentItem("sync_frm", "ON", true));
        variableAssignmentList.add(new AssignmentItem("sync_master_info", "10000", true));
        variableAssignmentList.add(new AssignmentItem("sync_relay_log", "10000", true));
        variableAssignmentList.add(new AssignmentItem("sync_relay_log_info", "10000", true));
        variableAssignmentList.add(new AssignmentItem("system_time_zone", "UTC", true));
        variableAssignmentList.add(new AssignmentItem("table_definition_cache", "1400", true));
        variableAssignmentList.add(new AssignmentItem("table_open_cache", "2000", true));
        variableAssignmentList.add(new AssignmentItem("table_open_cache_instances", "16", true));
        variableAssignmentList.add(new AssignmentItem("thread_cache_size", "58", true));
        variableAssignmentList.add(new AssignmentItem("thread_handling", "\"one-thread-per-connection\"", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_high_prio_mode", "transactions", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_high_prio_tickets", "4294967295", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_idle_timeout", "60", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_max_threads", "500000", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_oversubscribe", "3", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_size", "6", true));
        variableAssignmentList.add(new AssignmentItem("thread_pool_stall_limit", "10", true));
        variableAssignmentList.add(new AssignmentItem("thread_stack", "524288", true));
        variableAssignmentList.add(new AssignmentItem("thread_stack_warning", "262144", true));
//        variableAssignmentList.add(new AssignmentItem("time_format", "%H:%i:%s", true));
//        variableAssignmentList.add(new AssignmentItem("time_zone", "\"+08:00\"", true));
        variableAssignmentList.add(new AssignmentItem("tls_version", "\"TLSv1,TLSv1.1,TLSv1.2\"", true));
        variableAssignmentList.add(new AssignmentItem("tmp_table_size", "16777216", true));
//        variableAssignmentList.add(new AssignmentItem("tmpdir", "/u01/my3306/tmp", true));
        variableAssignmentList.add(new AssignmentItem("transaction_alloc_block_size", "8192", true));
        variableAssignmentList.add(new AssignmentItem("transaction_isolation", "\"REPEATABLE-READ\"", true));
        variableAssignmentList.add(new AssignmentItem("transaction_prealloc_size", "4096", true));
        variableAssignmentList.add(new AssignmentItem("transaction_read_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("transaction_write_set_extraction", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("tx_isolation", "\"REPEATABLE-READ\"", true));
        variableAssignmentList.add(new AssignmentItem("tx_read_only", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("unique_checks", "ON", true));
        variableAssignmentList.add(new AssignmentItem("updatable_views_with_limit", "YES", true));
        variableAssignmentList.add(new AssignmentItem("use_myfs", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("use_polarfs", "OFF", true));
        variableAssignmentList
            .add(new AssignmentItem("version", "\"5.7.14-AliSQL-X-Cluster-1.6.0.7-20210330-log\"", true));
        variableAssignmentList.add(new AssignmentItem("version_comment", "\"Source distribution\"", true));
        variableAssignmentList.add(new AssignmentItem("version_compile_machine", "x86_64", true));
        variableAssignmentList.add(new AssignmentItem("version_compile_os", "Linux", true));
        variableAssignmentList.add(new AssignmentItem("version_hide_xcluster", "OFF", true));
        variableAssignmentList.add(new AssignmentItem("wait_timeout", "28800", true));
        variableAssignmentList.add(new AssignmentItem("weak_consensus_mode", "OFF", true));
        batchSetGlobalVariableTest(variableAssignmentList, true);
    }

    /**
     * 执行sql
     */
    public static void execute(Connection conn, String sql) throws SQLException {
        Statement statement = createStatement(conn);
        try {
            statement.execute(sql);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static Statement createStatement(Connection conn) throws SQLException {
        Statement statement = null;
        try {
            statement = conn.createStatement();
        } catch (SQLException e) {
            throw e;
        }
        return statement;
    }

    private int getCount() throws SQLException {
        String sql;
        sql = "SHOW VARIABLES LIKE '%auto_increment_offset%'";
        final PreparedStatement ps = JdbcUtil.preparedStatement(sql, tddlConnection);
        final ResultSet rs = ps.executeQuery();
        int count = -1;
        try {
            while (rs.next()) {
                count = rs.getInt(2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return count;
    }

    private String getVariableValue(String variableName, boolean isGlobal) throws SQLException {
        StringBuilder sql = new StringBuilder("SHOW ");
        if (isGlobal) {
            sql.append("GLOBAL ");
        }
        sql.append("VARIABLES LIKE '");
        sql.append(variableName);
        sql.append("'");
        final PreparedStatement ps = JdbcUtil.preparedStatement(sql.toString(), tddlConnection);
        final ResultSet rs = ps.executeQuery();
        String result = null;
        try {
            while (rs.next()) {
                result = rs.getString(2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return result;
    }

    private String getDnVariableValue(String variableName, boolean isGlobal) throws SQLException {
        StringBuilder sql = new StringBuilder("/*+TDDL: node(0)*/SHOW ");
        if (isGlobal) {
            sql.append("GLOBAL ");
        }
        sql.append("VARIABLES LIKE '");
        sql.append(variableName);
        sql.append("'");
        final PreparedStatement ps = JdbcUtil.preparedStatement(sql.toString(), tddlConnection);
        final ResultSet rs = ps.executeQuery();
        String result = null;
        try {
            while (rs.next()) {
                result = rs.getString(2);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        return result;
    }

    private boolean testIfDnVariableCanBeSet(AssignmentItem variableAssignment) {
        StringBuilder sql = new StringBuilder("/*+TDDL: node(0)*/SET ");
        if (variableAssignment.isGlobal) {
            sql.append("GLOBAL ");
        }
        sql.append(variableAssignment.variableName);
        sql.append("=");
        sql.append(variableAssignment.variableValue);

        try {
            execute(tddlConnection, sql.toString());
            return true;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return false;
        }
    }

    private void setVariableValue(String variableName, String variableValue, boolean isGlobal) throws SQLException {
        StringBuilder sql = new StringBuilder("SET ");
        if (isGlobal) {
            sql.append("GLOBAL ");
        }
        sql.append(variableName);
        sql.append("=");
        sql.append(variableValue);
        execute(tddlConnection, sql.toString());
    }

    private void enableSetGlobal() throws SQLException {
        String sql = "SET ENABLE_SET_GLOBAL=true";
        execute(tddlConnection, sql);
    }

    private void batchSetVariableValue(List<AssignmentItem> assignmentList) throws SQLException {
        StringBuilder sql = new StringBuilder("SET ");
        for (AssignmentItem item : assignmentList) {
            if (item.isGlobal) {
                sql.append("GLOBAL ");
            }
            sql.append(item.variableName);
            sql.append("=");
            sql.append(item.variableValue);
            sql.append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        execute(tddlConnection, sql.toString());
    }

    private class AssignmentItem {
        String variableName;
        String variableValue;
        boolean isGlobal;

        AssignmentItem(String variableName, String variableValue, boolean isGlobal) {
            this.variableName = variableName;
            this.variableValue = variableValue;
            this.isGlobal = isGlobal;
        }
    }
}
