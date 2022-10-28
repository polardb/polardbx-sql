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

package com.alibaba.polardbx.optimizer.parse.visitor;

/**
 * ${DESCRIPTION}
 *
 * @author hongxi.chx
 */
public enum ContextParameterKey {
    INDEX_HINT, PARAMS, DO_HAVING, META_DATA, ALIAS_TO_TABLE_REFERENCE_LEVEL1, ORIGIN_PARAMETER_COUNT, SCHEMA, WINDOW,
    HAS_IN_EXPR, BIND_TYPE_PARAMS;
}
