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

package com.alibaba.polardbx.executor.partitionvisualizer.model;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 热力图上的纵轴
 *
 * @author ximing.yd
 */
@Data
public class VisualAxis implements Serializable {

    private static final long serialVersionUID = -5920685035163198622L;

    /**
     * 上确界,格式：schemaName,logicalTable,partitionSeq,partitionName
     */
    private List<String> bounds = new ArrayList<>();

    private Map<String/** type **/, List<Long>> valuesMap = new HashMap<>();

}
