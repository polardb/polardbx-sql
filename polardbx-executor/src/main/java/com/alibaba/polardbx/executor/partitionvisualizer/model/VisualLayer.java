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
import java.util.List;

/**
 * @author ximing.yd
 */
@Data
public class VisualLayer implements Serializable {
    private static final long serialVersionUID = 603089901122701689L;

    private Long startTimestamp;
    private Long endTimestamp;
    private List<VisualAxis> ringAxis;
    private List<Long> ringTimestamp;

    private Integer layerNum;
    private Integer head;
    private Integer tail;
    private Boolean empty;
    private Integer length;

    private Integer ratio;

    private VisualLayer next;

}
