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

import java.io.Serializable;

import lombok.Data;

/**
 * 存储到数据库中的纵向轴数据，可以和VisualAxis互相转换
 * @author ximing.yd
 * @date 2021/12/20 上午11:14
 */
@Data
public class VisualAxisModel implements Serializable {

    private static final long serialVersionUID = 4110095652178920772L;

    private Integer layerNum;
    private Long timestamp;
    private String axisJson;

    public VisualAxisModel(Integer layerNum, Long timestamp, String axisJson) {
        this.layerNum = layerNum;
        this.timestamp = timestamp;
        this.axisJson = axisJson;
    }
}
