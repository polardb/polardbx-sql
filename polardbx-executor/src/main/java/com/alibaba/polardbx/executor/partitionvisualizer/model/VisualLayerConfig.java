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
 * @author ximing.yd
 * @date 2021/12/20 下午4:03
 */
@Data
public class VisualLayerConfig implements Serializable {

    private static final long serialVersionUID = 8847584303685916953L;
    private Integer length;
    private Integer ratio;

    public VisualLayerConfig(Integer length, Integer ratio) {
        this.length = length;
        this.ratio = ratio;
    }
}
