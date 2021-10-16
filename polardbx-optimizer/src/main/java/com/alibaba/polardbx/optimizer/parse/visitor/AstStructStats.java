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
 * @author chenghui.lch
 */
public class AstStructStats {

    public Integer unionCnt = 0;
    public Integer projCnt = 0;
    public Integer joinCnt = 0;
    public Integer aggCnt = 0;
    public Integer filterCnt = 0;
    public Integer limitCnt = 0;
    public Integer subQueryCnt = 0;
    public Integer sortCnt = 0;
    public Integer tableScanCnt = 0;
    public Integer relNodeCnt = 0;
    public Integer rexNodeCnt = 0;

    public void reset() {
        unionCnt = 0;
        projCnt = 0;
        joinCnt = 0;
        aggCnt = 0;
        filterCnt = 0;
        limitCnt = 0;
        subQueryCnt = 0;
        sortCnt = 0;
        tableScanCnt = 0;
        relNodeCnt = 0;
        rexNodeCnt = 0;
    }
}
