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

package com.alibaba.polardbx.executor.balancer.solver;

import com.sun.jna.Library;
import com.sun.jna.Pointer;

public interface LpSolverLibrary extends Library {

    public Pointer Cbc_newModel();

    public void Cbc_addCol(Pointer model, String name, double lb, double ub, double obj,
                           char isInteger, int nz, Pointer row, Pointer coefs);

    public void Cbc_addRow(Pointer model, String name, int nz, Pointer cols, Pointer coefs, char sense, double rhs);

    public int Cbc_solve(Pointer model);

    public void Cbc_setMaximumSeconds(Pointer model, double maxSeconds);

    public Pointer Cbc_getColSolution(Pointer model);

    public int Cbc_getColNz(Pointer model, int col);

    public void Cbc_setLogLevel(Pointer model, int value);

    public int Cbc_getLogLevel(Pointer model);

    public Boolean Cbc_isProvenOptimal(Pointer model);

    public Boolean Cbc_isProvenInfeasible(Pointer model);

    public void Cbc_setAllowableFractionGap(Pointer model, double allowedFracionGap);

    public int Cbc_status(Pointer model);

    public void Cbc_deleteModel(Pointer model);

    public void Cbc_deleteColBuffer(Pointer model);

    public void Cbc_deleteRowBuffer(Pointer model);

}