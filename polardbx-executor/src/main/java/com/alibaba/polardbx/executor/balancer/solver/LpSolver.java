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

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Pointer;
import org.ojalgo.optimisation.Expression;
import org.ojalgo.optimisation.ExpressionsBasedModel;
import org.ojalgo.optimisation.Optimisation;
import org.ojalgo.optimisation.Variable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LpSolver {

    public Logger logger = Logger.getLogger(String.valueOf(getClass()));

    int variableCount;
    Pointer model;

    ExpressionsBasedModel ojModel;

    Map<LpVariable, Variable> variableMap = new HashMap<>();

    public static final int BEFORE_BRANCH_AND_CUT = -1;
    public static final int OPTIMAL = 0;
    public static final int INFEASIBLE = 1;
    public static final int STOP_ON_TIME = 4;

    public static LpSolverLibrary INSTANCE = null;

    public static Boolean loadSolverLibrary;

    static {
        if (Platform.isMac() && Platform.isIntel()) {
            String resourceDir = "darwin-x86-64";
            System.setProperty("jna.library.path",
                LpSolverLibrary.class.getClassLoader().getResource(resourceDir).getPath());
            loadSolverLibrary = false;
        } else if (Platform.isLinux() && Platform.isIntel()) {
            String resourceDir = "linux-x86-64";
            System.setProperty("jna.library.path",
                LpSolverLibrary.class.getClassLoader().getResource(resourceDir).getPath());
            loadSolverLibrary = true;
        } else if (Platform.isMac() && Platform.isARM()) {
            String resourceDir = "darwin-arm";
            System.setProperty("jna.library.path",
                LpSolverLibrary.class.getClassLoader().getResource(resourceDir).getPath());
            loadSolverLibrary = true;
        } else if (Platform.isLinux() && Platform.isARM()) {
            loadSolverLibrary = false;
//            String resourceDir1 = "linux-arm";
//            String resourceDir2 = "linux-aarch64";
//            System.setProperty("jna.library.path",
//                LpSolverLibrary.class.getClassLoader().getResource(resourceDir1).getPath());
//            System.setProperty("jna.library.path",
//                LpSolverLibrary.class.getClassLoader().getResource(resourceDir2).getPath());
        } else {
            loadSolverLibrary = false;
        }
        if (loadSolverLibrary) {
            INSTANCE = (LpSolverLibrary) Native.load((Platform.isWindows() ? "?" : "CbcSolver"), LpSolverLibrary.class);
        }
    }

    static LpSolverLibrary nativeCbc = INSTANCE;

    public LpSolver() {
        if (loadSolverLibrary) {
            model = nativeCbc.Cbc_newModel();
            nativeCbc.Cbc_setLogLevel(model, 4);
            variableCount = 0;
        } else {
            ojModel = new ExpressionsBasedModel();
        }
    }

    public int solve() {
        if (loadSolverLibrary) {
            return nativeCbc.Cbc_solve(model);
        } else {
            ojModel.minimise();
            return 0;
        }
    }

    public void addVariable(LpVariable variable) {
        if (loadSolverLibrary) {
            nativeCbc.Cbc_addCol(model, variable.name, variable.lb, variable.ub, variable.objective, variable.isInteger,
                0,
                Pointer.NULL, Pointer.NULL);
            variable.id = this.variableCount;
            variableCount++;
        } else {
            Variable ojVariable = new Variable(variable.name).weight(variable.objective);
            Boolean isInteger = (variable.isInteger == (char) 1);
            if (variable.lb == 0.0 && variable.ub == 1.0 && isInteger) {
                ojVariable.binary();
            } else {
                ojVariable.lower(variable.lb).upper(variable.ub).integer(isInteger);
            }
            ojModel.addVariable(ojVariable);
            variableMap.put(variable, ojVariable);
        }
    }

    public void addExpression(LpExpression expr) {
        if (loadSolverLibrary) {
            List<Map.Entry<LpVariable, Double>> termList = expr.terms.entrySet().stream().collect(Collectors.toList());
            termList.sort((o1, o2) -> {
                if (o1.getKey().id < o2.getKey().id) {
                    return -1;
                } else if (o1.getKey().id == o2.getKey().id) {
                    return 0;
                } else {
                    return 1;
                }
            });
            int nz = termList.size();
            final Pointer cols = new Memory((long) nz * Native.getNativeSize(Integer.TYPE));
            final Pointer coefs = new Memory((long) nz * Native.getNativeSize(Double.TYPE));
            char sense = expr.sense;
            for (int i = 0; i < nz; i++) {
                int id = termList.get(i).getKey().id;
                double coef = termList.get(i).getValue();
                cols.setInt((long) i * Native.getNativeSize(Integer.TYPE), id);
                coefs.setDouble((long) i * Native.getNativeSize(Double.TYPE), coef);
            }
            nativeCbc.Cbc_addRow(model, expr.name, nz, cols, coefs, sense, expr.rhs);
            List<Integer> colInts = new ArrayList<>();
            List<Double> coefDoubles = new ArrayList<>();
            for (int i = 0; i < nz; i++) {
                colInts.add(cols.getInt((long) i * Native.getNativeSize(Integer.TYPE)));
                coefDoubles.add(coefs.getDouble(i * Native.getNativeSize(Double.TYPE)));
            }
        } else {
            String name = expr.name;
            double rhs = expr.rhs;
            Expression expression = ojModel.addExpression(name);
            if (expr.sense == expr.EQ_SENSE) {
                expression.upper(rhs);
                expression.lower(rhs);
            } else if (expr.sense == expr.LQ_SENSE) {
                expression.upper(rhs);
            } else if (expr.sense == expr.GQ_SENSE) {
                expression.lower(rhs);
            }
            for (LpVariable lpVariable : expr.terms.keySet()) {
                double value = expr.terms.get(lpVariable);
                Variable variable = variableMap.get(lpVariable);
                expression.set(variable, value);
            }
        }
    }

    public void setMaximumTime(double maxSeconds) {
        if (loadSolverLibrary) {
            nativeCbc.Cbc_setMaximumSeconds(model, maxSeconds);
        }
    }

    public void setGapPercent(double gapPercent) {
        if (loadSolverLibrary) {
            nativeCbc.Cbc_setAllowableFractionGap(model, gapPercent / 100.0);
        }
    }

    public List<Double> getSolution() {
        List<Double> solutionList = new ArrayList<>();
        if (loadSolverLibrary) {
            Pointer pointer = nativeCbc.Cbc_getColSolution(model);
            for (int i = 0; i < variableCount; i++) {
                double value = pointer.getDouble(i * Native.getNativeSize(Double.TYPE));
                solutionList.add(value);
            }
        } else {
            Optimisation.Result result = ojModel.getVariableValues();
            int resultSize = result.size();
            for (int i = 0; i < resultSize; i++) {
                solutionList.add(result.get(i).doubleValue());
            }
        }
        return solutionList;
    }

    public int getStatus() {
        int finalStatus = 0;
        if (loadSolverLibrary) {
            int status = nativeCbc.Cbc_status(model);
            switch (status) {
            case -1:
                finalStatus = BEFORE_BRANCH_AND_CUT;
                break;
            case 0: {
                if (nativeCbc.Cbc_isProvenInfeasible(model)) {
                    finalStatus = INFEASIBLE;
                } else {
                    finalStatus = OPTIMAL;
                }
                break;
            }
            case 2:
                finalStatus = INFEASIBLE;
                break;
            default:
                finalStatus = STOP_ON_TIME;
                break;
            }
        } else {
            finalStatus = OPTIMAL;
        }
        return finalStatus;
    }

    public void close() {
        if (loadSolverLibrary) {
            nativeCbc.Cbc_deleteModel(model);
        }
//        nativeCbc.Cbc_deleteRowBuffer(model);
//        nativeCbc.Cbc_deleteColBuffer(model);
    }

}
