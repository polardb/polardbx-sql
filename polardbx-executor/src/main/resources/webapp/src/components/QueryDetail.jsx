/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import React from "react";
import Reactable from "reactable";

import {
    addToHistory,
    computeRate,
    formatCount,
    formatDataSize,
    formatDataSizeBytes,
    formatDurationMs, formatDurationNs,
    formatShortDateTime,
    getFirstParameter, getFullSplitIdSuffix,
    getHostAndPort,
    getHostname,
    getStageNumber, getStagePipelineFromDriverId,
    getStageStateColor,
    getTaskIdSuffix,
    getTaskNumber,
    GLYPHICON_HIGHLIGHT,
    parseDataSize,
    parseDuration,
    precisionRound
} from "../utils";
import {QueryHeader} from "./QueryHeader";

const Table = Reactable.Table,
    Thead = Reactable.Thead,
    Th = Reactable.Th,
    Tr = Reactable.Tr,
    Td = Reactable.Td;

class TaskList extends React.Component {
    static removeQueryId(id) {
        const pos = id.indexOf('.');
        if (pos !== -1) {
            return id.substring(pos + 1);
        }
        return id;
    }

    static compareTaskId(taskA, taskB) {
        const taskIdArrA = TaskList.removeQueryId(taskA).split(".");
        const taskIdArrB = TaskList.removeQueryId(taskB).split(".");

        if (taskIdArrA.length > taskIdArrB.length) {
            return 1;
        }
        for (let i = 0; i < taskIdArrA.length; i++) {
            const anum = Number.parseInt(taskIdArrA[i]);
            const bnum = Number.parseInt(taskIdArrB[i]);
            if (anum !== bnum) {
                return anum > bnum ? 1 : -1;
            }
        }

        return 0;
    }

    static formatState(state, fullyBlocked) {
        if (fullyBlocked && state === "RUNNING") {
            return "BLOCKED";
        }
        else {
            return state;
        }
    }

    openStageDetail(event, id: number) {
        const queryId = this.props.queryId;
        this.setState({ selectedStageId: id });
        const url : string = "/ui/stage.html?" + queryId + "." + id;
        event.preventDefault();
        window.open(url, '_blank');
    }

    render() {
        const tasks = this.props.tasks;

        if (tasks === undefined || tasks.length === 0) {
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>No threads in the selected group</h4></div>
                </div>);
        }

        const renderTasks = (taskList) => taskList.map(task => {
            if (typeof(task.detailedStats) === "undefined") {
                return (
                    <Tr key={task.taskStatus.taskId}>
                        <Td column="id" value={task.taskStatus.taskId}>
                            {getTaskIdSuffix(task.taskStatus.taskId)}
                        </Td>
                        <Td column="host" value={getHostname(task.taskStatus.self)}>
                            <a href={"worker.html?" + task.taskStatus.nodeId} target="_blank">
                                {getHostAndPort(task.taskStatus.self)}
                            </a>
                        </Td>
                        <Td column="state">
                            {task.taskStatus.state}
                        </Td>
                        <Td column="outputRows">
                            {0}
                        </Td>
                        <Td column="inputRows">
                            {0}
                        </Td>
                        <Td column="inputBytes">
                            {0}
                        </Td>
                        <Td column="intputBytesSec">
                            {0}
                        </Td>
                        <Td column="splitsPending">
                            {0}
                        </Td>
                        <Td column="splitsRunning">
                            {0}
                        </Td>
                        <Td column="splitsDone">
                            {task.completedPipelineExecs}
                        </Td>
                        <Td column="elapsedTime">
                            {formatDurationMs(task.elapsedTimeMillis)}
                        </Td>
                        <Td column="deliveryTime">
                            {formatDurationMs(task.deliveryTimeMillis)}
                        </Td>
                        <Td column="processTime">
                            {formatDurationMs(task.processTimeMillis)}
                        </Td>
                        <Td column="dataFinishTime">
                            {formatDurationMs(task.pullDataTimeMillis)}
                        </Td>
                    </Tr>
                );
            } else {
                return (
                    <Tr key={task.taskStatus.taskId}>
                        <Td column="id" value={task.taskStatus.taskId}>
                            {getTaskIdSuffix(task.taskStatus.taskId)}
                        </Td>
                        <Td column="host" value={getHostname(task.taskStatus.self)}>
                            <a href={"worker.html?" + task.taskStatus.nodeId} target="_blank">
                                {getHostAndPort(task.taskStatus.self)}
                            </a>
                        </Td>
                        <Td column="state">
                            {task.taskStatus.state}
                        </Td>
                        <Td column="outputRows">
                            {formatCount(task.detailedStats.outputPositions)}
                        </Td>
                        <Td column="inputRows">
                            {formatCount(task.detailedStats.processedInputPositions)}
                        </Td>
                        {/*<Td column="inputBytes">*/}
                        {/*    {formatDataSizeBytes(task.stats.processedInputDataSize)}*/}
                        {/*</Td>*/}
                        <Td column="splitsPending">
                            {task.detailedStats.queuedPipelineExecs}
                        </Td>
                        <Td column="splitsRunning">
                            {task.detailedStats.runningPipelineExecs}
                        </Td>
                        <Td column="splitsDone">
                            {task.completedPipelineExecs}
                        </Td>
                        <Td column="elapsedTime">
                            {formatDurationMs(task.elapsedTimeMillis)}
                        </Td>
                        <Td column="deliveryTime">
                            {formatDurationMs(task.deliveryTimeMillis)}
                        </Td>
                        <Td column="processTime">
                            {formatDurationMs(task.processTimeMillis)}
                        </Td>
                        <Td column="dataFinishTime">
                            {formatDurationMs(task.pullDataTimeMillis)}
                        </Td>
                    </Tr>
                );
            }
        });

        // separate each stage
        let stageTaskMap = new Map();
        tasks.forEach(task => {
            let taskId = getTaskIdSuffix(task.taskStatus.taskId);
            // find stageId, like: 0.0 and 0.1 => 0
            let stageId = taskId.substring(0, taskId.indexOf('.'));

            if (!stageTaskMap.has(stageId)) {
                stageTaskMap.set(stageId, []);
            }

            stageTaskMap.get(stageId).push(task);
        });

        const stageKeys = Array.from(stageTaskMap.keys());
        stageKeys.sort((a, b) => {
            const numA = Number(a);
            const numB = Number(b);
            return numA - numB;
        });
        // console.debug("stages: " + stageKeys);

        var renderedStages = [];
        for (const stageId of stageKeys) {
            const taskList = stageTaskMap.get(stageId);
            if (!taskList) {
                continue;
            }
            const tableId = "stage-" + stageId;
            // console.debug("Rendering table:" + tableId);
            renderedStages.push(
                <div key={stageId}>
                    <h4>
                        <a href="javascript:void(0);" onClick={(event) => this.openStageDetail(event, Number(stageId))}>{tableId}</a>
                    </h4>
                    <Table id={tableId} className="table table-striped sortable" sortable=
                        {[
                            {
                                column: 'id',
                                sortFunction: TaskList.compareTaskId
                            },
                            'host',
                            'state',
                            'splitsPending',
                            'splitsRunning',
                            'splitsDone',
                            'outputRows',
                            'inputRows',
                            // 'inputBytes',
                            'elapsedTime',
                            'deliveryTime',
                            'processTime',
                            'dataFinishTime',
                        ]}
                           defaultSort={{column: 'id', direction: 'asc'}}>
                        <Thead>
                            <Th column="id">ID</Th>
                            <Th column="host">Host</Th>
                            <Th column="state">State</Th>
                            <Th column="splitsPending"><span key={"Pending"} className="glyphicon glyphicon-pause" style={GLYPHICON_HIGHLIGHT}
                                                             data-toggle="tooltip" data-placement="top"
                                                             title="Pending splits"></span></Th>
                            <Th column="splitsRunning"><span key={"Running"} className="glyphicon glyphicon-play" style={GLYPHICON_HIGHLIGHT}
                                                             data-toggle="tooltip" data-placement="top"
                                                             title="Running splits"></span></Th>
                            <Th column="splitsDone"><span key={"Completed"} className="glyphicon glyphicon-ok" style={GLYPHICON_HIGHLIGHT}
                                                          data-toggle="tooltip" data-placement="top"
                                                          title="Completed splits"></span></Th>
                            <Th column="outputRows">OutputRows</Th>
                            <Th column="inputRows">InputRows</Th>
                            {/*<Th column="inputBytes">inputBytes</Th>*/}
                            <Th column="elapsedTime">Elapsed</Th>
                            <Th column="deliveryTime">Delivery</Th>
                            <Th column="processTime">Process</Th>
                            <Th column="dataFinishTime">DataFinish</Th>
                        </Thead>
                        {renderTasks(taskList)}
                    </Table>
                </div>);
        }

        return renderedStages;
    }
}

class SplitList extends React.Component {
    static removeQueryId(id) {
        const pos = id.indexOf('.');
        if (pos !== -1) {
            return id.substring(pos + 1);
        }
        return id;
    }

    static compareTaskId(taskA, taskB) {
        const taskIdArrA = TaskList.removeQueryId(taskA).split(".");
        const taskIdArrB = TaskList.removeQueryId(taskB).split(".");

        if (taskIdArrA.length > taskIdArrB.length) {
            return 1;
        }
        for (let i = 0; i < taskIdArrA.length; i++) {
            const anum = Number.parseInt(taskIdArrA[i]);
            const bnum = Number.parseInt(taskIdArrB[i]);
            if (anum !== bnum) {
                return anum > bnum ? 1 : -1;
            }
        }

        return 0;
    }

    static formatState(state, fullyBlocked) {
        if (fullyBlocked && state === "RUNNING") {
            return "BLOCKED";
        }
        else {
            return state;
        }
    }

    openStageDetail(event, id: number) {
        const queryId = this.props.queryId;
        this.setState({ selectedStageId: id });
        const url : string = "/ui/stage.html?" + queryId + "." + id;
        event.preventDefault();
        window.open(url, '_blank');
    }

    render() {
        const splits = this.props.splits;

        if (splits === undefined || splits.length === 0) {
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>No splits in the selected group</h4></div>
                </div>);
        }

        const renderSplits = (splitList) => splitList.map(split => {
            return (
                <Tr key={split.driverId}>
                    <Td column="id" value={split.driverId}>
                        {getFullSplitIdSuffix(split.driverId)}
                    </Td>
                    {/*<Td column="state">*/}
                    {/*    {split.state}*/}
                    {/*</Td>*/}
                    <Td column="outputRows">
                        {formatCount(split.outputPositions)}
                    </Td>
                    <Td column="inputRows">
                        {formatCount(split.inputPositions)}
                    </Td>
                    <Td column="elapsedTime">
                        {formatDurationMs(split.endMillis - split.startMillis)}
                    </Td>
                    <Td column="blockTime">
                        {formatDurationNs(split.blockedNanos)}
                    </Td>
                    <Td column="processTime">
                        {formatDurationNs(split.processNanos)}
                    </Td>
                </Tr>
            );
        });

        // separate each pipeline
        let pipelineSplitMap = new Map();
        splits.forEach(split => {
            let driverId = getFullSplitIdSuffix(split.driverId)
            let pipelineId = getStagePipelineFromDriverId(driverId);

            if (!pipelineSplitMap.has(pipelineId)) {
                pipelineSplitMap.set(pipelineId, []);
            }

            pipelineSplitMap.get(pipelineId).push(split);
        });

        const pipelineKeys = Array.from(pipelineSplitMap.keys());
        pipelineKeys.sort((a, b) => {
            const parts1 = a.split('.');
            const parts2 = b.split('.');
            const stageId1 = Number(parts1[0]);
            const pId1 = Number(parts1[1]);
            const stageId2 = Number(parts2[0]);
            const pId2 = Number(parts2[1]);
            if (stageId1 !== stageId2) {
                return stageId1 - stageId2;
            }

            return pId1 - pId2;
        });
        // console.debug("pipelines: " + pipelineKeys);

        var renderedSplits = [];
        for (const pipelineId of pipelineKeys) {
            const splitList = pipelineSplitMap.get(pipelineId);
            const parts = pipelineId.split('.');
            const stageId = parts[0];
            const pId = parts[1];
            const tableId = "stage-" + stageId + "-pipeline-" + pId;
            renderedSplits.push(
                <div key={pipelineId}>
                    <h4>
                        <a href="javascript:void(0);" onClick={(event) => this.openStageDetail(event, Number(stageId))}>{tableId}</a>
                    </h4>
                    <Table id={tableId} className="table table-striped sortable" sortable=
                        {[
                            {
                                column: 'id',
                                sortFunction: TaskList.compareTaskId
                            },
                            'host',
                            'state',
                            'outputRows',
                            'inputRows',
                            'elapsedTime',
                            'blockTime',
                            'processTime',
                        ]}
                           defaultSort={{column: 'id', direction: 'asc'}}>
                        <Thead>
                            <Th column="id">ID</Th>
                            {/*<Th column="state">State</Th>*/}
                            <Th column="outputRows">OutputRows</Th>
                            <Th column="inputRows">InputRows</Th>
                            {/*<Th column="inputBytes">inputBytes</Th>*/}
                            <Th column="elapsedTime">Elapsed</Th>
                            <Th column="blockTime">Blocked</Th>
                            <Th column="processTime">Process</Th>
                        </Thead>
                        {renderSplits(splitList)}
                    </Table>
                </div>
            );
        }

        return renderedSplits;
    }
}

const BAR_CHART_WIDTH = 800;

const BAR_CHART_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#8997B3',
    chartRangeMin: 0,
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: 'Task {{offset:offset}} - {{value}}',
    disableHiddenCheck: true,
};

const HISTOGRAM_WIDTH = 175;

const HISTOGRAM_PROPERTIES = {
    type: 'bar',
    barSpacing: '0',
    height: '80px',
    barColor: '#747F96',
    zeroColor: '#747F96',
    zeroAxis: true,
    chartRangeMin: 0,
    tooltipClassname: 'sparkline-tooltip',
    tooltipFormat: '{{offset:offset}} -- {{value}} tasks',
    disableHiddenCheck: true,
};

class StageSummary extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            expanded: false,
            lastRender: null
        };
    }

    getExpandedIcon() {
        return this.state.expanded ? "glyphicon-chevron-up" : "glyphicon-chevron-down";
    }

    getExpandedStyle() {
        return this.state.expanded ? {} : {display: "none"};
    }

    toggleExpanded() {
        this.setState({
            expanded: !this.state.expanded,
        })
    }

    static renderHistogram(histogramId, inputData, numberFormatter) {
        const numBuckets = Math.min(HISTOGRAM_WIDTH, Math.sqrt(inputData.length));
        const dataMin = Math.min.apply(null, inputData);
        const dataMax = Math.max.apply(null, inputData);
        const bucketSize = (dataMax - dataMin) / numBuckets;

        let histogramData = [];
        if (bucketSize === 0) {
            histogramData = [inputData.length];
        }
        else {
            for (let i = 0; i < numBuckets + 1; i++) {
                histogramData.push(0);
            }

            for (let i in inputData) {
                const dataPoint = inputData[i];
                const bucket = Math.floor((dataPoint - dataMin) / bucketSize);
                histogramData[bucket] = histogramData[bucket] + 1;
            }
        }

        const tooltipValueLookups = {'offset': {}};
        for (let i = 0; i < histogramData.length; i++) {
            tooltipValueLookups['offset'][i] = numberFormatter(dataMin + (i * bucketSize)) + "-" + numberFormatter(dataMin + ((i + 1) * bucketSize));
        }

        const stageHistogramProperties = $.extend({}, HISTOGRAM_PROPERTIES, {
            barWidth: (HISTOGRAM_WIDTH / histogramData.length),
            tooltipValueLookups: tooltipValueLookups
        });
        $(histogramId).sparkline(histogramData, stageHistogramProperties);
    }

    componentDidUpdate() {
        const stage = this.props.stage;
        const numTasks = stage.taskStats.length;

        // sort the x-axis
        stage.taskStats.sort((taskA, taskB) => getTaskNumber(taskA.taskStatus.taskId) - getTaskNumber(taskB.taskStatus.taskId));

        const scheduledTimes = stage.taskStats.map(task => {
            if (typeof(task.stats) === "undefined") {
                parseDuration(0);
            } else {
                parseDuration(task.stats.totalScheduledTime);
            }
        });
        const cpuTimes = stage.taskStats.map(task => {
                if (typeof(task.stats) === "undefined") {
                    parseDuration(0);
                } else {
                    parseDuration(task.stats.totalCpuTime);
                }
            }
        );

        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender === null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            const stageId = getStageNumber(stage.stageId);

            StageSummary.renderHistogram('#scheduled-time-histogram-' + stageId, scheduledTimes, formatDurationMs);
            StageSummary.renderHistogram('#cpu-time-histogram-' + stageId, cpuTimes, formatDurationMs);

            if (this.state.expanded) {
                // this needs to be a string otherwise it will also be passed to numberFormatter
                const tooltipValueLookups = {'offset': {}};
                for (let i = 0; i < numTasks; i++) {
                    tooltipValueLookups['offset'][i] = getStageNumber(stage.stageId) + "." + i;
                }

                const stageBarChartProperties = $.extend({}, BAR_CHART_PROPERTIES, {
                    barWidth: BAR_CHART_WIDTH / numTasks,
                    tooltipValueLookups: tooltipValueLookups
                });

                $('#scheduled-time-bar-chart-' + stageId).sparkline(scheduledTimes, $.extend({}, stageBarChartProperties, {numberFormatter: formatDurationMs}));
                $('#cpu-time-bar-chart-' + stageId).sparkline(cpuTimes, $.extend({}, stageBarChartProperties, {numberFormatter: formatDurationMs}));
            }

            this.setState({
                lastRender: renderTimestamp
            });
        }
    }

    render() {
        const stage = this.props.stage;
        if (stage === undefined) {
            return (
                <tr>
                    <td>Information about this stage is unavailable.</td>
                </tr>);
        }
        //
        // const totalBufferedBytes = stage.taskStats
        //     .map(task => task.outputBuffers.totalBufferedBytes)
        //     .reduce((a, b) => a + b, 0);

        const stageId = getStageNumber(stage.stageId);

        return (
            <tr>
                <td className="stage-id">
                    <div className="stage-state-color"
                         style={{borderLeftColor: getStageStateColor(stage)}}>{stageId}</div>
                </td>
                <td>
                    <table className="table single-stage-table">
                        <tbody>
                        <tr>
                            <td>
                                <table className="stage-table stage-table-time">
                                    <thead>
                                    <tr>
                                        <th className="stage-table-stat-title stage-table-stat-header">
                                            Time
                                        </th>
                                        <th/>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Scheduled
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {formatDurationNs(stage.stageStats.totalScheduledTimeNanos)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Blocked
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {formatDurationNs(stage.stageStats.totalBlockedTimeNanos)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Wall
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {formatDurationNs(stage.stageStats.totalUserTimeNanos)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            CPU
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {formatDurationNs(stage.stageStats.totalCpuTimeNanos)}
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table stage-table-memory">
                                    <thead>
                                    <tr>
                                        <th className="stage-table-stat-title stage-table-stat-header">
                                            Memory
                                        </th>
                                        <th/>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Cumulative
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {formatDataSizeBytes(stage.stageStats.cumulativeMemory / 1000)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Current
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.stageStats.totalMemoryReservation}
                                        </td>
                                    </tr>
                                    {/*<tr>*/}
                                    {/*    <td className="stage-table-stat-title">*/}
                                    {/*        Buffers*/}
                                    {/*    </td>*/}
                                    {/*    <td className="stage-table-stat-text">*/}
                                    {/*        {formatDataSize(totalBufferedBytes)}*/}
                                    {/*    </td>*/}
                                    {/*</tr>*/}
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Peak
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.stageStats.peakMemoryReservation}
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table stage-table-tasks">
                                    <thead>
                                    <tr>
                                        <th className="stage-table-stat-title stage-table-stat-header">
                                            Tasks
                                        </th>
                                        <th/>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Pending
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.taskStats.filter(task => task.taskStatus.state === "PLANNED").length}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Running
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.taskStats.filter(task => task.taskStatus.state === "RUNNING").length}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Finished
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.taskStats.filter(function (task) {
                                                return task.taskStatus.state == "FINISHED" ||
                                                    task.taskStatus.state == "CANCELED" ||
                                                    task.taskStatus.state == "ABORTED" ||
                                                    task.taskStatus.state == "FAILED"
                                            }).length}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="stage-table-stat-title">
                                            Total
                                        </td>
                                        <td className="stage-table-stat-text">
                                            {stage.taskStats.length}
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table histogram-table">
                                    <thead>
                                    <tr>
                                        <th className="stage-table-stat-title stage-table-chart-header">
                                            Scheduled Time Skew
                                        </th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr>
                                        <td className="histogram-container">
                                            <span className="histogram" id={"scheduled-time-histogram-" + stageId}><div
                                                className="loader"/></span>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td>
                                <table className="stage-table histogram-table">
                                    <thead>
                                    <tr>
                                        <th className="stage-table-stat-title stage-table-chart-header">
                                            CPU Time Skew
                                        </th>
                                    </tr>
                                    </thead>
                                    <tbody>
                                    <tr>
                                        <td className="histogram-container">
                                            <span className="histogram" id={"cpu-time-histogram-" + stageId}><div
                                                className="loader"/></span>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                            <td className="expand-charts-container">
                                <a onClick={this.toggleExpanded.bind(this)} className="expand-charts-button">
                                    <span className={"glyphicon " + this.getExpandedIcon()} style={GLYPHICON_HIGHLIGHT}
                                          data-toggle="tooltip" data-placement="top" title="More"/>
                                </a>
                            </td>
                        </tr>
                        <tr style={this.getExpandedStyle()}>
                            <td colSpan="6">
                                <table className="expanded-chart">
                                    <tbody>
                                    <tr>
                                        <td className="stage-table-stat-title expanded-chart-title">
                                            Task Scheduled Time
                                        </td>
                                        <td className="bar-chart-container">
                                            <span className="bar-chart" id={"scheduled-time-bar-chart-" + stageId}><div
                                                className="loader"/></span>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        <tr style={this.getExpandedStyle()}>
                            <td colSpan="6">
                                <table className="expanded-chart">
                                    <tbody>
                                    <tr>
                                        <td className="stage-table-stat-title expanded-chart-title">
                                            Task CPU Time
                                        </td>
                                        <td className="bar-chart-container">
                                            <span className="bar-chart" id={"cpu-time-bar-chart-" + stageId}><div
                                                className="loader"/></span>
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </td>
                        </tr>
                        </tbody>
                    </table>
                </td>
            </tr>);
    }
}

class StageList extends React.Component {
    getStages(stage) {
        if (stage === undefined || !stage.hasOwnProperty('subStages')) {
            return []
        }

        return [].concat.apply(stage, stage.subStages.map(this.getStages, this));
    }

    render() {
        const stages = this.getStages(this.props.outputStage);

        if (stages === undefined || stages.length === 0) {
            return (
                <div className="row">
                    <div className="col-xs-12">
                        No stage information available.
                    </div>
                </div>
            );
        }

        const renderedStages = stages.map(stage => <StageSummary key={stage.stageId} stage={stage}/>);

        return (
            <div className="row">
                <div className="col-xs-12">
                    <table className="table" id="stage-list">
                        <tbody>
                        {renderedStages}
                        </tbody>
                    </table>
                </div>
            </div>
        );
    }
}

const SMALL_SPARKLINE_PROPERTIES = {
    width: '100%',
    height: '57px',
    fillColor: '#3F4552',
    lineColor: '#747F96',
    spotColor: '#1EDCFF',
    tooltipClassname: 'sparkline-tooltip',
    disableHiddenCheck: true,
};

const TASK_FILTER = {
    NONE: {
        text: "None",
        predicate: function () {
            return false
        }
    },
    ALL: {
        text: "All",
        predicate: function () {
            return true
        }
    },
    PLANNED: {
        text: "Planned",
        predicate: function (state) {
            return state === 'PLANNED'
        }
    },
    RUNNING: {
        text: "Running",
        predicate: function (state) {
            return state === 'RUNNING'
        }
    },
    FINISHED: {
        text: "Finished",
        predicate: function (state) {
            return state === 'FINISHED'
        }
    },
    FAILED: {
        text: "Aborted/Canceled/Failed",
        predicate: function (state) {
            return state === 'FAILED' || state === 'ABORTED' || state === 'CANCELED'
        }
    },
};

export class QueryDetail extends React.Component {

    constructor(props) {
        super(props);
        this.state = {
            query: null,
            lastSnapshotStages: null,
            lastSnapshotTasks: null,

            lastScheduledTime: 0,
            lastCpuTime: 0,
            lastRowInput: 0,
            lastByteInput: 0,

            scheduledTimeRate: [],
            cpuTimeRate: [],
            rowInputRate: [],
            byteInputRate: [],

            reservedMemory: [],

            initialized: false,
            ended: false,

            lastRefresh: null,
            lastRender: null,

            stageRefresh: true,
            taskRefresh: true,
            splitRefresh: true,

            taskFilter: TASK_FILTER.NONE,
            splitFilter: TASK_FILTER.NONE,
        };

        this.refreshLoop = this.refreshLoop.bind(this);
    }

    static formatStackTrace(info) {
        return QueryDetail.formatStackTraceHelper(info, [], "", "");
    }

    static formatErrorCode(errorCode) {
        if (typeof(errorCode) === "undefined") {
            return ""
        } else {
            return errorCode.name + " (" + errorCode.code + ")"
        }
    }

    static formatStackTraceHelper(info, parentStack, prefix, linePrefix) {
        let s = linePrefix + prefix + QueryDetail.failureInfoToString(info) + "\n";

        if (info.stack) {
            let sharedStackFrames = 0;
            if (parentStack !== null) {
                sharedStackFrames = QueryDetail.countSharedStackFrames(info.stack, parentStack);
            }

            for (let i = 0; i < info.stack.length - sharedStackFrames; i++) {
                s += linePrefix + "\tat " + info.stack[i] + "\n";
            }
            if (sharedStackFrames !== 0) {
                s += linePrefix + "\t... " + sharedStackFrames + " more" + "\n";
            }
        }

        if (info.suppressed) {
            for (let i = 0; i < info.suppressed.length; i++) {
                s += QueryDetail.formatStackTraceHelper(info.suppressed[i], info.stack, "Suppressed: ", linePrefix + "\t");
            }
        }

        if (info.cause) {
            s += QueryDetail.formatStackTraceHelper(info.cause, info.stack, "Caused by: ", linePrefix);
        }

        return s;
    }

    static countSharedStackFrames(stack, parentStack) {
        let n = 0;
        const minStackLength = Math.min(stack.length, parentStack.length);
        while (n < minStackLength && stack[stack.length - 1 - n] === parentStack[parentStack.length - 1 - n]) {
            n++;
        }
        return n;
    }

    static failureInfoToString(t) {
        return (t.message !== null) ? (t.type + ": " + t.message) : t.type;
    }

    resetTimer() {
        clearTimeout(this.timeoutId);
        // stop refreshing when query finishes or fails
        if (this.state.query === null || !this.state.ended) {
            // task.info-update-interval is set to 3 seconds by default
            this.timeoutId = setTimeout(this.refreshLoop, 5000);
        }
    }

    refreshLoop() {
        clearTimeout(this.timeoutId); // to stop multiple series of refreshLoop from going on simultaneously
        const queryId = getFirstParameter(window.location.search);
        $.get('/v1/query/stats/' + queryId, function (query) {
            let lastSnapshotStages = this.state.lastSnapshotStage;
            if (this.state.stageRefresh) {
                lastSnapshotStages = query.outputStage;
            }
            let lastSnapshotTasks = this.state.lastSnapshotTasks;
            if (this.state.taskRefresh) {
                lastSnapshotTasks = query.outputStage;
            }

            let lastRefresh = this.state.lastRefresh;
            const lastScheduledTime = this.state.lastScheduledTime;
            const lastCpuTime = this.state.lastCpuTime;
            const lastRowInput = this.state.lastRowInput;
            const lastByteInput = this.state.lastByteInput;
            const alreadyEnded = this.state.ended;
            const nowMillis = Date.now();

            this.setState({
                query: query,
                lastSnapshotStage: lastSnapshotStages,
                lastSnapshotTasks: lastSnapshotTasks,

                lastScheduledTime: parseDuration(query.queryStats.totalScheduledTime),
                lastCpuTime: parseDuration(query.queryStats.totalCpuTime),
                lastRowInput: query.queryStats.processedInputPositions,
                lastByteInput: parseDataSize(query.queryStats.processedInputDataSize),

                initialized: true,
                ended: query.finalQueryInfo,

                lastRefresh: nowMillis,
            });

            // i.e. don't show sparklines if we've already decided not to update or if we don't have one previous measurement
            if (alreadyEnded || (lastRefresh === null && query.state === "RUNNING")) {
                this.resetTimer();
                return;
            }

            if (lastRefresh === null) {
                lastRefresh = nowMillis - parseDuration(query.queryStats.elapsedTime);
            }

            const elapsedSecsSinceLastRefresh = (nowMillis - lastRefresh) / 1000.0;
            if (elapsedSecsSinceLastRefresh >= 0) {
                const currentScheduledTimeRate = (parseDuration(query.queryStats.totalScheduledTime) - lastScheduledTime) / (elapsedSecsSinceLastRefresh * 1000);
                const currentCpuTimeRate = (parseDuration(query.queryStats.totalCpuTime) - lastCpuTime) / (elapsedSecsSinceLastRefresh * 1000);
                const currentRowInputRate = (query.queryStats.processedInputPositions - lastRowInput) / elapsedSecsSinceLastRefresh;
                const currentByteInputRate = (parseDataSize(query.queryStats.processedInputDataSize) - lastByteInput) / elapsedSecsSinceLastRefresh;
                this.setState({
                    scheduledTimeRate: addToHistory(currentScheduledTimeRate, this.state.scheduledTimeRate),
                    cpuTimeRate: addToHistory(currentCpuTimeRate, this.state.cpuTimeRate),
                    rowInputRate: addToHistory(currentRowInputRate, this.state.rowInputRate),
                    byteInputRate: addToHistory(currentByteInputRate, this.state.byteInputRate),
                    reservedMemory: addToHistory(parseDataSize(query.queryStats.totalMemoryReservation), this.state.reservedMemory),
                });
            }
            this.resetTimer();
        }.bind(this))
            .error(() => {
                this.setState({
                    initialized: true,
                });
                this.resetTimer();
            });
    }

    handleTaskRefreshClick() {
        if (this.state.taskRefresh) {
            this.setState({
                taskRefresh: false,
                lastSnapshotTasks: this.state.query.outputStage,
            });
        }
        else {
            this.setState({
                taskRefresh: true,
            });
        }
    }

    handleSplitRefreshClick() {
        if (this.state.splitRefresh) {
            this.setState({
                splitRefresh: false,
                // lastSnapshotTasks: this.state.query.outputStage,
            });
        }
        else {
            this.setState({
                splitRefresh: true,
            });
        }
    }

    renderTaskRefreshButton() {
        if (this.state.taskRefresh) {
            return <button className="btn btn-info live-button"
                           onClick={this.handleTaskRefreshClick.bind(this)}>Auto-Refresh: On</button>
        }
        else {
            return <button className="btn btn-info live-button"
                           onClick={this.handleTaskRefreshClick.bind(this)}>Auto-Refresh: Off</button>
        }
    }

    renderSplitRefreshButton() {
        if (this.state.splitRefresh) {
            return <button className="btn btn-info live-button"
                           onClick={this.handleSplitRefreshClick.bind(this)}>Auto-Refresh: On</button>
        }
        else {
            return <button className="btn btn-info live-button"
                           onClick={this.handleSplitRefreshClick.bind(this)}>Auto-Refresh: Off</button>
        }
    }

    handleStageRefreshClick() {
        if (this.state.stageRefresh) {
            this.setState({
                stageRefresh: false,
                lastSnapshotStages: this.state.query.outputStage,
            });
        }
        else {
            this.setState({
                stageRefresh: true,
            });
        }
    }

    renderStageRefreshButton() {
        if (this.state.stageRefresh) {
            return <button className="btn btn-info live-button"
                           onClick={this.handleStageRefreshClick.bind(this)}>Auto-Refresh: On</button>
        }
        else {
            return <button className="btn btn-info live-button"
                           onClick={this.handleStageRefreshClick.bind(this)}>Auto-Refresh: Off</button>
        }
    }

    renderTaskFilterListItem(taskFilter) {
        return (
            <li><a href="#" className={this.state.taskFilter === taskFilter ? "selected" : ""}
                   onClick={this.handleTaskFilterClick.bind(this, taskFilter)}>{taskFilter.text}</a></li>
        );
    }

    handleTaskFilterClick(filter, event) {
        this.setState({
            taskFilter: filter
        });
        event.preventDefault();
    }

    renderSplitFilterListItem(splitFilter) {
        return (
            <li><a href="#" className={this.state.splitFilter === splitFilter ? "selected" : ""}
                   onClick={this.handleSplitFilterClick.bind(this, splitFilter)}>{splitFilter.text}</a></li>
        );
    }

    handleSplitFilterClick(filter, event) {
        this.setState({
            splitFilter: filter
        });
        event.preventDefault();
    }

    getTasksFromStage(stage) {
        if (stage === undefined || !stage.hasOwnProperty('subStages') || !stage.hasOwnProperty('taskStats')) {
            return []
        }

        return [].concat.apply(stage.taskStats, stage.subStages.map(this.getTasksFromStage, this));
    }

    getSplitsFromStage(stage) {
        // console.debug("getting splits from stage" + stage);
        let tasks = this.getTasksFromStage(stage);
        let splits = []
        for (let i = 0; i < tasks.length; i++) {
            splits = splits.concat(tasks[i].detailedStats.driverStats)
        }

        return splits;
    }

    componentDidMount() {
        this.refreshLoop();
    }

    componentDidUpdate() {
        // prevent multiple calls to componentDidUpdate (resulting from calls to setState or otherwise) within the refresh interval from re-rendering sparklines/charts
        if (this.state.lastRender === null || (Date.now() - this.state.lastRender) >= 1000) {
            const renderTimestamp = Date.now();
            $('#scheduled-time-rate-sparkline').sparkline(this.state.scheduledTimeRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {
                chartRangeMin: 0,
                numberFormatter: precisionRound
            }));
            $('#cpu-time-rate-sparkline').sparkline(this.state.cpuTimeRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {
                chartRangeMin: 0,
                numberFormatter: precisionRound
            }));
            $('#row-input-rate-sparkline').sparkline(this.state.rowInputRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatCount}));
            $('#byte-input-rate-sparkline').sparkline(this.state.byteInputRate, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatDataSize}));
            $('#reserved-memory-sparkline').sparkline(this.state.reservedMemory, $.extend({}, SMALL_SPARKLINE_PROPERTIES, {numberFormatter: formatDataSize}));

            if (this.state.lastRender === null) {
                $('#query').each((i, block) => {
                    hljs.highlightBlock(block);
                });
            }

            this.setState({
                lastRender: renderTimestamp,
            });
        }

        $('[data-toggle="tooltip"]').tooltip();
        new Clipboard('.copy-button');
    }

    renderSplits() {
        if (this.state.lastSnapshotTasks === null) {
            return;
        }

        let splits = [];
        if (this.state.splitFilter !== TASK_FILTER.NONE) {
            // TODO split state
            splits = this.getSplitsFromStage(this.state.lastSnapshotTasks).filter(split => this.state.splitFilter.predicate(""), this);
        }

        return (
            <div className="info-container-next">
                <div className="row">
                    <div className="col-xs-6">
                        <h3 className="container-title">Splits</h3>
                    </div>
                    <div className="col-xs-6">
                        <table className="header-inline-links">
                            <tbody>
                            <tr>
                                <td>
                                    <div className="input-group-btn text-right">
                                        <button type="button"
                                                className="btn btn-default dropdown-toggle pull-right text-right"
                                                data-toggle="dropdown" aria-haspopup="true"
                                                aria-expanded="false">
                                            Show: {this.state.splitFilter.text} <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            {this.renderSplitFilterListItem(TASK_FILTER.NONE)}
                                            {this.renderSplitFilterListItem(TASK_FILTER.ALL)}
                                            {this.renderSplitFilterListItem(TASK_FILTER.FINISHED)}
                                        </ul>
                                    </div>
                                </td>
                                <td>&nbsp;&nbsp;{this.renderSplitRefreshButton()}</td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <SplitList key={this.state.query.queryId} splits={splits}  queryId={this.state.query.queryId}/>
                    </div>
                </div>
            </div>
        );
    }

    renderTasks() {
        if (this.state.lastSnapshotTasks === null) {
            return;
        }

        let tasks = [];
        if (this.state.taskFilter !== TASK_FILTER.NONE) {
            tasks = this.getTasksFromStage(this.state.lastSnapshotTasks).filter(task => this.state.taskFilter.predicate(task.taskStatus.state), this);
        }

        return (
            <div className="info-container-next">
                <div className="row">
                    <div className="col-xs-6">
                        <h3 className="container-title">Tasks</h3>
                    </div>
                    <div className="col-xs-6">
                        <table className="header-inline-links">
                            <tbody>
                            <tr>
                                <td>
                                    <div className="input-group-btn text-right">
                                        <button type="button"
                                                className="btn btn-default dropdown-toggle pull-right text-right"
                                                data-toggle="dropdown" aria-haspopup="true"
                                                aria-expanded="false">
                                            Show: {this.state.taskFilter.text} <span className="caret"/>
                                        </button>
                                        <ul className="dropdown-menu">
                                            {this.renderTaskFilterListItem(TASK_FILTER.NONE)}
                                            {this.renderTaskFilterListItem(TASK_FILTER.ALL)}
                                            {this.renderTaskFilterListItem(TASK_FILTER.PLANNED)}
                                            {this.renderTaskFilterListItem(TASK_FILTER.RUNNING)}
                                            {this.renderTaskFilterListItem(TASK_FILTER.FINISHED)}
                                            {this.renderTaskFilterListItem(TASK_FILTER.FAILED)}
                                        </ul>
                                    </div>
                                </td>
                                <td>&nbsp;&nbsp;{this.renderTaskRefreshButton()}</td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <TaskList key={this.state.query.queryId} tasks={tasks} queryId={this.state.query.queryId}/>
                    </div>
                </div>
            </div>
        );
    }

    renderStages() {
        if (this.state.lastSnapshotStage === null) {
            return;
        }

        return (
            <div className="info-container-next">
                <div className="row">
                    <div className="col-xs-9">
                        <h3 className="container-title">Stages</h3>
                    </div>
                    <div className="col-xs-3">
                        <table className="header-inline-links">
                            <tbody>
                            <tr>
                                <td>
                                    {this.renderStageRefreshButton()}
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row">
                    <div className="col-xs-12">
                        <StageList key={this.state.query.queryId} outputStage={this.state.lastSnapshotStage}/>
                    </div>
                </div>
            </div>
        );
    }

    renderWarningInfo() {
        const query = this.state.query;
        if (query.warnings != null && query.warnings.length > 0) {
            return (
                <div className="row info-container-next">
                    <div className="col-xs-12">
                        <h3>Warnings</h3>
                        <hr className="h3-hr"/>
                        <table className="table" id="warnings-table">
                            {query.warnings.map((warning) =>
                                <tr>
                                    <td>
                                        {warning.warningCode.name}
                                    </td>
                                    <td>
                                        {warning.message}
                                    </td>
                                </tr>
                            )}
                        </table>
                    </div>
                </div>
            );
        }
        else {
            return null;
        }
    }

    renderUserProperties() {
        var query = this.state.query;

        var properties = [];
        for (var property in query.session.userDefVariables) {
            if (query.session.userDefVariables.hasOwnProperty(property)) {
                properties.push(
                    <span key={property}>- {property + "=" + query.session.userDefVariables[property]} <br/></span>
                );
            }
        }

        return properties;
    }

    renderServerProperties() {
        var query = this.state.query;

        var properties = [];
        for (var property in query.session.serverVariables) {
            if (query.session.serverVariables.hasOwnProperty(property)) {
                properties.push(
                    <span key={property}>- {property + "=" + query.session.serverVariables[property]} <br/></span>
                );
            }
        }

        return properties;
    }

    renderFailureInfo() {
        const query = this.state.query;
        if (query.failureInfo) {
            return (
                <div className="row info-container-next">
                    <div className="col-xs-12">
                        <h3>Error Information</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                            <tr>
                                <td className="info-title">
                                    Error Type
                                </td>
                                <td className="info-text">
                                    {query.errorType}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Error Code
                                </td>
                                <td className="info-text">
                                    {QueryDetail.formatErrorCode(query.errorCode)}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Stack Trace
                                    <a className="btn copy-button" data-clipboard-target="#stack-trace"
                                       data-toggle="tooltip" data-placement="right" title="Copy to clipboard">
                                        <span className="glyphicon glyphicon-copy" aria-hidden="true"
                                              alt="Copy to clipboard"/>
                                    </a>
                                </td>
                                <td className="info-text">
                                        <pre id="stack-trace">
                                            {QueryDetail.formatStackTrace(query.failureInfo)}
                                        </pre>
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            );
        }
        else {
            return "";
        }
    }

    render() {
        const query = this.state.query;

        if (query === null || this.state.initialized === false) {
            let label = (<div className="loader">Loading...</div>);
            if (this.state.initialized) {
                label = "Query not found";
            }
            return (
                <div className="row error-message">
                    <div className="col-xs-12"><h4>{label}</h4></div>
                </div>
            );
        }

        return (
            <div>
                <QueryHeader query={query}/>
                <div className="row info-container-next">
                    <div className="col-xs-6">
                        <h3 className="container-title">Session</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                            <tr>
                                <td className="info-title">
                                    User
                                </td>
                                <td className="info-text wrap-text">
                                    {query.session.user}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Schema
                                </td>
                                <td className="info-text">
                                    {query.session.schema}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Submission Time
                                </td>
                                <td className="info-text">
                                    {formatShortDateTime(new Date(query.queryStats.createTime))}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Completion Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.endTime ? formatShortDateTime(new Date(query.queryStats.endTime)) : ""}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Server Properties
                                </td>
                                <td className="info-text">
                                    {this.renderServerProperties()}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    User Properties
                                </td>
                                <td className="info-text">
                                    {this.renderUserProperties()}
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                    <div className="col-xs-6">
                        <h3 className="container-title">Execution</h3>
                        <hr className="h3-hr"/>
                        <table className="table">
                            <tbody>
                            <tr>
                                <td className="info-title">
                                    Elapsed Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.elapsedTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Queued Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.queuedTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Execution Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.executionTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Total Plan Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.totalPlanningTime}
                                </td>
                            </tr>
                            <tr>
                                <td className="info-title">
                                    Distributed Plan Time
                                </td>
                                <td className="info-text">
                                    {query.queryStats.distributedPlanningTime}
                                </td>
                            </tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div className="row info-container-next">
                    <div className="col-xs-12">
                        <div className="row">
                            <div className="col-xs-6">
                                <h3 className="container-title">Resource Utilization Summary</h3>
                                <hr className="h3-hr"/>
                                <table className="table">
                                    <tbody>
                                    <tr>
                                        <td className="info-title">
                                            CPU Time
                                        </td>
                                        <td className="info-text">
                                            {query.queryStats.totalCpuTime}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(query.queryStats.processedInputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Input Data
                                        </td>
                                        <td className="info-text">
                                            {query.queryStats.processedInputDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Raw Input Rows
                                        </td>
                                        <td className="info-text">
                                            {formatCount(query.queryStats.processedInputPositions)}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Raw Input Data
                                        </td>
                                        <td className="info-text">
                                            {query.queryStats.processedInputDataSize}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Peak Memory
                                        </td>
                                        <td className="info-text">
                                            {query.queryStats.peakMemoryReservation}
                                        </td>
                                    </tr>
                                    <tr>
                                        <td className="info-title">
                                            Cumulative Memory
                                        </td>
                                        <td className="info-text">
                                            {formatDataSizeBytes(query.queryStats.cumulativeMemory / 1000.0, "") + " seconds"}
                                        </td>
                                    </tr>
                                    </tbody>
                                </table>
                            </div>
                            {/*<div className="col-xs-6">*/}
                            {/*    <h3>Timeline</h3>*/}
                            {/*    <hr className="h3-hr"/>*/}
                            {/*    <table className="table">*/}
                            {/*        <tbody>*/}
                            {/*        <tr>*/}
                            {/*            <td className="info-title">*/}
                            {/*                Parallelism*/}
                            {/*            </td>*/}
                            {/*            <td rowSpan="2">*/}
                            {/*                <div className="query-stats-sparkline-container">*/}
                            {/*                    <span className="sparkline" id="cpu-time-rate-sparkline"><div*/}
                            {/*                        className="loader">Loading ...</div></span>*/}
                            {/*                </div>*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr className="tr-noborder">*/}
                            {/*            <td className="info-sparkline-text">*/}
                            {/*                {formatCount(this.state.cpuTimeRate[this.state.cpuTimeRate.length - 1])}*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr>*/}
                            {/*            <td className="info-title">*/}
                            {/*                Scheduled Time/s*/}
                            {/*            </td>*/}
                            {/*            <td rowSpan="2">*/}
                            {/*                <div className="query-stats-sparkline-container">*/}
                            {/*                    <span className="sparkline" id="scheduled-time-rate-sparkline"><div*/}
                            {/*                        className="loader">Loading ...</div></span>*/}
                            {/*                </div>*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr className="tr-noborder">*/}
                            {/*            <td className="info-sparkline-text">*/}
                            {/*                {formatCount(this.state.scheduledTimeRate[this.state.scheduledTimeRate.length - 1])}*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr>*/}
                            {/*            <td className="info-title">*/}
                            {/*                Input Rows/s*/}
                            {/*            </td>*/}
                            {/*            <td rowSpan="2">*/}
                            {/*                <div className="query-stats-sparkline-container">*/}
                            {/*                    <span className="sparkline" id="row-input-rate-sparkline"><div*/}
                            {/*                        className="loader">Loading ...</div></span>*/}
                            {/*                </div>*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr className="tr-noborder">*/}
                            {/*            <td className="info-sparkline-text">*/}
                            {/*                {formatCount(this.state.rowInputRate[this.state.rowInputRate.length - 1])}*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr>*/}
                            {/*            <td className="info-title">*/}
                            {/*                Input Bytes/s*/}
                            {/*            </td>*/}
                            {/*            <td rowSpan="2">*/}
                            {/*                <div className="query-stats-sparkline-container">*/}
                            {/*                    <span className="sparkline" id="byte-input-rate-sparkline"><div*/}
                            {/*                        className="loader">Loading ...</div></span>*/}
                            {/*                </div>*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr className="tr-noborder">*/}
                            {/*            <td className="info-sparkline-text">*/}
                            {/*                {formatDataSize(this.state.byteInputRate[this.state.byteInputRate.length - 1])}*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr>*/}
                            {/*            <td className="info-title">*/}
                            {/*                Memory Utilization*/}
                            {/*            </td>*/}
                            {/*            <td rowSpan="2">*/}
                            {/*                <div className="query-stats-sparkline-container">*/}
                            {/*                    <span className="sparkline" id="reserved-memory-sparkline"><div*/}
                            {/*                        className="loader">Loading ...</div></span>*/}
                            {/*                </div>*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        <tr className="tr-noborder">*/}
                            {/*            <td className="info-sparkline-text">*/}
                            {/*                {formatDataSize(this.state.reservedMemory[this.state.reservedMemory.length - 1])}*/}
                            {/*            </td>*/}
                            {/*        </tr>*/}
                            {/*        </tbody>*/}
                            {/*    </table>*/}
                            {/*</div>*/}
                        </div>
                    </div>
                </div>
                {/*{this.renderWarningInfo()}*/}
                {this.renderFailureInfo()}
                <div className="row info-container-next">
                    <div className="col-xs-12">
                        <h3 className="container-title">
                            Query
                            <a className="btn copy-button" data-clipboard-target="#query-text" data-toggle="tooltip"
                               data-placement="right" title="Copy to clipboard">
                                <span className="glyphicon glyphicon-copy" aria-hidden="true" alt="Copy to clipboard"/>
                            </a>
                        </h3>
                        <pre id="query">
                            <code className="lang-sql" id="query-text">
                                {query.query}
                            </code>
                        </pre>
                    </div>
                </div>
                {this.renderStages()}
                {this.renderTasks()}
                {this.renderSplits()}
            </div>
        );
    }
}
