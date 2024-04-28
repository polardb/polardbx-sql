import React from "react";
import ReactDOM from "react-dom";
import {PipelinePlan} from "./components/Pipeline";
import {PageTitle} from "./components/PageTitle";
import {getFirstParameter} from "./utils";

ReactDOM.render(
    <PageTitle title="Pipeline Overview"/>,
    document.getElementById('title')
);

ReactDOM.render(
    <PipelinePlan queryId={getFirstParameter(window.location.search)} isEmbedded={false}/>,
    document.getElementById('live-plan-container')
);
