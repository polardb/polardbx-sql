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

package com.alibaba.polardbx.rpc.cdc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 *
 */
// @javax.annotation.Generated(
//     value = "by gRPC proto compiler (version 1.30.0)",
//     comments = "Source: DumperServer.proto")
public final class CdcServiceGrpc {

    private CdcServiceGrpc() {
    }

    public static final String SERVICE_NAME = "dumper.CdcService";

    // Static method descriptors that strictly reflect the proto.
    private static volatile io.grpc.MethodDescriptor<Request,
        BinaryLog> getShowBinaryLogsMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ShowBinaryLogs",
        requestType = Request.class,
        responseType = BinaryLog.class,
        methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<Request,
        BinaryLog> getShowBinaryLogsMethod() {
        io.grpc.MethodDescriptor<Request, BinaryLog>
            getShowBinaryLogsMethod;
        if ((getShowBinaryLogsMethod = CdcServiceGrpc.getShowBinaryLogsMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getShowBinaryLogsMethod = CdcServiceGrpc.getShowBinaryLogsMethod) == null) {
                    CdcServiceGrpc.getShowBinaryLogsMethod = getShowBinaryLogsMethod =
                        io.grpc.MethodDescriptor.<Request, BinaryLog>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowBinaryLogs"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                Request.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                BinaryLog.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowBinaryLogs"))
                            .build();
                }
            }
        }
        return getShowBinaryLogsMethod;
    }

    private static volatile io.grpc.MethodDescriptor<Request,
        MasterStatus> getShowMasterStatusMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ShowMasterStatus",
        requestType = Request.class,
        responseType = MasterStatus.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<Request,
        MasterStatus> getShowMasterStatusMethod() {
        io.grpc.MethodDescriptor<Request, MasterStatus>
            getShowMasterStatusMethod;
        if ((getShowMasterStatusMethod = CdcServiceGrpc.getShowMasterStatusMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getShowMasterStatusMethod = CdcServiceGrpc.getShowMasterStatusMethod) == null) {
                    CdcServiceGrpc.getShowMasterStatusMethod = getShowMasterStatusMethod =
                        io.grpc.MethodDescriptor.<Request, MasterStatus>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowMasterStatus"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                Request.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                MasterStatus.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowMasterStatus"))
                            .build();
                }
            }
        }
        return getShowMasterStatusMethod;
    }

    private static volatile io.grpc.MethodDescriptor<ShowBinlogEventsRequest,
        BinlogEvent> getShowBinlogEventsMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ShowBinlogEvents",
        requestType = ShowBinlogEventsRequest.class,
        responseType = BinlogEvent.class,
        methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<ShowBinlogEventsRequest,
        BinlogEvent> getShowBinlogEventsMethod() {
        io.grpc.MethodDescriptor<ShowBinlogEventsRequest, BinlogEvent>
            getShowBinlogEventsMethod;
        if ((getShowBinlogEventsMethod = CdcServiceGrpc.getShowBinlogEventsMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getShowBinlogEventsMethod = CdcServiceGrpc.getShowBinlogEventsMethod) == null) {
                    CdcServiceGrpc.getShowBinlogEventsMethod = getShowBinlogEventsMethod =
                        io.grpc.MethodDescriptor.<ShowBinlogEventsRequest, BinlogEvent>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowBinlogEvents"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ShowBinlogEventsRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                BinlogEvent.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowBinlogEvents"))
                            .build();
                }
            }
        }
        return getShowBinlogEventsMethod;
    }

    private static volatile io.grpc.MethodDescriptor<DumpRequest,
        DumpStream> getDumpMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "Dump",
        requestType = DumpRequest.class,
        responseType = DumpStream.class,
        methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<DumpRequest,
        DumpStream> getDumpMethod() {
        io.grpc.MethodDescriptor<DumpRequest, DumpStream>
            getDumpMethod;
        if ((getDumpMethod = CdcServiceGrpc.getDumpMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getDumpMethod = CdcServiceGrpc.getDumpMethod) == null) {
                    CdcServiceGrpc.getDumpMethod = getDumpMethod =
                        io.grpc.MethodDescriptor.<DumpRequest, DumpStream>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Dump"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                DumpRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                DumpStream.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("Dump"))
                            .build();
                }
            }
        }
        return getDumpMethod;
    }

    private static volatile io.grpc.MethodDescriptor<DumpRequest,
        DumpStream> getSyncMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "Sync",
        requestType = DumpRequest.class,
        responseType = DumpStream.class,
        methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<DumpRequest,
        DumpStream> getSyncMethod() {
        io.grpc.MethodDescriptor<DumpRequest, DumpStream>
            getSyncMethod;
        if ((getSyncMethod = CdcServiceGrpc.getSyncMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getSyncMethod = CdcServiceGrpc.getSyncMethod) == null) {
                    CdcServiceGrpc.getSyncMethod = getSyncMethod =
                        io.grpc.MethodDescriptor.<DumpRequest, DumpStream>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Sync"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                DumpRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                DumpStream.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("Sync"))
                            .build();
                }
            }
        }
        return getSyncMethod;
    }

    private static volatile io.grpc.MethodDescriptor<ChangeMasterRequest,
        RplCommandResponse> getChangeMasterMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ChangeMaster",
        requestType = ChangeMasterRequest.class,
        responseType = RplCommandResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<ChangeMasterRequest,
        RplCommandResponse> getChangeMasterMethod() {
        io.grpc.MethodDescriptor<ChangeMasterRequest, RplCommandResponse>
            getChangeMasterMethod;
        if ((getChangeMasterMethod = CdcServiceGrpc.getChangeMasterMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getChangeMasterMethod = CdcServiceGrpc.getChangeMasterMethod) == null) {
                    CdcServiceGrpc.getChangeMasterMethod = getChangeMasterMethod =
                        io.grpc.MethodDescriptor.<ChangeMasterRequest, RplCommandResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeMaster"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ChangeMasterRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                RplCommandResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ChangeMaster"))
                            .build();
                }
            }
        }
        return getChangeMasterMethod;
    }

    private static volatile io.grpc.MethodDescriptor<ChangeReplicationFilterRequest,
        RplCommandResponse> getChangeReplicationFilterMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ChangeReplicationFilter",
        requestType = ChangeReplicationFilterRequest.class,
        responseType = RplCommandResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<ChangeReplicationFilterRequest,
        RplCommandResponse> getChangeReplicationFilterMethod() {
        io.grpc.MethodDescriptor<ChangeReplicationFilterRequest, RplCommandResponse>
            getChangeReplicationFilterMethod;
        if ((getChangeReplicationFilterMethod = CdcServiceGrpc.getChangeReplicationFilterMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getChangeReplicationFilterMethod = CdcServiceGrpc.getChangeReplicationFilterMethod) == null) {
                    CdcServiceGrpc.getChangeReplicationFilterMethod = getChangeReplicationFilterMethod =
                        io.grpc.MethodDescriptor.<ChangeReplicationFilterRequest, RplCommandResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeReplicationFilter"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ChangeReplicationFilterRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                RplCommandResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ChangeReplicationFilter"))
                            .build();
                }
            }
        }
        return getChangeReplicationFilterMethod;
    }

    private static volatile io.grpc.MethodDescriptor<StartSlaveRequest,
        RplCommandResponse> getStartSlaveMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "StartSlave",
        requestType = StartSlaveRequest.class,
        responseType = RplCommandResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<StartSlaveRequest,
        RplCommandResponse> getStartSlaveMethod() {
        io.grpc.MethodDescriptor<StartSlaveRequest, RplCommandResponse>
            getStartSlaveMethod;
        if ((getStartSlaveMethod = CdcServiceGrpc.getStartSlaveMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getStartSlaveMethod = CdcServiceGrpc.getStartSlaveMethod) == null) {
                    CdcServiceGrpc.getStartSlaveMethod = getStartSlaveMethod =
                        io.grpc.MethodDescriptor.<StartSlaveRequest, RplCommandResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartSlave"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                StartSlaveRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                RplCommandResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("StartSlave"))
                            .build();
                }
            }
        }
        return getStartSlaveMethod;
    }

    private static volatile io.grpc.MethodDescriptor<StopSlaveRequest,
        RplCommandResponse> getStopSlaveMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "StopSlave",
        requestType = StopSlaveRequest.class,
        responseType = RplCommandResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<StopSlaveRequest,
        RplCommandResponse> getStopSlaveMethod() {
        io.grpc.MethodDescriptor<StopSlaveRequest, RplCommandResponse>
            getStopSlaveMethod;
        if ((getStopSlaveMethod = CdcServiceGrpc.getStopSlaveMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getStopSlaveMethod = CdcServiceGrpc.getStopSlaveMethod) == null) {
                    CdcServiceGrpc.getStopSlaveMethod = getStopSlaveMethod =
                        io.grpc.MethodDescriptor.<StopSlaveRequest, RplCommandResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StopSlave"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                StopSlaveRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                RplCommandResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("StopSlave"))
                            .build();
                }
            }
        }
        return getStopSlaveMethod;
    }

    private static volatile io.grpc.MethodDescriptor<ResetSlaveRequest,
        RplCommandResponse> getResetSlaveMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ResetSlave",
        requestType = ResetSlaveRequest.class,
        responseType = RplCommandResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
    public static io.grpc.MethodDescriptor<ResetSlaveRequest,
        RplCommandResponse> getResetSlaveMethod() {
        io.grpc.MethodDescriptor<ResetSlaveRequest, RplCommandResponse>
            getResetSlaveMethod;
        if ((getResetSlaveMethod = CdcServiceGrpc.getResetSlaveMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getResetSlaveMethod = CdcServiceGrpc.getResetSlaveMethod) == null) {
                    CdcServiceGrpc.getResetSlaveMethod = getResetSlaveMethod =
                        io.grpc.MethodDescriptor.<ResetSlaveRequest, RplCommandResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResetSlave"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ResetSlaveRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                RplCommandResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ResetSlave"))
                            .build();
                }
            }
        }
        return getResetSlaveMethod;
    }

    private static volatile io.grpc.MethodDescriptor<ShowSlaveStatusRequest,
        ShowSlaveStatusResponse> getShowSlaveStatusMethod;

    @io.grpc.stub.annotations.RpcMethod(
        fullMethodName = SERVICE_NAME + '/' + "ShowSlaveStatus",
        requestType = ShowSlaveStatusRequest.class,
        responseType = ShowSlaveStatusResponse.class,
        methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
    public static io.grpc.MethodDescriptor<ShowSlaveStatusRequest,
        ShowSlaveStatusResponse> getShowSlaveStatusMethod() {
        io.grpc.MethodDescriptor<ShowSlaveStatusRequest, ShowSlaveStatusResponse>
            getShowSlaveStatusMethod;
        if ((getShowSlaveStatusMethod = CdcServiceGrpc.getShowSlaveStatusMethod) == null) {
            synchronized (CdcServiceGrpc.class) {
                if ((getShowSlaveStatusMethod = CdcServiceGrpc.getShowSlaveStatusMethod) == null) {
                    CdcServiceGrpc.getShowSlaveStatusMethod = getShowSlaveStatusMethod =
                        io.grpc.MethodDescriptor.<ShowSlaveStatusRequest, ShowSlaveStatusResponse>newBuilder()
                            .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
                            .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowSlaveStatus"))
                            .setSampledToLocalTracing(true)
                            .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ShowSlaveStatusRequest.getDefaultInstance()))
                            .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                                ShowSlaveStatusResponse.getDefaultInstance()))
                            .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowSlaveStatus"))
                            .build();
                }
            }
        }
        return getShowSlaveStatusMethod;
    }

    /**
     * Creates a new async stub that supports all call types for the service
     */
    public static CdcServiceStub newStub(io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CdcServiceStub> factory =
            new io.grpc.stub.AbstractStub.StubFactory<CdcServiceStub>() {
                @java.lang.Override
                public CdcServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                    return new CdcServiceStub(channel, callOptions);
                }
            };
        return CdcServiceStub.newStub(factory, channel);
    }

    /**
     * Creates a new blocking-style stub that supports unary and streaming output calls on the service
     */
    public static CdcServiceBlockingStub newBlockingStub(
        io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CdcServiceBlockingStub> factory =
            new io.grpc.stub.AbstractStub.StubFactory<CdcServiceBlockingStub>() {
                @java.lang.Override
                public CdcServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                    return new CdcServiceBlockingStub(channel, callOptions);
                }
            };
        return CdcServiceBlockingStub.newStub(factory, channel);
    }

    /**
     * Creates a new ListenableFuture-style stub that supports unary calls on the service
     */
    public static CdcServiceFutureStub newFutureStub(
        io.grpc.Channel channel) {
        io.grpc.stub.AbstractStub.StubFactory<CdcServiceFutureStub> factory =
            new io.grpc.stub.AbstractStub.StubFactory<CdcServiceFutureStub>() {
                @java.lang.Override
                public CdcServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
                    return new CdcServiceFutureStub(channel, callOptions);
                }
            };
        return CdcServiceFutureStub.newStub(factory, channel);
    }

    /**
     *
     */
    public static abstract class CdcServiceImplBase implements io.grpc.BindableService {

        /**
         * <pre>
         * ShowBinaryLogs + ShowMasterLogs
         * </pre>
         */
        public void showBinaryLogs(Request request,
                                   io.grpc.stub.StreamObserver<BinaryLog> responseObserver) {
            asyncUnimplementedUnaryCall(getShowBinaryLogsMethod(), responseObserver);
        }

        /**
         *
         */
        public void showMasterStatus(Request request,
                                     io.grpc.stub.StreamObserver<MasterStatus> responseObserver) {
            asyncUnimplementedUnaryCall(getShowMasterStatusMethod(), responseObserver);
        }

        /**
         *
         */
        public void showBinlogEvents(ShowBinlogEventsRequest request,
                                     io.grpc.stub.StreamObserver<BinlogEvent> responseObserver) {
            asyncUnimplementedUnaryCall(getShowBinlogEventsMethod(), responseObserver);
        }

        /**
         *
         */
        public void dump(DumpRequest request,
                         io.grpc.stub.StreamObserver<DumpStream> responseObserver) {
            asyncUnimplementedUnaryCall(getDumpMethod(), responseObserver);
        }

        /**
         *
         */
        public void sync(DumpRequest request,
                         io.grpc.stub.StreamObserver<DumpStream> responseObserver) {
            asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
        }

        /**
         * <pre>
         * &#47;/////////////////////////// replicate   ///////////////////////////
         * </pre>
         */
        public void changeMaster(ChangeMasterRequest request,
                                 io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getChangeMasterMethod(), responseObserver);
        }

        /**
         *
         */
        public void changeReplicationFilter(ChangeReplicationFilterRequest request,
                                            io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getChangeReplicationFilterMethod(), responseObserver);
        }

        /**
         *
         */
        public void startSlave(StartSlaveRequest request,
                               io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getStartSlaveMethod(), responseObserver);
        }

        /**
         *
         */
        public void stopSlave(StopSlaveRequest request,
                              io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getStopSlaveMethod(), responseObserver);
        }

        /**
         *
         */
        public void resetSlave(ResetSlaveRequest request,
                               io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getResetSlaveMethod(), responseObserver);
        }

        /**
         *
         */
        public void showSlaveStatus(ShowSlaveStatusRequest request,
                                    io.grpc.stub.StreamObserver<ShowSlaveStatusResponse> responseObserver) {
            asyncUnimplementedUnaryCall(getShowSlaveStatusMethod(), responseObserver);
        }

        @java.lang.Override
        public final io.grpc.ServerServiceDefinition bindService() {
            return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
                .addMethod(
                    getShowBinaryLogsMethod(),
                    asyncServerStreamingCall(
                        new MethodHandlers<
                            Request,
                            BinaryLog>(
                            this, METHODID_SHOW_BINARY_LOGS)))
                .addMethod(
                    getShowMasterStatusMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            Request,
                            MasterStatus>(
                            this, METHODID_SHOW_MASTER_STATUS)))
                .addMethod(
                    getShowBinlogEventsMethod(),
                    asyncServerStreamingCall(
                        new MethodHandlers<
                            ShowBinlogEventsRequest,
                            BinlogEvent>(
                            this, METHODID_SHOW_BINLOG_EVENTS)))
                .addMethod(
                    getDumpMethod(),
                    asyncServerStreamingCall(
                        new MethodHandlers<
                            DumpRequest,
                            DumpStream>(
                            this, METHODID_DUMP)))
                .addMethod(
                    getSyncMethod(),
                    asyncServerStreamingCall(
                        new MethodHandlers<
                            DumpRequest,
                            DumpStream>(
                            this, METHODID_SYNC)))
                .addMethod(
                    getChangeMasterMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            ChangeMasterRequest,
                            RplCommandResponse>(
                            this, METHODID_CHANGE_MASTER)))
                .addMethod(
                    getChangeReplicationFilterMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            ChangeReplicationFilterRequest,
                            RplCommandResponse>(
                            this, METHODID_CHANGE_REPLICATION_FILTER)))
                .addMethod(
                    getStartSlaveMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            StartSlaveRequest,
                            RplCommandResponse>(
                            this, METHODID_START_SLAVE)))
                .addMethod(
                    getStopSlaveMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            StopSlaveRequest,
                            RplCommandResponse>(
                            this, METHODID_STOP_SLAVE)))
                .addMethod(
                    getResetSlaveMethod(),
                    asyncUnaryCall(
                        new MethodHandlers<
                            ResetSlaveRequest,
                            RplCommandResponse>(
                            this, METHODID_RESET_SLAVE)))
                .addMethod(
                    getShowSlaveStatusMethod(),
                    asyncServerStreamingCall(
                        new MethodHandlers<
                            ShowSlaveStatusRequest,
                            ShowSlaveStatusResponse>(
                            this, METHODID_SHOW_SLAVE_STATUS)))
                .build();
        }
    }

    /**
     *
     */
    public static final class CdcServiceStub extends io.grpc.stub.AbstractAsyncStub<CdcServiceStub> {
        private CdcServiceStub(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CdcServiceStub build(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CdcServiceStub(channel, callOptions);
        }

        /**
         * <pre>
         * ShowBinaryLogs + ShowMasterLogs
         * </pre>
         */
        public void showBinaryLogs(Request request,
                                   io.grpc.stub.StreamObserver<BinaryLog> responseObserver) {
            asyncServerStreamingCall(
                getChannel().newCall(getShowBinaryLogsMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void showMasterStatus(Request request,
                                     io.grpc.stub.StreamObserver<MasterStatus> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getShowMasterStatusMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void showBinlogEvents(ShowBinlogEventsRequest request,
                                     io.grpc.stub.StreamObserver<BinlogEvent> responseObserver) {
            asyncServerStreamingCall(
                getChannel().newCall(getShowBinlogEventsMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void dump(DumpRequest request,
                         io.grpc.stub.StreamObserver<DumpStream> responseObserver) {
            asyncServerStreamingCall(
                getChannel().newCall(getDumpMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void sync(DumpRequest request,
                         io.grpc.stub.StreamObserver<DumpStream> responseObserver) {
            asyncServerStreamingCall(
                getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         * <pre>
         * &#47;/////////////////////////// replicate   ///////////////////////////
         * </pre>
         */
        public void changeMaster(ChangeMasterRequest request,
                                 io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getChangeMasterMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void changeReplicationFilter(ChangeReplicationFilterRequest request,
                                            io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getChangeReplicationFilterMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void startSlave(StartSlaveRequest request,
                               io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getStartSlaveMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void stopSlave(StopSlaveRequest request,
                              io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getStopSlaveMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void resetSlave(ResetSlaveRequest request,
                               io.grpc.stub.StreamObserver<RplCommandResponse> responseObserver) {
            asyncUnaryCall(
                getChannel().newCall(getResetSlaveMethod(), getCallOptions()), request, responseObserver);
        }

        /**
         *
         */
        public void showSlaveStatus(ShowSlaveStatusRequest request,
                                    io.grpc.stub.StreamObserver<ShowSlaveStatusResponse> responseObserver) {
            asyncServerStreamingCall(
                getChannel().newCall(getShowSlaveStatusMethod(), getCallOptions()), request, responseObserver);
        }
    }

    /**
     *
     */
    public static final class CdcServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<CdcServiceBlockingStub> {
        private CdcServiceBlockingStub(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CdcServiceBlockingStub build(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CdcServiceBlockingStub(channel, callOptions);
        }

        /**
         * <pre>
         * ShowBinaryLogs + ShowMasterLogs
         * </pre>
         */
        public java.util.Iterator<BinaryLog> showBinaryLogs(
            Request request) {
            return blockingServerStreamingCall(
                getChannel(), getShowBinaryLogsMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public MasterStatus showMasterStatus(Request request) {
            return blockingUnaryCall(
                getChannel(), getShowMasterStatusMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public java.util.Iterator<BinlogEvent> showBinlogEvents(
            ShowBinlogEventsRequest request) {
            return blockingServerStreamingCall(
                getChannel(), getShowBinlogEventsMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public java.util.Iterator<DumpStream> dump(
            DumpRequest request) {
            return blockingServerStreamingCall(
                getChannel(), getDumpMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public java.util.Iterator<DumpStream> sync(
            DumpRequest request) {
            return blockingServerStreamingCall(
                getChannel(), getSyncMethod(), getCallOptions(), request);
        }

        /**
         * <pre>
         * &#47;/////////////////////////// replicate   ///////////////////////////
         * </pre>
         */
        public RplCommandResponse changeMaster(
            ChangeMasterRequest request) {
            return blockingUnaryCall(
                getChannel(), getChangeMasterMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public RplCommandResponse changeReplicationFilter(
            ChangeReplicationFilterRequest request) {
            return blockingUnaryCall(
                getChannel(), getChangeReplicationFilterMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public RplCommandResponse startSlave(
            StartSlaveRequest request) {
            return blockingUnaryCall(
                getChannel(), getStartSlaveMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public RplCommandResponse stopSlave(
            StopSlaveRequest request) {
            return blockingUnaryCall(
                getChannel(), getStopSlaveMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public RplCommandResponse resetSlave(
            ResetSlaveRequest request) {
            return blockingUnaryCall(
                getChannel(), getResetSlaveMethod(), getCallOptions(), request);
        }

        /**
         *
         */
        public java.util.Iterator<ShowSlaveStatusResponse> showSlaveStatus(
            ShowSlaveStatusRequest request) {
            return blockingServerStreamingCall(
                getChannel(), getShowSlaveStatusMethod(), getCallOptions(), request);
        }
    }

    /**
     *
     */
    public static final class CdcServiceFutureStub extends io.grpc.stub.AbstractFutureStub<CdcServiceFutureStub> {
        private CdcServiceFutureStub(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            super(channel, callOptions);
        }

        @java.lang.Override
        protected CdcServiceFutureStub build(
            io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
            return new CdcServiceFutureStub(channel, callOptions);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<MasterStatus> showMasterStatus(
            Request request) {
            return futureUnaryCall(
                getChannel().newCall(getShowMasterStatusMethod(), getCallOptions()), request);
        }

        /**
         * <pre>
         * &#47;/////////////////////////// replicate   ///////////////////////////
         * </pre>
         */
        public com.google.common.util.concurrent.ListenableFuture<RplCommandResponse> changeMaster(
            ChangeMasterRequest request) {
            return futureUnaryCall(
                getChannel().newCall(getChangeMasterMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<RplCommandResponse> changeReplicationFilter(
            ChangeReplicationFilterRequest request) {
            return futureUnaryCall(
                getChannel().newCall(getChangeReplicationFilterMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<RplCommandResponse> startSlave(
            StartSlaveRequest request) {
            return futureUnaryCall(
                getChannel().newCall(getStartSlaveMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<RplCommandResponse> stopSlave(
            StopSlaveRequest request) {
            return futureUnaryCall(
                getChannel().newCall(getStopSlaveMethod(), getCallOptions()), request);
        }

        /**
         *
         */
        public com.google.common.util.concurrent.ListenableFuture<RplCommandResponse> resetSlave(
            ResetSlaveRequest request) {
            return futureUnaryCall(
                getChannel().newCall(getResetSlaveMethod(), getCallOptions()), request);
        }
    }

    private static final int METHODID_SHOW_BINARY_LOGS = 0;
    private static final int METHODID_SHOW_MASTER_STATUS = 1;
    private static final int METHODID_SHOW_BINLOG_EVENTS = 2;
    private static final int METHODID_DUMP = 3;
    private static final int METHODID_SYNC = 4;
    private static final int METHODID_CHANGE_MASTER = 5;
    private static final int METHODID_CHANGE_REPLICATION_FILTER = 6;
    private static final int METHODID_START_SLAVE = 7;
    private static final int METHODID_STOP_SLAVE = 8;
    private static final int METHODID_RESET_SLAVE = 9;
    private static final int METHODID_SHOW_SLAVE_STATUS = 10;

    private static final class MethodHandlers<Req, Resp> implements
        io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
        io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
        private final CdcServiceImplBase serviceImpl;
        private final int methodId;

        MethodHandlers(CdcServiceImplBase serviceImpl, int methodId) {
            this.serviceImpl = serviceImpl;
            this.methodId = methodId;
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
            case METHODID_SHOW_BINARY_LOGS:
                serviceImpl.showBinaryLogs((Request) request,
                    (io.grpc.stub.StreamObserver<BinaryLog>) responseObserver);
                break;
            case METHODID_SHOW_MASTER_STATUS:
                serviceImpl.showMasterStatus((Request) request,
                    (io.grpc.stub.StreamObserver<MasterStatus>) responseObserver);
                break;
            case METHODID_SHOW_BINLOG_EVENTS:
                serviceImpl.showBinlogEvents((ShowBinlogEventsRequest) request,
                    (io.grpc.stub.StreamObserver<BinlogEvent>) responseObserver);
                break;
            case METHODID_DUMP:
                serviceImpl.dump((DumpRequest) request,
                    (io.grpc.stub.StreamObserver<DumpStream>) responseObserver);
                break;
            case METHODID_SYNC:
                serviceImpl.sync((DumpRequest) request,
                    (io.grpc.stub.StreamObserver<DumpStream>) responseObserver);
                break;
            case METHODID_CHANGE_MASTER:
                serviceImpl.changeMaster((ChangeMasterRequest) request,
                    (io.grpc.stub.StreamObserver<RplCommandResponse>) responseObserver);
                break;
            case METHODID_CHANGE_REPLICATION_FILTER:
                serviceImpl.changeReplicationFilter((ChangeReplicationFilterRequest) request,
                    (io.grpc.stub.StreamObserver<RplCommandResponse>) responseObserver);
                break;
            case METHODID_START_SLAVE:
                serviceImpl.startSlave((StartSlaveRequest) request,
                    (io.grpc.stub.StreamObserver<RplCommandResponse>) responseObserver);
                break;
            case METHODID_STOP_SLAVE:
                serviceImpl.stopSlave((StopSlaveRequest) request,
                    (io.grpc.stub.StreamObserver<RplCommandResponse>) responseObserver);
                break;
            case METHODID_RESET_SLAVE:
                serviceImpl.resetSlave((ResetSlaveRequest) request,
                    (io.grpc.stub.StreamObserver<RplCommandResponse>) responseObserver);
                break;
            case METHODID_SHOW_SLAVE_STATUS:
                serviceImpl.showSlaveStatus((ShowSlaveStatusRequest) request,
                    (io.grpc.stub.StreamObserver<ShowSlaveStatusResponse>) responseObserver);
                break;
            default:
                throw new AssertionError();
            }
        }

        @java.lang.Override
        @java.lang.SuppressWarnings("unchecked")
        public io.grpc.stub.StreamObserver<Req> invoke(
            io.grpc.stub.StreamObserver<Resp> responseObserver) {
            switch (methodId) {
            default:
                throw new AssertionError();
            }
        }
    }

    private static abstract class CdcServiceBaseDescriptorSupplier
        implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
        CdcServiceBaseDescriptorSupplier() {
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
            return DumperServer.getDescriptor();
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
            return getFileDescriptor().findServiceByName("CdcService");
        }
    }

    private static final class CdcServiceFileDescriptorSupplier
        extends CdcServiceBaseDescriptorSupplier {
        CdcServiceFileDescriptorSupplier() {
        }
    }

    private static final class CdcServiceMethodDescriptorSupplier
        extends CdcServiceBaseDescriptorSupplier
        implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
        private final String methodName;

        CdcServiceMethodDescriptorSupplier(String methodName) {
            this.methodName = methodName;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
            return getServiceDescriptor().findMethodByName(methodName);
        }
    }

    private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

    public static io.grpc.ServiceDescriptor getServiceDescriptor() {
        io.grpc.ServiceDescriptor result = serviceDescriptor;
        if (result == null) {
            synchronized (CdcServiceGrpc.class) {
                result = serviceDescriptor;
                if (result == null) {
                    serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
                        .setSchemaDescriptor(new CdcServiceFileDescriptorSupplier())
                        .addMethod(getShowBinaryLogsMethod())
                        .addMethod(getShowMasterStatusMethod())
                        .addMethod(getShowBinlogEventsMethod())
                        .addMethod(getDumpMethod())
                        .addMethod(getSyncMethod())
                        .addMethod(getChangeMasterMethod())
                        .addMethod(getChangeReplicationFilterMethod())
                        .addMethod(getStartSlaveMethod())
                        .addMethod(getStopSlaveMethod())
                        .addMethod(getResetSlaveMethod())
                        .addMethod(getShowSlaveStatusMethod())
                        .build();
                }
            }
        }
        return result;
    }
}
