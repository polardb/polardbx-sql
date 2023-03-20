package com.alibaba.polardbx.rpc.cdc;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.30.0)",
    comments = "Source: DumperServer.proto")
public final class CdcServiceGrpc {

  private CdcServiceGrpc() {}

  public static final String SERVICE_NAME = "dumper.CdcService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request,
      com.alibaba.polardbx.rpc.cdc.BinaryLog> getShowBinaryLogsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShowBinaryLogs",
      requestType = com.alibaba.polardbx.rpc.cdc.Request.class,
      responseType = com.alibaba.polardbx.rpc.cdc.BinaryLog.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request,
      com.alibaba.polardbx.rpc.cdc.BinaryLog> getShowBinaryLogsMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request, com.alibaba.polardbx.rpc.cdc.BinaryLog> getShowBinaryLogsMethod;
    if ((getShowBinaryLogsMethod = CdcServiceGrpc.getShowBinaryLogsMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getShowBinaryLogsMethod = CdcServiceGrpc.getShowBinaryLogsMethod) == null) {
          CdcServiceGrpc.getShowBinaryLogsMethod = getShowBinaryLogsMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.Request, com.alibaba.polardbx.rpc.cdc.BinaryLog>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowBinaryLogs"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.BinaryLog.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowBinaryLogs"))
              .build();
        }
      }
    }
    return getShowBinaryLogsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request,
      com.alibaba.polardbx.rpc.cdc.MasterStatus> getShowMasterStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShowMasterStatus",
      requestType = com.alibaba.polardbx.rpc.cdc.Request.class,
      responseType = com.alibaba.polardbx.rpc.cdc.MasterStatus.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request,
      com.alibaba.polardbx.rpc.cdc.MasterStatus> getShowMasterStatusMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.Request, com.alibaba.polardbx.rpc.cdc.MasterStatus> getShowMasterStatusMethod;
    if ((getShowMasterStatusMethod = CdcServiceGrpc.getShowMasterStatusMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getShowMasterStatusMethod = CdcServiceGrpc.getShowMasterStatusMethod) == null) {
          CdcServiceGrpc.getShowMasterStatusMethod = getShowMasterStatusMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.Request, com.alibaba.polardbx.rpc.cdc.MasterStatus>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowMasterStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.Request.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.MasterStatus.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowMasterStatus"))
              .build();
        }
      }
    }
    return getShowMasterStatusMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest,
      com.alibaba.polardbx.rpc.cdc.BinlogEvent> getShowBinlogEventsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShowBinlogEvents",
      requestType = com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.BinlogEvent.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest,
      com.alibaba.polardbx.rpc.cdc.BinlogEvent> getShowBinlogEventsMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest, com.alibaba.polardbx.rpc.cdc.BinlogEvent> getShowBinlogEventsMethod;
    if ((getShowBinlogEventsMethod = CdcServiceGrpc.getShowBinlogEventsMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getShowBinlogEventsMethod = CdcServiceGrpc.getShowBinlogEventsMethod) == null) {
          CdcServiceGrpc.getShowBinlogEventsMethod = getShowBinlogEventsMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest, com.alibaba.polardbx.rpc.cdc.BinlogEvent>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowBinlogEvents"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.BinlogEvent.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ShowBinlogEvents"))
              .build();
        }
      }
    }
    return getShowBinlogEventsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest,
      com.alibaba.polardbx.rpc.cdc.DumpStream> getDumpMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Dump",
      requestType = com.alibaba.polardbx.rpc.cdc.DumpRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.DumpStream.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest,
      com.alibaba.polardbx.rpc.cdc.DumpStream> getDumpMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest, com.alibaba.polardbx.rpc.cdc.DumpStream> getDumpMethod;
    if ((getDumpMethod = CdcServiceGrpc.getDumpMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getDumpMethod = CdcServiceGrpc.getDumpMethod) == null) {
          CdcServiceGrpc.getDumpMethod = getDumpMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.DumpRequest, com.alibaba.polardbx.rpc.cdc.DumpStream>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Dump"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.DumpRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.DumpStream.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("Dump"))
              .build();
        }
      }
    }
    return getDumpMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest,
      com.alibaba.polardbx.rpc.cdc.DumpStream> getSyncMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Sync",
      requestType = com.alibaba.polardbx.rpc.cdc.DumpRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.DumpStream.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest,
      com.alibaba.polardbx.rpc.cdc.DumpStream> getSyncMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.DumpRequest, com.alibaba.polardbx.rpc.cdc.DumpStream> getSyncMethod;
    if ((getSyncMethod = CdcServiceGrpc.getSyncMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getSyncMethod = CdcServiceGrpc.getSyncMethod) == null) {
          CdcServiceGrpc.getSyncMethod = getSyncMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.DumpRequest, com.alibaba.polardbx.rpc.cdc.DumpStream>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Sync"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.DumpRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.DumpStream.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("Sync"))
              .build();
        }
      }
    }
    return getSyncMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeMasterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeMaster",
      requestType = com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.RplCommandResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeMasterMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeMasterMethod;
    if ((getChangeMasterMethod = CdcServiceGrpc.getChangeMasterMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getChangeMasterMethod = CdcServiceGrpc.getChangeMasterMethod) == null) {
          CdcServiceGrpc.getChangeMasterMethod = getChangeMasterMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeMaster"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.RplCommandResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ChangeMaster"))
              .build();
        }
      }
    }
    return getChangeMasterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeReplicationFilterMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ChangeReplicationFilter",
      requestType = com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.RplCommandResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeReplicationFilterMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getChangeReplicationFilterMethod;
    if ((getChangeReplicationFilterMethod = CdcServiceGrpc.getChangeReplicationFilterMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getChangeReplicationFilterMethod = CdcServiceGrpc.getChangeReplicationFilterMethod) == null) {
          CdcServiceGrpc.getChangeReplicationFilterMethod = getChangeReplicationFilterMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ChangeReplicationFilter"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.RplCommandResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ChangeReplicationFilter"))
              .build();
        }
      }
    }
    return getChangeReplicationFilterMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StartSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStartSlaveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StartSlave",
      requestType = com.alibaba.polardbx.rpc.cdc.StartSlaveRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.RplCommandResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StartSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStartSlaveMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StartSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStartSlaveMethod;
    if ((getStartSlaveMethod = CdcServiceGrpc.getStartSlaveMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getStartSlaveMethod = CdcServiceGrpc.getStartSlaveMethod) == null) {
          CdcServiceGrpc.getStartSlaveMethod = getStartSlaveMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.StartSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StartSlave"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.StartSlaveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.RplCommandResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("StartSlave"))
              .build();
        }
      }
    }
    return getStartSlaveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StopSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStopSlaveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "StopSlave",
      requestType = com.alibaba.polardbx.rpc.cdc.StopSlaveRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.RplCommandResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StopSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStopSlaveMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.StopSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getStopSlaveMethod;
    if ((getStopSlaveMethod = CdcServiceGrpc.getStopSlaveMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getStopSlaveMethod = CdcServiceGrpc.getStopSlaveMethod) == null) {
          CdcServiceGrpc.getStopSlaveMethod = getStopSlaveMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.StopSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "StopSlave"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.StopSlaveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.RplCommandResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("StopSlave"))
              .build();
        }
      }
    }
    return getStopSlaveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getResetSlaveMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ResetSlave",
      requestType = com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.RplCommandResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest,
      com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getResetSlaveMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse> getResetSlaveMethod;
    if ((getResetSlaveMethod = CdcServiceGrpc.getResetSlaveMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getResetSlaveMethod = CdcServiceGrpc.getResetSlaveMethod) == null) {
          CdcServiceGrpc.getResetSlaveMethod = getResetSlaveMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest, com.alibaba.polardbx.rpc.cdc.RplCommandResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ResetSlave"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.RplCommandResponse.getDefaultInstance()))
              .setSchemaDescriptor(new CdcServiceMethodDescriptorSupplier("ResetSlave"))
              .build();
        }
      }
    }
    return getResetSlaveMethod;
  }

  private static volatile io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest,
      com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> getShowSlaveStatusMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ShowSlaveStatus",
      requestType = com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest.class,
      responseType = com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
  public static io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest,
      com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> getShowSlaveStatusMethod() {
    io.grpc.MethodDescriptor<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest, com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> getShowSlaveStatusMethod;
    if ((getShowSlaveStatusMethod = CdcServiceGrpc.getShowSlaveStatusMethod) == null) {
      synchronized (CdcServiceGrpc.class) {
        if ((getShowSlaveStatusMethod = CdcServiceGrpc.getShowSlaveStatusMethod) == null) {
          CdcServiceGrpc.getShowSlaveStatusMethod = getShowSlaveStatusMethod =
              io.grpc.MethodDescriptor.<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest, com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.SERVER_STREAMING)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ShowSlaveStatus"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse.getDefaultInstance()))
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
   */
  public static abstract class CdcServiceImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * ShowBinaryLogs + ShowMasterLogs
     * </pre>
     */
    public void showBinaryLogs(com.alibaba.polardbx.rpc.cdc.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinaryLog> responseObserver) {
      asyncUnimplementedUnaryCall(getShowBinaryLogsMethod(), responseObserver);
    }

    /**
     */
    public void showMasterStatus(com.alibaba.polardbx.rpc.cdc.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.MasterStatus> responseObserver) {
      asyncUnimplementedUnaryCall(getShowMasterStatusMethod(), responseObserver);
    }

    /**
     */
    public void showBinlogEvents(com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinlogEvent> responseObserver) {
      asyncUnimplementedUnaryCall(getShowBinlogEventsMethod(), responseObserver);
    }

    /**
     */
    public void dump(com.alibaba.polardbx.rpc.cdc.DumpRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream> responseObserver) {
      asyncUnimplementedUnaryCall(getDumpMethod(), responseObserver);
    }

    /**
     */
    public void sync(com.alibaba.polardbx.rpc.cdc.DumpRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream> responseObserver) {
      asyncUnimplementedUnaryCall(getSyncMethod(), responseObserver);
    }

    /**
     * <pre>
     *&#47;/////////////////////////// replicate   ///////////////////////////
     * </pre>
     */
    public void changeMaster(com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getChangeMasterMethod(), responseObserver);
    }

    /**
     */
    public void changeReplicationFilter(com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getChangeReplicationFilterMethod(), responseObserver);
    }

    /**
     */
    public void startSlave(com.alibaba.polardbx.rpc.cdc.StartSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getStartSlaveMethod(), responseObserver);
    }

    /**
     */
    public void stopSlave(com.alibaba.polardbx.rpc.cdc.StopSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getStopSlaveMethod(), responseObserver);
    }

    /**
     */
    public void resetSlave(com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getResetSlaveMethod(), responseObserver);
    }

    /**
     */
    public void showSlaveStatus(com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getShowSlaveStatusMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getShowBinaryLogsMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.Request,
                com.alibaba.polardbx.rpc.cdc.BinaryLog>(
                  this, METHODID_SHOW_BINARY_LOGS)))
          .addMethod(
            getShowMasterStatusMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.Request,
                com.alibaba.polardbx.rpc.cdc.MasterStatus>(
                  this, METHODID_SHOW_MASTER_STATUS)))
          .addMethod(
            getShowBinlogEventsMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest,
                com.alibaba.polardbx.rpc.cdc.BinlogEvent>(
                  this, METHODID_SHOW_BINLOG_EVENTS)))
          .addMethod(
            getDumpMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.DumpRequest,
                com.alibaba.polardbx.rpc.cdc.DumpStream>(
                  this, METHODID_DUMP)))
          .addMethod(
            getSyncMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.DumpRequest,
                com.alibaba.polardbx.rpc.cdc.DumpStream>(
                  this, METHODID_SYNC)))
          .addMethod(
            getChangeMasterMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest,
                com.alibaba.polardbx.rpc.cdc.RplCommandResponse>(
                  this, METHODID_CHANGE_MASTER)))
          .addMethod(
            getChangeReplicationFilterMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest,
                com.alibaba.polardbx.rpc.cdc.RplCommandResponse>(
                  this, METHODID_CHANGE_REPLICATION_FILTER)))
          .addMethod(
            getStartSlaveMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.StartSlaveRequest,
                com.alibaba.polardbx.rpc.cdc.RplCommandResponse>(
                  this, METHODID_START_SLAVE)))
          .addMethod(
            getStopSlaveMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.StopSlaveRequest,
                com.alibaba.polardbx.rpc.cdc.RplCommandResponse>(
                  this, METHODID_STOP_SLAVE)))
          .addMethod(
            getResetSlaveMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest,
                com.alibaba.polardbx.rpc.cdc.RplCommandResponse>(
                  this, METHODID_RESET_SLAVE)))
          .addMethod(
            getShowSlaveStatusMethod(),
            asyncServerStreamingCall(
              new MethodHandlers<
                com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest,
                com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse>(
                  this, METHODID_SHOW_SLAVE_STATUS)))
          .build();
    }
  }

  /**
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
    public void showBinaryLogs(com.alibaba.polardbx.rpc.cdc.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinaryLog> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getShowBinaryLogsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void showMasterStatus(com.alibaba.polardbx.rpc.cdc.Request request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.MasterStatus> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getShowMasterStatusMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void showBinlogEvents(com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinlogEvent> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getShowBinlogEventsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dump(com.alibaba.polardbx.rpc.cdc.DumpRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getDumpMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sync(com.alibaba.polardbx.rpc.cdc.DumpRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getSyncMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     * <pre>
     *&#47;/////////////////////////// replicate   ///////////////////////////
     * </pre>
     */
    public void changeMaster(com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getChangeMasterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void changeReplicationFilter(com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getChangeReplicationFilterMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void startSlave(com.alibaba.polardbx.rpc.cdc.StartSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getStartSlaveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void stopSlave(com.alibaba.polardbx.rpc.cdc.StopSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getStopSlaveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void resetSlave(com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getResetSlaveMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void showSlaveStatus(com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest request,
        io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> responseObserver) {
      asyncServerStreamingCall(
          getChannel().newCall(getShowSlaveStatusMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
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
    public java.util.Iterator<com.alibaba.polardbx.rpc.cdc.BinaryLog> showBinaryLogs(
        com.alibaba.polardbx.rpc.cdc.Request request) {
      return blockingServerStreamingCall(
          getChannel(), getShowBinaryLogsMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.polardbx.rpc.cdc.MasterStatus showMasterStatus(com.alibaba.polardbx.rpc.cdc.Request request) {
      return blockingUnaryCall(
          getChannel(), getShowMasterStatusMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.polardbx.rpc.cdc.BinlogEvent> showBinlogEvents(
        com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getShowBinlogEventsMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.polardbx.rpc.cdc.DumpStream> dump(
        com.alibaba.polardbx.rpc.cdc.DumpRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getDumpMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.polardbx.rpc.cdc.DumpStream> sync(
        com.alibaba.polardbx.rpc.cdc.DumpRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getSyncMethod(), getCallOptions(), request);
    }

    /**
     * <pre>
     *&#47;/////////////////////////// replicate   ///////////////////////////
     * </pre>
     */
    public com.alibaba.polardbx.rpc.cdc.RplCommandResponse changeMaster(com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest request) {
      return blockingUnaryCall(
          getChannel(), getChangeMasterMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.polardbx.rpc.cdc.RplCommandResponse changeReplicationFilter(com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest request) {
      return blockingUnaryCall(
          getChannel(), getChangeReplicationFilterMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.polardbx.rpc.cdc.RplCommandResponse startSlave(com.alibaba.polardbx.rpc.cdc.StartSlaveRequest request) {
      return blockingUnaryCall(
          getChannel(), getStartSlaveMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.polardbx.rpc.cdc.RplCommandResponse stopSlave(com.alibaba.polardbx.rpc.cdc.StopSlaveRequest request) {
      return blockingUnaryCall(
          getChannel(), getStopSlaveMethod(), getCallOptions(), request);
    }

    /**
     */
    public com.alibaba.polardbx.rpc.cdc.RplCommandResponse resetSlave(com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest request) {
      return blockingUnaryCall(
          getChannel(), getResetSlaveMethod(), getCallOptions(), request);
    }

    /**
     */
    public java.util.Iterator<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse> showSlaveStatus(
        com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest request) {
      return blockingServerStreamingCall(
          getChannel(), getShowSlaveStatusMethod(), getCallOptions(), request);
    }
  }

  /**
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
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.MasterStatus> showMasterStatus(
        com.alibaba.polardbx.rpc.cdc.Request request) {
      return futureUnaryCall(
          getChannel().newCall(getShowMasterStatusMethod(), getCallOptions()), request);
    }

    /**
     * <pre>
     *&#47;/////////////////////////// replicate   ///////////////////////////
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> changeMaster(
        com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getChangeMasterMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> changeReplicationFilter(
        com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getChangeReplicationFilterMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> startSlave(
        com.alibaba.polardbx.rpc.cdc.StartSlaveRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getStartSlaveMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> stopSlave(
        com.alibaba.polardbx.rpc.cdc.StopSlaveRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getStopSlaveMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.alibaba.polardbx.rpc.cdc.RplCommandResponse> resetSlave(
        com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest request) {
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
          serviceImpl.showBinaryLogs((com.alibaba.polardbx.rpc.cdc.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinaryLog>) responseObserver);
          break;
        case METHODID_SHOW_MASTER_STATUS:
          serviceImpl.showMasterStatus((com.alibaba.polardbx.rpc.cdc.Request) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.MasterStatus>) responseObserver);
          break;
        case METHODID_SHOW_BINLOG_EVENTS:
          serviceImpl.showBinlogEvents((com.alibaba.polardbx.rpc.cdc.ShowBinlogEventsRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.BinlogEvent>) responseObserver);
          break;
        case METHODID_DUMP:
          serviceImpl.dump((com.alibaba.polardbx.rpc.cdc.DumpRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream>) responseObserver);
          break;
        case METHODID_SYNC:
          serviceImpl.sync((com.alibaba.polardbx.rpc.cdc.DumpRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.DumpStream>) responseObserver);
          break;
        case METHODID_CHANGE_MASTER:
          serviceImpl.changeMaster((com.alibaba.polardbx.rpc.cdc.ChangeMasterRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse>) responseObserver);
          break;
        case METHODID_CHANGE_REPLICATION_FILTER:
          serviceImpl.changeReplicationFilter((com.alibaba.polardbx.rpc.cdc.ChangeReplicationFilterRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse>) responseObserver);
          break;
        case METHODID_START_SLAVE:
          serviceImpl.startSlave((com.alibaba.polardbx.rpc.cdc.StartSlaveRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse>) responseObserver);
          break;
        case METHODID_STOP_SLAVE:
          serviceImpl.stopSlave((com.alibaba.polardbx.rpc.cdc.StopSlaveRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse>) responseObserver);
          break;
        case METHODID_RESET_SLAVE:
          serviceImpl.resetSlave((com.alibaba.polardbx.rpc.cdc.ResetSlaveRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.RplCommandResponse>) responseObserver);
          break;
        case METHODID_SHOW_SLAVE_STATUS:
          serviceImpl.showSlaveStatus((com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusRequest) request,
              (io.grpc.stub.StreamObserver<com.alibaba.polardbx.rpc.cdc.ShowSlaveStatusResponse>) responseObserver);
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
    CdcServiceBaseDescriptorSupplier() {}

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
    CdcServiceFileDescriptorSupplier() {}
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
