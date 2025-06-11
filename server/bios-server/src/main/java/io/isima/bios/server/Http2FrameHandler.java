/*
 * Copyright (C) 2025 Isima, Inc.
 *
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
package io.isima.bios.server;

import io.isima.bios.errors.GenericError;
import io.isima.bios.errors.exception.TfosException;
import io.isima.bios.exceptions.BiosServerException;
import io.isima.bios.maintenance.ServiceStatus;
import io.isima.bios.server.services.RequestStream;
import io.isima.bios.service.Keywords;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2WindowUpdateFrame;
import io.netty.handler.codec.http2.Http2ChannelDuplexHandler;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Exception;
import io.netty.handler.codec.http2.Http2FrameStream;
import io.netty.handler.codec.http2.Http2GoAwayFrame;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.util.AttributeKey;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.concurrent.CompletionException;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Http2FrameHandler extends Http2ChannelDuplexHandler {
  private static final Logger logger = LoggerFactory.getLogger(Http2FrameHandler.class);

  private static final int MAX_DATA_LENGTH_PER_FRAME = 32768;

  private static final AttributeKey<HashMap<Integer, RequestStream>> STREAMS =
      AttributeKey.valueOf("streams");

  private final ServiceRouter dispatcher;
  private final ServiceStatus serviceStatus;

  public Http2FrameHandler(ServiceRouter dispatcher, ServiceStatus serviceStatus) {
    this.dispatcher = dispatcher;
    this.serviceStatus = serviceStatus;
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    final var remoteAddress = ctx.channel().remoteAddress();
    if (cause instanceof Http2Exception
        && cause.getMessage().startsWith("Unexpected HTTP/1.x request:")) {
      logger.warn("HTTP2 error; remote={}, error={}", remoteAddress, cause.getMessage());
    } else {
      logger.warn("Channel error; remote={}, error={}", remoteAddress, cause.toString());
    }
    ctx.close();
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    if (msg instanceof Http2HeadersFrame) {
      handleHeadersFrame(ctx, (Http2HeadersFrame) msg);
    } else if (msg instanceof Http2DataFrame) {
      handleDataFrame(ctx, (Http2DataFrame) msg);
    } else if (msg instanceof Http2GoAwayFrame) {
      handleGoawayFrame(ctx, (Http2GoAwayFrame) msg);
    } else {
      super.channelRead(ctx, msg);
    }
  }

  private void handleHeadersFrame(ChannelHandlerContext ctx, Http2HeadersFrame headersFrame) {
    logger.debug(
        "Frame type={}, stream={}, isEndStream={}",
        headersFrame.name(),
        headersFrame.stream().id(),
        headersFrame.isEndStream());
    final var request = new Http2RequestStream(ctx, headersFrame.headers(), headersFrame.stream());
    if (headersFrame.isEndStream()) {
      dispatch(request);
      // NOTE: Don't access the context and stream beyond this point. Request handling has started
      return;
    }
    // keep track of the streams
    final var streams = ctx.channel().attr(STREAMS);
    if (streams.get() == null) {
      streams.set(new HashMap<>());
    }
    streams.get().put(request.getId(), request);
  }

  private void handleDataFrame(ChannelHandlerContext ctx, Http2DataFrame dataFrame) {
    int frameSize = dataFrame.content().readableBytes();
    logger.debug(
        "Frame type={}, stream={}, size={}, isEndStream={}",
        dataFrame.name(),
        dataFrame.stream().id(),
        frameSize,
        dataFrame.isEndStream());
    final var requestStreams = ctx.channel().attr(STREAMS).get();
    final var stream = requestStreams.get(dataFrame.stream().id());
    stream.addContent(dataFrame.content());
    ctx.write(new DefaultHttp2WindowUpdateFrame(frameSize).stream(dataFrame.stream()));
    if (dataFrame.isEndStream()) {
      requestStreams.remove(dataFrame.stream().id()); // no need to track the stream anymore
      dispatch(stream);
      // NOTE: Don't access the context and stream beyond this point. Request handling has started
    }
  }

  private void handleGoawayFrame(ChannelHandlerContext ctx, Http2GoAwayFrame msg) {
    logger.debug("Go away frame received");
  }

  private void dispatch(RequestStream<?, ?> requestStream) {
    final var executor = requestStream.getEventExecutor();

    // Parse the request
    try {
      dispatcher.resolveHandler(requestStream);
    } catch (BiosServerException e) {
      // We defer the error handling in a later stage. This dispatcher is called by a frame handler
      // method where they may still touch the context and stream resources after this method,
      // although they shouldn't do it ideally. Following error handling would terminate the stream
      // and the context, we do it after making sure the frame handler does not touch them anymore.
      executor.execute(
          () -> {
            logger.warn(
                "Server exception; method={}, path={}, error={}",
                requestStream.getMethod(),
                requestStream.getPath(),
                e.toString());
            requestStream.handleError(e);
            requestStream.releaseContent();
          });
      return;
    }

    if (serviceStatus.isInMaintenance()
        && !requestStream.getHandler().getOperationName().equals("ExitMaintenanceMode")) {
      executor.execute(
          () -> {
            requestStream.handleError(new TfosException(GenericError.SERVER_IN_MAINTENANCE));
            requestStream.releaseContent();
          });
      return;
    }

    serviceStatus.getInFlightOperations().add(requestStream);
    requestStream.start();
    try {
      requestStream
          .handleRequest(requestStream.supplyContent())
          .whenCompleteAsync(
              (result, t) -> {
                if (t == null) {
                  try {
                    requestStream.reply(result);
                  } catch (Throwable tt) {
                    t = tt;
                  }
                }
                if (t != null) {
                  final var cause = (t instanceof CompletionException) ? t.getCause() : t;
                  requestStream.handleError(cause);
                }
                requestStream.releaseContent();
                serviceStatus.getInFlightOperations().remove(requestStream);
              },
              executor);
    } catch (Throwable t) {
      final var cause = (t instanceof CompletionException) ? t.getCause() : t;
      executor.execute(
          () -> {
            requestStream.handleError(cause);
            requestStream.releaseContent();
            serviceStatus.getInFlightOperations().remove(requestStream);
          });
    }
  }

  private static class Http2RequestStream extends RequestStream<Http2Headers, Integer> {
    private final ChannelHandlerContext context;

    @Getter private final Http2FrameStream stream;

    public Http2RequestStream(
        ChannelHandlerContext context, Http2Headers requestHeaders, Http2FrameStream stream) {
      super(
          context.executor(),
          requestHeaders,
          new DefaultHttp2Headers(),
          context.channel().remoteAddress());
      this.stream = stream;
      this.context = context;
    }

    @Override
    public Integer getId() {
      return stream.id();
    }

    @Override
    protected <T> void sendResponse(T content, MediaType contentType) throws BiosServerException {
      responseHeaders.add(HttpHeaderNames.DATE, Http2Utils.toRfc1123Date(LocalDateTime.now()));
      responseHeaders.add(Keywords.X_CONTENT_TYPE_OPTIONS, "nosniff");
      if (contentType == MediaType.NONE || content == null) {
        responseHeaders.addInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        metricsTracer.doneEncoding();
        context
            .writeAndFlush(new DefaultHttp2HeadersFrame(responseHeaders, true).stream(stream))
            .addListener((future) -> metricsTracer.stop(0));
      } else {
        // encode content
        final var payload = context.alloc().buffer();
        payload.retain();
        contentType.payloadCodec().encode(content, payload);
        metricsTracer.doneEncoding();
        final int replyContentLength = payload.readableBytes();
        responseHeaders.addInt(HttpHeaderNames.CONTENT_LENGTH, replyContentLength);

        // reply
        if (replyContentLength > 0) {
          responseHeaders.add(HttpHeaderNames.CONTENT_TYPE, contentType.value());
        }
        int remaining = replyContentLength;
        var future =
            context.write(
                new DefaultHttp2HeadersFrame(responseHeaders, remaining == 0).stream(stream));
        int index = 0;
        // TODO(Naoki): Is this write loop safe?  The method context.write() is asynchronous and
        //  we do not wait for the completion. Would the frame order be preserved?
        // Also, is it fine to release the payload immediately after placing the write request?
        while (remaining > 0) {
          int size = Math.min(remaining, MAX_DATA_LENGTH_PER_FRAME);
          final var frameData = payload.retainedSlice(index, size);
          index += size;
          remaining -= size;
          future =
              context.write(new DefaultHttp2DataFrame(frameData, remaining == 0).stream(stream));
        }
        future.addListener(
            (f) -> {
              payload.release(2);
              metricsTracer.stop(replyContentLength);
            });
        context.flush();
      }
    }
  }
}
