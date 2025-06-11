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
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3Headers;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3Headers;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3HeadersValidationException;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.incubator.codec.quic.QuicException;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.EventExecutorGroup;
import java.time.LocalDateTime;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Http3FrameHandler extends Http3RequestStreamInboundHandler {
  private static final Logger logger = LoggerFactory.getLogger(Http3FrameHandler.class);

  private final ServiceRouter serviceRouter;
  private final ServiceStatus serviceStatus;
  private final long streamId;
  private RequestStream<?, ?> requestStream;
  private final EventExecutor workerExecutor;

  public Http3FrameHandler(
      QuicStreamChannel ch,
      ServiceRouter serviceRouter,
      ServiceStatus serviceStatus,
      EventExecutorGroup handlerGroup) {
    streamId = ch.streamId();
    this.serviceRouter = serviceRouter;
    this.serviceStatus = serviceStatus;
    this.workerExecutor = handlerGroup.next();
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3HeadersFrame frame) throws Exception {
    final Http3Headers headers = frame.headers();
    requestStream = new Http3RequestStream(ctx, headers);
    ReferenceCountUtil.release(frame);
  }

  @Override
  protected void channelRead(ChannelHandlerContext ctx, Http3DataFrame frame) throws Exception {
    final long id = ((QuicStreamChannel) ctx.channel()).streamId();
    requestStream.addContent(frame.content().retain());
    ReferenceCountUtil.release(frame);
  }

  @Override
  protected void channelInputClosed(ChannelHandlerContext ctx) throws Exception {
    dispatch(ctx);
  }

  @Override
  protected void handleQuicException(
      @SuppressWarnings("unused") ChannelHandlerContext ctx, QuicException exception) {
    logger.error("Caught QuicException on channel {}", ctx.channel(), exception);
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (cause instanceof Http3HeadersValidationException) {
      logger.warn("HTTP/3 header validation error: {}", cause.getMessage());
    } else {
      logger.error("Unexpected channel error encountered", cause);
    }
    ctx.close();
  }

  private void dispatch(ChannelHandlerContext ctx) {
    if (requestStream == null) {
      return;
    }

    workerExecutor.execute(
        () -> {
          try {
            // Parse the request
            serviceRouter.resolveHandler(requestStream);
          } catch (BiosServerException e) {
            // We defer the error handling in a later stage. This dispatcher is called by a frame
            // handler method where they may still touch the context and stream resources after this
            // method, although they shouldn't do it ideally. Following error handling would
            // terminate
            // the stream and the context, we do it after making sure the frame handler does not
            // touch
            // them anymore.
            workerExecutor.execute(
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
            ctx.executor()
                .execute(
                    () -> {
                      requestStream.handleError(
                          new TfosException(GenericError.SERVER_IN_MAINTENANCE));
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
                    workerExecutor);
          } catch (Throwable t) {
            final var cause = (t instanceof CompletionException) ? t.getCause() : t;
            ctx.executor()
                .execute(
                    () -> {
                      requestStream.handleError(cause);
                      requestStream.releaseContent();
                      serviceStatus.getInFlightOperations().remove(requestStream);
                    });
          }
        });
  }

  private class Http3RequestStream extends RequestStream<Http3Headers, Long> {
    private final ChannelHandlerContext context;

    public Http3RequestStream(ChannelHandlerContext context, Http3Headers requestHeaders) {
      super(
          workerExecutor,
          requestHeaders,
          new DefaultHttp3Headers(),
          context.channel().remoteAddress());
      this.context = context;
    }

    @Override
    public Long getId() {
      return streamId;
    }

    @Override
    protected <T> void sendResponse(T content, MediaType contentType) throws BiosServerException {
      responseHeaders.add(HttpHeaderNames.DATE, Http2Utils.toRfc1123Date(LocalDateTime.now()));
      responseHeaders.add(Keywords.X_CONTENT_TYPE_OPTIONS, "nosniff");
      if (contentType == MediaType.NONE || content == null) {
        responseHeaders.addInt(HttpHeaderNames.CONTENT_LENGTH, 0);
        metricsTracer.doneEncoding();
        final var headersFrame = new DefaultHttp3HeadersFrame(responseHeaders);
        context
            .executor()
            .execute(
                () -> {
                  context
                      .writeAndFlush(headersFrame)
                      .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT)
                      .addListener((future) -> metricsTracer.stop(0));
                });
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
        context
            .executor()
            .execute(
                () -> {
                  context.write(new DefaultHttp3HeadersFrame(responseHeaders));
                  context
                      .writeAndFlush(new DefaultHttp3DataFrame(payload))
                      .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT)
                      .addListener(
                          (future) -> {
                            payload.release();
                            if (payload.refCnt() > 0) {
                              payload.release();
                            }
                            metricsTracer.stop(replyContentLength);
                          });
                });
      }
    }
  }
}
