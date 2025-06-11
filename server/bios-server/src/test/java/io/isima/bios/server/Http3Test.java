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

import io.isima.bios.configuration.Bios2Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.incubator.codec.http3.DefaultHttp3DataFrame;
import io.netty.incubator.codec.http3.DefaultHttp3HeadersFrame;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3DataFrame;
import io.netty.incubator.codec.http3.Http3HeadersFrame;
import io.netty.incubator.codec.http3.Http3RequestStreamInboundHandler;
import io.netty.incubator.codec.http3.Http3ServerConnectionHandler;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.KeyManagerFactory;

public class Http3Test {
  private static final byte[] CONTENT = "Hello World!\r\n".getBytes(CharsetUtil.US_ASCII);
  static final int PORT = 9999;

  private Http3Test() {}

  public static void main(String... args) throws Exception {
    int port;
    // Allow to pass in the port so we can also use it to run h3spec against
    if (args.length == 1) {
      port = Integer.parseInt(args[0]);
    } else {
      port = PORT;
    }
    NioEventLoopGroup group = new NioEventLoopGroup(1);

    final String password = Bios2Config.sslKeyStorePassword();
    final String keystoreType = Bios2Config.sslKeyStoreType();

    final File keyStoreFile = new File("/home/naoki/workspace/tfos/tools/certs/mydomain.p12");
    InputStream inputStream = new FileInputStream(keyStoreFile);
    final var keystore = KeyStore.getInstance(keystoreType);
    keystore.load(inputStream, password.toCharArray());
    final var kmf = KeyManagerFactory.getInstance("SunX509");
    kmf.init(keystore, password.toCharArray());

    final var sslContext =
        QuicSslContextBuilder.forServer(kmf, password)
            .applicationProtocols(Http3.supportedApplicationProtocols())
            .build();
    ChannelHandler codec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(5000, TimeUnit.MILLISECONDS)
            .initialMaxData(10000000)
            .initialMaxStreamDataBidirectionalLocal(1000000)
            .initialMaxStreamDataBidirectionalRemote(1000000)
            .initialMaxStreamsBidirectional(100)
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    // Called for each connection
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  // Called for each request-stream,
                                  @Override
                                  protected void initChannel(QuicStreamChannel ch) {
                                    ch.pipeline()
                                        .addLast(
                                            new Http3RequestStreamInboundHandler() {

                                              @Override
                                              protected void channelRead(
                                                  ChannelHandlerContext ctx,
                                                  Http3HeadersFrame frame) {
                                                ReferenceCountUtil.release(frame);
                                              }

                                              @Override
                                              protected void channelRead(
                                                  ChannelHandlerContext ctx, Http3DataFrame frame) {
                                                ReferenceCountUtil.release(frame);
                                              }

                                              @Override
                                              protected void channelInputClosed(
                                                  ChannelHandlerContext ctx) {
                                                Http3HeadersFrame headersFrame =
                                                    new DefaultHttp3HeadersFrame();
                                                headersFrame.headers().status("404");
                                                headersFrame.headers().add("server", "netty");
                                                headersFrame
                                                    .headers()
                                                    .addInt("content-length", CONTENT.length);
                                                ctx.write(headersFrame);
                                                ctx.writeAndFlush(
                                                        new DefaultHttp3DataFrame(
                                                            Unpooled.wrappedBuffer(CONTENT)))
                                                    .addListener(QuicStreamChannel.SHUTDOWN_OUTPUT);
                                              }
                                            });
                                  }
                                }));
                  }
                })
            .build();
    try {
      Bootstrap bs = new Bootstrap();
      Channel channel =
          bs.group(group)
              .channel(NioDatagramChannel.class)
              .handler(codec)
              .bind(new InetSocketAddress(port))
              .sync()
              .channel();
      channel.closeFuture().sync();
    } finally {
      group.shutdownGracefully();
    }
  }
}
