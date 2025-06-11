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

import com.google.common.net.InetAddresses;
import io.isima.bios.common.BuildVersion;
import io.isima.bios.configuration.Bios2Config;
import io.isima.bios.exceptions.ApplicationException;
import io.isima.bios.execution.ExecutorManager;
import io.isima.bios.framework.BiosModules;
import io.isima.bios.framework.ExtensionServicesLoader;
import io.isima.bios.server.services.AdminBiosService;
import io.isima.bios.server.services.AuthBiosService;
import io.isima.bios.server.services.BiBiosService;
import io.isima.bios.server.services.BiosService;
import io.isima.bios.server.services.DataBiosService;
import io.isima.bios.server.services.MiscBiosService;
import io.isima.bios.server.services.SignupBiosService;
import io.isima.bios.server.services.SystemAdminBiosService;
import io.isima.bios.server.services.UserManagementBiosService;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.incubator.codec.http3.Http3;
import io.netty.incubator.codec.http3.Http3ServerConnectionHandler;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicChannel;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicStreamChannel;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.impl.StaticLoggerBinder;

/**
 * The BIOS2 server starting point.
 *
 * <p>In order to launch a server process, execute {@link BiosServer#main(String[])}. Prerequisites
 * to run the server are:
 *
 * <ul>
 *   <li>A Cassandra server must be started, it must listen on port 10109 (not default 9042)
 *   <li>Environment variable <code>BIOS_HOME</code> is set and points to a BIOS home directory
 * </ul>
 *
 * <p>You can also run the server in an integration test case. Put following code block in the
 * BeforeAll method:
 *
 * <pre>
 * server = BiosServer.instantiate();
 * serverChannelFuture = server.start();
 * </pre>
 *
 * <p>In order to shut down, do
 *
 * <pre>
 * server.shutdown();
 * serverChannelFuture.sync();
 * </pre>
 */
public class BiosServer {
  private static final Logger logger = LoggerFactory.getLogger(BiosServer.class);

  public static final String SERVER_NAME = "bios";
  public static final String BIOS_HOME = "BIOS_HOME";

  private static BiosServer instance;

  public static BiosServer getInstance() {
    return instance;
  }

  protected static BiosServer instantiate() {
    logger.info("biOS Version {}", BuildVersion.VERSION);

    // Start the logger
    final var binder = StaticLoggerBinder.getSingleton();
    logger.info("Logger is {}", binder.getLoggerFactory());
    System.out.println("Logger is " + binder.getLoggerFactory().toString());

    printJvmOptions();

    // Check home directory
    final var biosHome = System.getenv(BIOS_HOME);
    if (biosHome == null) {
      throw new RuntimeException("Required environment variable BIOS_HOME is not set");
    }
    final var biosHomePath = Paths.get(biosHome);
    final var biosConfPath = Paths.get(biosHomePath.toString(), "configuration/server.options");
    if (!Files.isRegularFile(biosConfPath)) {
      throw new RuntimeException(
          "Configuration file " + biosConfPath + " not found or not a regular file");
    }
    System.setProperty("user.dir", biosHome);

    // Prepare server properties
    final Properties properties = new Properties();
    try (final var configStream = new FileInputStream(biosConfPath.toFile())) {
      properties.load(configStream);
      for (String key : System.getProperties().stringPropertyNames()) {
        properties.setProperty(key, System.getProperty(key));
      }
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to open directory %s=%s", BIOS_HOME, biosConfPath), e);
    }

    logger.info("== Properties ===========================================");
    properties.keySet().stream()
        .sorted()
        .forEach((name) -> logger.info("  {}: {}", name, properties.get(name)));
    logger.info("== Properties ===========================================");

    // Create and configure the server
    try {
      instance = new BiosServer(properties);
      instance.configure();
    } catch (ApplicationException e) {
      throw new RuntimeException(e);
    }
    return instance;
  }

  private static void printJvmOptions() {
    RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();
    logger.info("== JVM Options ==========================================");
    runtimeMxBean.getInputArguments().stream().forEach((arg) -> logger.info("  {}", arg));
    logger.info("== JVM Options ==========================================");
    logger.info("");
  }

  private final Properties properties;
  private final List<BiosService> services;
  private MiscBiosService miscService;

  private InetAddress address;
  private int port;

  private Channel channel;
  private EventLoopGroup group;

  public BiosServer(Properties properties) {
    this.properties = properties;
    services = new ArrayList<>();
  }

  /**
   * Configure the server.
   *
   * @throws ApplicationException Thrown to indicate an unexpected error happened.
   */
  private void configure() throws ApplicationException {
    // Start the server components
    BiosModules.startModules(properties);
    BiosModules.getServiceStatus().enterMaintenanceMode();

    // Configure the listener
    address = InetAddresses.forString(Bios2Config.serverAddress());
    port = Bios2Config.serverPort();

    // Add services
    services.add(new AuthBiosService());
    services.add(new UserManagementBiosService());
    services.add(new AdminBiosService());
    services.add(new SystemAdminBiosService());
    services.add(new DataBiosService(BiosModules.getDataServiceHandler()));
    services.add(new BiBiosService());
    services.add(new SignupBiosService());
    miscService = new MiscBiosService();
    services.add(miscService);

    final var serviceExtensionsLoaderClassName = Bios2Config.getServiceExtensionsLoaderClassName();
    if (!serviceExtensionsLoaderClassName.isBlank()) {
      ClassLoader classLoader = BiosServer.class.getClassLoader();
      try {
        Class extensionsLoaderClass = classLoader.loadClass(serviceExtensionsLoaderClassName);
        logger.info("Service extensions loader: {}", extensionsLoaderClass.getName());
        final var ctor = extensionsLoaderClass.getConstructor();
        ((ExtensionServicesLoader) ctor.newInstance()).invoke(instance.services);
      } catch (ClassNotFoundException
          | NoSuchMethodException
          | InstantiationException
          | IllegalAccessException
          | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Bootstrap the server.
   *
   * @throws Exception For any errors during startup.
   */
  public ChannelFuture start() throws Exception {
    final SslContext sslCtx = Http2Utils.createSslContext();

    final var serviceRouter = new ServiceRouter();
    services.forEach((service) -> service.register(serviceRouter));
    if (logger.isDebugEnabled()) {
      final var servicePaths = serviceRouter.collectServices();
      logger.debug("Services:");
      servicePaths.forEach((service) -> logger.debug("  {}", service));
    }

    if (Bios2Config.isHttp3Enabled()) {
      start3(serviceRouter);
    }
    if (Bios2Config.serverPort2nd() > 0) {
      start2(serviceRouter);
    }
    final var manager = BiosModules.getExecutorManager();
    group = new NioEventLoopGroup(manager.getIoNumThreads(), manager.createIoThreadFactory());
    ServerBootstrap nettyBootstrap = new ServerBootstrap();
    nettyBootstrap
        .option(ChannelOption.SO_BACKLOG, 1024)
        .group(group)
        .channel(NioServerSocketChannel.class);
    if (Bios2Config.nioLoggingEnabled()) {
      nettyBootstrap.handler(new LoggingHandler(LogLevel.DEBUG));
    }
    nettyBootstrap.childHandler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            logger.info("Connection established; port={}, peer={}", port, ch.remoteAddress());
            if (sslCtx != null) {
              ch.pipeline()
                  .addLast(
                      sslCtx.newHandler(ch.alloc()),
                      Http2Utils.getHandler(serviceRouter, BiosModules.getServiceStatus()));
            }
          }
        });
    logger.info("Server bootstrap");
    channel = nettyBootstrap.bind(address, port).syncUninterruptibly().channel();
    logger.info("Removing failure mark if set");
    BiosModules.getServiceStatus().removeFailedEndpoint();
    logger.info("BIOS Server started listening on https://{}:{}/", address, port);
    BiosModules.getMaintenance()
        .runSingleTaskWithDelay(
            () -> {
              try {
                BiosModules.getServiceStatus().clearMaintenanceMode();
                BiosModules.getMaintenance().start();
              } catch (ApplicationException e) {
                logger.error("Failed to unset maintenance mode", e);
              }
            },
            5,
            TimeUnit.SECONDS);
    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
    return channel.closeFuture();
  }

  private void start2(ServiceRouter serviceRouter)
      throws UnrecoverableKeyException,
          CertificateException,
          IOException,
          KeyStoreException,
          NoSuchAlgorithmException {
    final SslContext sslCtx = Http2Utils.createSslContext2nd();
    final var manager = BiosModules.getExecutorManager();
    final var handlerGroup =
        new NioEventLoopGroup(
            manager.getIoNumThreads(),
            ExecutorManager.makeThreadFactory("io2-worker", Thread.MAX_PRIORITY - 1));
    final var secondPort = Bios2Config.serverPort2nd();
    ServerBootstrap nettyBootstrap = new ServerBootstrap();
    nettyBootstrap
        .option(ChannelOption.SO_BACKLOG, 1024)
        .group(handlerGroup)
        .channel(NioServerSocketChannel.class);
    if (Bios2Config.nioLoggingEnabled()) {
      nettyBootstrap.handler(new LoggingHandler(LogLevel.DEBUG));
    }
    nettyBootstrap.childHandler(
        new ChannelInitializer<SocketChannel>() {
          @Override
          protected void initChannel(SocketChannel ch) throws Exception {
            logger.info("Connection established; port={}, peer={}", secondPort, ch.remoteAddress());
            if (sslCtx != null) {
              ch.pipeline()
                  .addLast(
                      sslCtx.newHandler(ch.alloc()),
                      Http2Utils.getHandler(serviceRouter, BiosModules.getServiceStatus()));
            }
          }
        });
    logger.info("2nd server bootstrap");
    channel = nettyBootstrap.bind(address, secondPort).syncUninterruptibly().channel();
    logger.info("BIOS 2nd Server started listening on https://{}:{}/", address, secondPort);
  }

  public ChannelFuture start3(ServiceRouter serviceRouter) throws Exception {
    final QuicSslContext sslContext = Http3Utils.createSslContext();

    final var manager = BiosModules.getExecutorManager();
    final var handlerGroup =
        new NioEventLoopGroup(
            manager.getIoNumThreads(),
            ExecutorManager.makeThreadFactory("io-worker", Thread.MAX_PRIORITY - 1));
    ChannelHandler codec =
        Http3.newQuicServerCodecBuilder()
            .sslContext(sslContext)
            .maxIdleTimeout(Bios2Config.quicMaxIdleTimeoutMillis(), TimeUnit.MILLISECONDS)
            .initialMaxData(Bios2Config.quicInitialMaxData())
            .initialMaxStreamDataBidirectionalLocal(Bios2Config.quicInitialMaxStreamDataLocal())
            .initialMaxStreamDataBidirectionalRemote(Bios2Config.quicInitialMaxStreamDataRemote())
            .initialMaxStreamsBidirectional(Bios2Config.quicInitialMaxStreamsBidirectional())
            .activeMigration(false)
            .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
            .handler(
                new ChannelInitializer<QuicChannel>() {
                  @Override
                  protected void initChannel(QuicChannel ch) {
                    // Called for each connection
                    logger.debug(
                        "Established a new QUIC connection; peer={}", ch.remoteSocketAddress());
                    ch.pipeline()
                        .addLast(
                            new Http3ServerConnectionHandler(
                                new ChannelInitializer<QuicStreamChannel>() {
                                  // Called for each request-stream,
                                  @Override
                                  protected void initChannel(QuicStreamChannel ch) {
                                    ch.pipeline()
                                        .addLast(
                                            new Http3FrameHandler(
                                                ch,
                                                serviceRouter,
                                                BiosModules.getServiceStatus(),
                                                handlerGroup));
                                  }
                                }));
                  }
                })
            .build();

    final var group3 =
        new NioEventLoopGroup(manager.getIoNumThreads(), manager.createIoThreadFactory());
    Bootstrap nettyBootstrap = new Bootstrap();
    nettyBootstrap.group(group3).channel(NioDatagramChannel.class).handler(codec);
    logger.info("HTTP/3 Server bootstrap");
    int h3Port = Bios2Config.http3Port();
    final var channel3 = nettyBootstrap.bind(address, h3Port).syncUninterruptibly().channel();
    logger.info("BIOS HTTP/3 Server started listening on https://{}:{}/", address, h3Port);
    return channel3.closeFuture();
  }

  /** Shutdown the server gracefully. */
  public synchronized void shutdown() {
    logger.info("Shutting down");
    try {
      final var serviceStatus = BiosModules.getServiceStatus();
      serviceStatus.enterMaintenanceMode();
      serviceStatus.waitForInFlightOperationsComplete();
      if (channel != null) {
        channel.close().sync();
        channel = null;
      }
      if (group != null) {
        group.shutdownGracefully().sync();
      }
    } catch (InterruptedException e) {
      logger.error("Error during shutdown", e);
      Thread.currentThread().interrupt();
    }
  }

  public MiscBiosService getMiscService() {
    return miscService;
  }

  public static void main(String[] args) throws Exception {
    try {
      instantiate();
      if (instance == null) {
        logger.error("biOS2 Server failed to start, exiting");
        System.exit(1);
      }
      // instance.start3();
      instance.start().sync();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Throwable t) {
      logger.error("Initialization error", t);
    }
    BiosModules.shutdown();
    logger.info("biOS2 service shutdown completed.");
  }
}
