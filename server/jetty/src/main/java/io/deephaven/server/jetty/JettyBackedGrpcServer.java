//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.server.browserstreaming.BrowserStreamInterceptor;
import io.deephaven.server.resources.ServerResources;
import io.deephaven.server.runner.GrpcServer;
import io.deephaven.ssl.config.CiphersIntermediate;
import io.deephaven.ssl.config.ProtocolsIntermediate;
import io.deephaven.ssl.config.SSLConfig;
import io.deephaven.ssl.config.TrustJdk;
import io.deephaven.ssl.config.impl.KickstartUtils;
import io.grpc.InternalStatus;
import io.grpc.internal.GrpcUtil;
import io.grpc.servlet.jakarta.web.GrpcWebFilter;
import io.grpc.servlet.web.websocket.GrpcWebsocket;
import io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream;
import io.grpc.servlet.web.websocket.WebSocketServerStream;
import jakarta.servlet.DispatcherType;
import jakarta.websocket.Endpoint;
import jakarta.websocket.server.ServerEndpointConfig;
import nl.altindag.ssl.SSLFactory;
import nl.altindag.ssl.jetty.util.JettySslUtils;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.eclipse.jetty.alpn.server.ALPNServerConnectionFactory;
import org.eclipse.jetty.ee10.servlet.DefaultServlet;
import org.eclipse.jetty.ee10.servlet.ErrorPageErrorHandler;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.security.ConstraintSecurityHandler;
import org.eclipse.jetty.ee10.webapp.WebAppContext;
import org.eclipse.jetty.ee10.websocket.jakarta.common.SessionTracker;
import org.eclipse.jetty.ee10.websocket.jakarta.server.JakartaWebSocketServerContainer;
import org.eclipse.jetty.ee10.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.http2.HTTP2Connection;
import org.eclipse.jetty.http2.HTTP2Session;
import org.eclipse.jetty.http2.RateControl;
import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory;
import org.eclipse.jetty.http2.server.HTTP2ServerConnectionFactory;
import org.eclipse.jetty.http2.server.internal.HTTP2ServerConnection;
import org.eclipse.jetty.io.Connection;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.server.handler.CrossOriginHandler;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.ExceptionUtil;
import org.eclipse.jetty.util.component.Graceful;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.resource.ResourceFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.jetbrains.annotations.NotNull;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.grpc.servlet.web.websocket.MultiplexedWebSocketServerStream.GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL;
import static io.grpc.servlet.web.websocket.WebSocketServerStream.GRPC_WEBSOCKETS_PROTOCOL;
import static org.eclipse.jetty.ee10.servlet.ServletContextHandler.NO_SESSIONS;

@Singleton
public class JettyBackedGrpcServer implements GrpcServer {
    private static final String JS_PLUGINS_PATH_SPEC = "/" + JsPlugins.JS_PLUGINS + "/*";

    private final WebAppContext context;
    private final Server jetty;
    private final JsPlugins jsPlugins;
    private final ScheduledExecutorService executorService;
    private final boolean websocketsEnabled;

    @Inject
    public JettyBackedGrpcServer(
            final JettyConfig config,
            final GrpcFilter filter,
            final JsPlugins jsPlugins,
            @Named("grpc.server") final ScheduledExecutorService executorService) {
        this.jsPlugins = jsPlugins;
        jetty = new Server();
        jetty.addConnector(createConnector(jetty, config));
        this.executorService = executorService;

        context = new WebAppContext("/", null, null, null, new ErrorPageErrorHandler(), NO_SESSIONS);
        context.setInitParameter(DefaultServlet.CONTEXT_INIT + "dirAllowed", "false");
        context.setInitParameter(DefaultServlet.CONTEXT_INIT + "etags", "true");

        // Cache all of the appropriate assets folders
        for (String appRoot : List.of("/ide/", "/iframe/table/", "/iframe/chart/", "/iframe/widget/")) {
            context.addFilter(NoCacheFilter.class, appRoot + "*", EnumSet.noneOf(DispatcherType.class));
            context.addFilter(CacheFilter.class, appRoot + "assets/*", EnumSet.noneOf(DispatcherType.class));
        }
        context.addFilter(NoCacheFilter.class, "/jsapi/*", EnumSet.noneOf(DispatcherType.class));
        context.addFilter(DropIfModifiedSinceHeader.class, "/*", EnumSet.noneOf(DispatcherType.class));
        // TODO(deephaven-core#4620): Add js-plugins version-aware caching
        context.addFilter(NoCacheFilter.class, JS_PLUGINS_PATH_SPEC, EnumSet.noneOf(DispatcherType.class));

        context.setSecurityHandler(new ConstraintSecurityHandler());

        // Add an extra filter to redirect from / to /ide/
        context.addFilter(HomeFilter.class, "/", EnumSet.noneOf(DispatcherType.class));

        // Handle grpc-web connections, translate to vanilla grpc
        context.addFilter(new FilterHolder(new GrpcWebFilter()), "/*", EnumSet.noneOf(DispatcherType.class));

        // Wire up the provided grpc filter
        context.addFilter(new FilterHolder(filter), "/*", EnumSet.noneOf(DispatcherType.class));

        // Set up websockets for grpc-web - depending on configuration, we can register both in case we encounter a
        // client using "vanilla"
        // grpc-websocket, that can't multiplex all streams on a single socket
        if (config.websocketsOrDefault() != JettyConfig.WebsocketsSupport.NONE) {
            JakartaWebSocketServletContainerInitializer.configure(context, (servletContext, container) -> {
                final Map<String, Supplier<Endpoint>> endpoints = new HashMap<>();
                if (config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.BOTH
                        || config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.GRPC_WEBSOCKET) {
                    endpoints.put(GRPC_WEBSOCKETS_PROTOCOL, () -> filter.create(WebSocketServerStream::new));
                }
                if (config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.BOTH
                        || config.websocketsOrDefault() == JettyConfig.WebsocketsSupport.GRPC_WEBSOCKET_MULTIPLEXED) {
                    endpoints.put(GRPC_WEBSOCKETS_MULTIPLEX_PROTOCOL,
                            () -> filter.create(MultiplexedWebSocketServerStream::new));
                }
                JakartaWebSocketServerContainer jettyWebsocketContainer = (JakartaWebSocketServerContainer) container;
                WebsocketFactory websocketFactory =
                        new WebsocketFactory(() -> new GrpcWebsocket(endpoints), jettyWebsocketContainer);
                jettyWebsocketContainer.addBean(websocketFactory);
                container.addEndpoint(ServerEndpointConfig.Builder.create(GrpcWebsocket.class, "/{service}/{method}")
                        .configurator(new ServerEndpointConfig.Configurator() {
                            @Override
                            public <T> T getEndpointInstance(Class<T> endpointClass) {
                                // noinspection unchecked
                                return (T) websocketFactory.create();
                            }
                        })
                        .subprotocols(new ArrayList<>(endpoints.keySet()))
                        .build());
            });
            this.websocketsEnabled = true;
        } else {
            this.websocketsEnabled = false;
        }

        final CrossOriginHandler corsHandler = createCrossOriginHandler(context);

        // Optionally wrap the webapp in a gzip handler
        final Handler handler;
        if (config.httpCompressionOrDefault()) {
            handler = createGzipHandler(corsHandler);
        } else {
            handler = corsHandler;
        }

        jetty.setHandler(handler);
    }

    /**
     * Initialize the resources for the server, including the js-plugins filesystem. We don't do this in the constructor
     * so that we can defer resources being opened until after JS plugins have had a chance to be registered.
     */
    private void initResources() {
        List<String> urls = ServerResources.resourcesFromServiceLoader(Configuration.getInstance());

        // Build resources List
        ArrayList<Resource> resources = new ArrayList<>(urls.stream().map(url -> {
            Resource resource = context.getResourceFactory().newResource(url);
            Require.neqNull(resource, "newResource(" + url + ")");
            return resource;
        }).toList());

        // Note that creating the jsPlugins resource will open the backing .zip file. Attempting to register any JS
        // plugins after this point will fail since they need to write to the filesystem. The general order of setup
        // should be 1) Construct the server. 2) Register JS plugins. 3) Create the JS plugins resource. 4) Start the
        // server. This is managed by DeephavenApiServer.
        Resource jsPluginsResource = context.getResourceFactory().newResource(jsPlugins.filesystem());
        resources.add(new PathPrefixResource("/js-plugins/", jsPluginsResource));

        Resource combinedResource = ResourceFactory.combine(resources);
        context.setBaseResource(combinedResource);

        // Create a HttpContent.Factory that can control caching (e.g. generating strong ETag headers) for static
        // resources.
        HttpContent.Factory controlledCacheHttpContentFactory = ControlledCacheHttpContentFactory.create(
                combinedResource,
                jetty.getByteBufferPool(),
                jetty.getMimeTypes());

        // Setting this attribute will override the default HttpContent.Factory created by the ResourceServlet.
        context.setAttribute(HttpContent.Factory.class.getName(), controlledCacheHttpContentFactory);
    }

    @Override
    public void start() throws IOException {
        try {
            initResources();
            jetty.start();
        } catch (RuntimeException exception) {
            throw exception;
        } catch (Exception exception) {
            throw new IOException(exception);
        }
    }

    @Override
    public void join() throws InterruptedException {
        jetty.join();
    }

    @Override
    public void beginShutdown() {
        // "start to stop" the jetty container, skipping over websockets, since their Graceful implementation isn't
        // very nice. This is roughly the implementation of Graceful.shutdown(Component), except avoiding anything that
        // would directly stop a websocket, which instead will be handled later, as part of the actual stop() call tell
        // the graceful handlers that we are shutting down.

        // For websockets, since the SessionTracker will instantly stop the socket rather than allow it to finish
        // nicely. Instead, when websockets were created, we registered extra graceful beans to shutdown like h2.
        // See Server.doStop(), this is roughly the implementation of the first phase of that method, only asking
        // Graceful instances to stop, but not stopping connectors or non-graceful components.

        // Note that this would not apply correctly if we used WebSockets for some purpose other than gRPC transport.
        Collection<Graceful> gracefuls = jetty.getContainedBeans(Graceful.class);
        gracefuls.stream().filter(g -> !(g instanceof SessionTracker)).forEach(Graceful::shutdown);
        executorService.shutdown();
    }

    @Override
    public void stopWithTimeout(long timeout, TimeUnit unit) {
        Thread shutdownThread = new Thread(() -> {
            ExceptionUtil.MultiException exceptions = new ExceptionUtil.MultiException();
            long millis = unit.toMillis(timeout);

            // If websockets are enabled, try to spend part of our shutdown timeout budget on waiting for websockets, as
            // in beginShutdown.
            if (websocketsEnabled && millis > 250) {
                // shut down everything except the websockets themselves with half our timeout
                millis /= 2;

                // Collect the same beans we gracefully stopped before (or, if we didn't already start a graceful
                // shutdown, this is the first attempt)
                Collection<Graceful> gracefuls = jetty.getContainedBeans(Graceful.class);
                try {
                    CompletableFuture.allOf(gracefuls.stream().filter(g -> !(g instanceof SessionTracker))
                            .map(Graceful::shutdown).toArray(CompletableFuture[]::new))
                            .get(millis, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }

            // regardless of failures so far, continue shutdown with remaining budget. This will end all websockets
            // right away.
            try {
                jetty.setStopTimeout(millis);
                jetty.stop();
                shutdownAndAwaitTermination(executorService);
                exceptions.ifExceptionThrow();
            } catch (Exception exception) {
                exceptions.add(exception);
            }
            exceptions.ifExceptionThrowRuntime();
        });
        shutdownThread.start();
    }

    private static void shutdownAndAwaitTermination(ScheduledExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ex) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int getPort() {
        return ((ServerConnector) jetty.getConnectors()[0]).getLocalPort();
    }

    private static ServerConnector createConnector(Server server, JettyConfig config) {
        // https://www.eclipse.org/jetty/documentation/jetty-11/programming-guide/index.html#pg-server-http-connector-protocol-http2-tls
        final HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.addCustomizer(new ForwardedRequestCustomizer());
        httpConfig.addCustomizer(new AllowedHttpMethodsCustomizer(config.allowedHttpMethods()));
        if (!config.extraHeaders().isEmpty()) {
            httpConfig.addCustomizer(new ConfiguredHeadersCustomizer(config.extraHeaders()));
        }
        final HttpConnectionFactory http11 = config.http1OrDefault() ? new HttpConnectionFactory(httpConfig) : null;
        final ServerConnector serverConnector;
        if (config.ssl().isPresent()) {
            httpConfig.addCustomizer(new SecureRequestCustomizer(config.sniHostCheck()));
            final HTTP2ServerConnectionFactory h2 = new HTTP2ServerConnectionFactory(httpConfig);
            h2.setRateControlFactory(new RateControl.Factory() {});
            config.maxConcurrentStreams().ifPresent(h2::setMaxConcurrentStreams);

            final ALPNServerConnectionFactory alpn = new ALPNServerConnectionFactory();
            alpn.setDefaultProtocol(http11 != null ? http11.getProtocol() : h2.getProtocol());
            // The Jetty server is getting intermediate setup by default if none are configured. This is most similar to
            // how the Netty servers gets setup by default via GrpcSslContexts.
            final SSLConfig sslConfig = config.ssl().get()
                    .orTrust(TrustJdk.of())
                    .orProtocols(ProtocolsIntermediate.of())
                    .orCiphers(CiphersIntermediate.of());
            final SSLFactory kickstart = KickstartUtils.create(sslConfig);
            final SslContextFactory.Server jetty = JettySslUtils.forServer(kickstart);
            final SslConnectionFactory tls = new SslConnectionFactory(jetty, alpn.getProtocol());
            if (http11 != null) {
                serverConnector = new ServerConnector(server, tls, alpn, h2, http11);
            } else {
                serverConnector = new ServerConnector(server, tls, alpn, h2);
            }
        } else {
            final HTTP2CServerConnectionFactory h2c = new HTTP2CServerConnectionFactory(httpConfig);
            h2c.setRateControlFactory(new RateControl.Factory() {});
            config.maxConcurrentStreams().ifPresent(h2c::setMaxConcurrentStreams);

            if (http11 != null) {
                serverConnector = new ServerConnector(server, http11, h2c);
            } else {
                serverConnector = new ServerConnector(server, h2c);
            }
        }
        config.host().ifPresent(serverConnector::setHost);
        serverConnector.setPort(config.port());
        config.maxHeaderRequestSize().ifPresent(httpConfig::setRequestHeaderSize);

        // Give connections extra time to shutdown, since we have an explicit server shutdown
        serverConnector.setShutdownIdleTimeout(serverConnector.getIdleTimeout());

        // Override the h2 stream timeout with a specified value
        serverConnector.addEventListener(new Connection.Listener() {
            @Override
            public void onOpened(Connection connection) {
                if (connection instanceof HTTP2ServerConnection) {
                    HTTP2Session session = (HTTP2Session) ((HTTP2Connection) connection).getSession();
                    session.setStreamIdleTimeout(config.http2StreamIdleTimeoutOrDefault());
                }
            }

            @Override
            public void onClosed(Connection connection) {

            }
        });

        return serverConnector;
    }

    private static CrossOriginHandler createCrossOriginHandler(Handler handler) {
        // If requested, permit CORS requests
        CrossOriginHandler corsHandler = new CrossOriginHandler();
        // Permit all origins
        corsHandler.setAllowedOriginPatterns(Set.of("*"));

        // Only support POST - technically gRPC can use GET, but we don't use any of those methods
        corsHandler.setAllowedMethods(Set.of("POST"));

        // Required request headers for gRPC, gRPC-web, flight, and deephaven
        corsHandler.setAllowedHeaders(Set.of(
                // Required for CORS itself to work
                HttpHeader.ORIGIN.asString(),
                HttpHeader.ACCESS_CONTROL_ALLOW_ORIGIN.asString(),

                // Required for gRPC
                GrpcUtil.CONTENT_TYPE_KEY.name(),
                GrpcUtil.TIMEOUT_KEY.name(),

                // Optional for gRPC
                GrpcUtil.MESSAGE_ENCODING_KEY.name(),
                GrpcUtil.MESSAGE_ACCEPT_ENCODING_KEY.name(),
                GrpcUtil.CONTENT_ENCODING_KEY.name(),
                GrpcUtil.CONTENT_ACCEPT_ENCODING_KEY.name(),

                // Required for gRPC-web
                "x-grpc-web",
                // Optional for gRPC-web
                "x-user-agent",

                // Required for Flight auth 1/2
                AuthConstants.TOKEN_NAME,
                Auth2Constants.AUTHORIZATION_HEADER,

                // Required for DH gRPC browser bidi stream support
                BrowserStreamInterceptor.TICKET_HEADER_NAME,
                BrowserStreamInterceptor.SEQUENCE_HEADER_NAME,
                BrowserStreamInterceptor.HALF_CLOSE_HEADER_NAME));

        // Response headers that the browser will need to be able to decode
        corsHandler.setExposedHeaders(Set.of(
                Auth2Constants.AUTHORIZATION_HEADER,
                GrpcUtil.CONTENT_TYPE_KEY.name(),
                InternalStatus.CODE_KEY.name(),
                InternalStatus.MESSAGE_KEY.name(),
                // Not used (yet?), see io.grpc.protobuf.StatusProto
                "grpc-status-details-bin"));

        corsHandler.setHandler(handler);

        return corsHandler;
    }

    private static @NotNull GzipHandler createGzipHandler(Handler handler) {
        final GzipHandler gzipHandler = new GzipHandler();
        // The default of 32 bytes seems a bit small.
        gzipHandler.setMinGzipSize(1024);
        // The GzipHandler documentation says GET is the default, but the constructor shows both GET and POST.
        // This should ensure our gRPC messages don't get compressed for now, but we may need to be more explicit in
        // the future as gRPC can technically operate over GET.
        gzipHandler.setIncludedMethods(HttpMethod.GET.asString());
        // Otherwise, the other defaults seem reasonable.
        gzipHandler.setHandler(handler);
        return gzipHandler;
    }

}
