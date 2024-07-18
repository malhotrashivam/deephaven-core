//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.netty;

import dagger.Component;
import dagger.Module;
import dagger.Provides;
import io.deephaven.engine.table.impl.ForkJoinPoolOperationInitializer;
import io.deephaven.engine.updategraph.OperationInitializer;
import io.deephaven.server.runner.ExecutionContextUnitTestModule;
import io.deephaven.server.test.FlightMessageRoundTripTest;

import javax.inject.Named;
import javax.inject.Singleton;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class NettyFlightRoundTripTest extends FlightMessageRoundTripTest {

    @Module
    public interface NettyTestConfig {
        @Provides
        static NettyConfig providesNettyConfig() {
            return NettyConfig.builder()
                    .port(0)
                    .tokenExpire(Duration.of(5, ChronoUnit.MINUTES))
                    .build();
        }

        @Provides
        @Singleton
        @Named(OperationInitializer.FORK_JOIN_NAME)
        static OperationInitializer provideForkJoinPoolOperationInitializer() {
            return ForkJoinPoolOperationInitializer.fromCommonPool();
        }
    }

    @Singleton
    @Component(modules = {
            ExecutionContextUnitTestModule.class,
            FlightTestModule.class,
            NettyServerModule.class,
            NettyTestConfig.class,
    })
    public interface NettyTestComponent extends TestComponent {
    }

    @Override
    protected TestComponent component() {
        return DaggerNettyFlightRoundTripTest_NettyTestComponent.create();
    }
}
