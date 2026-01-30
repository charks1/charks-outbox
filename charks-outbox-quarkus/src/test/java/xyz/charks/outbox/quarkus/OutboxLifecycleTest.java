package xyz.charks.outbox.quarkus;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import xyz.charks.outbox.relay.OutboxRelay;

import static org.mockito.Mockito.*;

@DisplayName("OutboxLifecycle")
class OutboxLifecycleTest {

    private OutboxLifecycle lifecycle;
    private Instance<OutboxRelay> relayInstance;
    private OutboxConfiguration config;
    private OutboxRelay relay;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        relayInstance = mock(Instance.class);
        config = mock(OutboxConfiguration.class);
        relay = mock(OutboxRelay.class);

        lifecycle = new OutboxLifecycle(relayInstance, config);
    }

    @Nested
    @DisplayName("onStart")
    class OnStartTest {

        @Test
        @DisplayName("skips startup when disabled")
        void skipsStartupWhenDisabled() {
            when(config.enabled()).thenReturn(false);

            lifecycle.onStart(new StartupEvent());

            verify(relayInstance, never()).get();
        }

        @Test
        @DisplayName("skips startup when relay not available")
        void skipsStartupWhenRelayNotAvailable() {
            when(config.enabled()).thenReturn(true);
            when(relayInstance.isUnsatisfied()).thenReturn(true);

            lifecycle.onStart(new StartupEvent());

            verify(relayInstance, never()).get();
        }

        @Test
        @DisplayName("starts relay when enabled and available")
        void startsRelayWhenEnabledAndAvailable() {
            when(config.enabled()).thenReturn(true);
            when(relayInstance.isUnsatisfied()).thenReturn(false);
            when(relayInstance.get()).thenReturn(relay);

            lifecycle.onStart(new StartupEvent());

            verify(relay).start();
        }

        @Test
        @DisplayName("handles null relay gracefully")
        void handlesNullRelayGracefully() {
            when(config.enabled()).thenReturn(true);
            when(relayInstance.isUnsatisfied()).thenReturn(false);
            when(relayInstance.get()).thenReturn(null);

            lifecycle.onStart(new StartupEvent());

            verify(relayInstance).get();
            verify(relay, never()).start();
        }
    }

    @Nested
    @DisplayName("onStop")
    class OnStopTest {

        @Test
        @DisplayName("skips shutdown when relay not available")
        void skipsShutdownWhenRelayNotAvailable() {
            when(relayInstance.isUnsatisfied()).thenReturn(true);

            lifecycle.onStop(new ShutdownEvent());

            verify(relayInstance, never()).get();
        }

        @Test
        @DisplayName("stops relay when available")
        void stopsRelayWhenAvailable() {
            when(relayInstance.isUnsatisfied()).thenReturn(false);
            when(relayInstance.get()).thenReturn(relay);

            lifecycle.onStop(new ShutdownEvent());

            verify(relay).stop();
        }

        @Test
        @DisplayName("handles null relay gracefully")
        void handlesNullRelayGracefully() {
            when(relayInstance.isUnsatisfied()).thenReturn(false);
            when(relayInstance.get()).thenReturn(null);

            lifecycle.onStop(new ShutdownEvent());

            verify(relayInstance).get();
            verify(relay, never()).stop();
        }
    }

}
