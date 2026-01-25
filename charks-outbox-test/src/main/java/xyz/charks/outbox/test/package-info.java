/**
 * Test utilities for the Charks Outbox library.
 *
 * <p>This module provides utilities for testing applications that use
 * the outbox pattern, including in-memory implementations and mocks.
 *
 * <h2>Key Classes</h2>
 * <ul>
 *   <li>{@link xyz.charks.outbox.test.InMemoryOutboxRepository} - Non-persistent repository for unit tests</li>
 *   <li>{@link xyz.charks.outbox.test.MockBrokerConnector} - Configurable mock for broker testing</li>
 * </ul>
 *
 * <h2>Usage Example</h2>
 * <pre>{@code
 * class MyServiceTest {
 *
 *     private InMemoryOutboxRepository repository;
 *     private MockBrokerConnector connector;
 *
 *     @BeforeEach
 *     void setup() {
 *         repository = new InMemoryOutboxRepository();
 *         connector = new MockBrokerConnector();
 *     }
 *
 *     @Test
 *     void shouldPublishEvent() {
 *         // Given
 *         MyService service = new MyService(repository, connector);
 *
 *         // When
 *         service.processOrder(order);
 *
 *         // Then
 *         assertThat(repository.size()).isEqualTo(1);
 *         assertThat(connector.publishCount()).isEqualTo(1);
 *     }
 *
 *     @Test
 *     void shouldHandlePublishFailure() {
 *         // Given
 *         connector.failNextPublish("Broker unavailable");
 *
 *         // When/Then
 *         // ... test failure handling
 *     }
 * }
 * }</pre>
 *
 * @see xyz.charks.outbox.test.InMemoryOutboxRepository
 * @see xyz.charks.outbox.test.MockBrokerConnector
 */
@NullMarked
package xyz.charks.outbox.test;

import org.jspecify.annotations.NullMarked;
