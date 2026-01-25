/**
 * Spring Boot auto-configuration for the Charks Outbox library.
 *
 * <p>This module provides automatic configuration for Spring Boot 4.x applications,
 * including:
 * <ul>
 *   <li>Configuration properties binding</li>
 *   <li>Conditional bean registration</li>
 *   <li>Health indicator integration</li>
 *   <li>Graceful shutdown support</li>
 * </ul>
 *
 * <h2>Quick Start</h2>
 * <p>Add the dependency:
 * <pre>
 * &lt;dependency&gt;
 *     &lt;groupId&gt;xyz.charks&lt;/groupId&gt;
 *     &lt;artifactId&gt;charks-outbox-spring-boot-starter&lt;/artifactId&gt;
 * &lt;/dependency&gt;
 * </pre>
 *
 * <h2>Configuration</h2>
 * <pre>
 * charks:
 *   outbox:
 *     enabled: true
 *     relay:
 *       poll-interval: 1s
 *       batch-size: 100
 *     kafka:
 *       bootstrap-servers: localhost:9092
 * </pre>
 *
 * @see xyz.charks.outbox.spring.OutboxAutoConfiguration
 * @see xyz.charks.outbox.spring.OutboxProperties
 */
@NullMarked
package xyz.charks.outbox.spring;

import org.jspecify.annotations.NullMarked;
