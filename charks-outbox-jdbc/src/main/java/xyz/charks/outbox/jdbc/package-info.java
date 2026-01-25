/**
 * JDBC-based implementation of the outbox repository.
 *
 * <p>This module provides a synchronous, JDBC-based implementation of
 * {@link xyz.charks.outbox.core.OutboxRepository} supporting multiple
 * SQL databases including PostgreSQL, MySQL, and H2.
 *
 * <p>Key classes:
 * <ul>
 *   <li>{@link xyz.charks.outbox.jdbc.JdbcOutboxRepository} - Main repository implementation</li>
 *   <li>{@link xyz.charks.outbox.jdbc.JdbcOutboxConfig} - Configuration options</li>
 *   <li>{@link xyz.charks.outbox.jdbc.SqlDialect} - Database-specific SQL generation</li>
 *   <li>{@link xyz.charks.outbox.jdbc.OutboxTableSchema} - DDL schema generator</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * DataSource dataSource = // obtain from connection pool
 * JdbcOutboxConfig config = JdbcOutboxConfig.builder()
 *     .tableName("outbox_events")
 *     .dialect(SqlDialect.POSTGRESQL)
 *     .build();
 *
 * OutboxRepository repository = new JdbcOutboxRepository(dataSource, config);
 * }</pre>
 *
 * <p>For schema creation:
 * <pre>{@code
 * OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
 * String ddl = schema.createTableStatement("outbox_events");
 * }</pre>
 *
 * @see xyz.charks.outbox.core.OutboxRepository
 */
@NullMarked
package xyz.charks.outbox.jdbc;

import org.jspecify.annotations.NullMarked;
