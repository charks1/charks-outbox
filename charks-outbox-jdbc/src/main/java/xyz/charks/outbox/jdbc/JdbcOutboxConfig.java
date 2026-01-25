package xyz.charks.outbox.jdbc;

import java.util.Objects;

/**
 * Configuration for the JDBC-based outbox repository.
 *
 * <p>Example usage:
 * <pre>{@code
 * JdbcOutboxConfig config = JdbcOutboxConfig.builder()
 *     .tableName("outbox_events")
 *     .dialect(SqlDialect.POSTGRESQL)
 *     .build();
 * }</pre>
 *
 * <p>Default values:
 * <ul>
 *   <li>tableName: "outbox_events"</li>
 *   <li>dialect: POSTGRESQL</li>
 * </ul>
 *
 * @param tableName the name of the outbox table in the database
 * @param dialect the SQL dialect for generating database-specific queries
 */
public record JdbcOutboxConfig(
        String tableName,
        SqlDialect dialect
) {

    /**
     * Default table name used when not explicitly configured.
     */
    public static final String DEFAULT_TABLE_NAME = "outbox_events";

    /**
     * Creates a JdbcOutboxConfig with validation.
     *
     * @throws NullPointerException if tableName or dialect is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public JdbcOutboxConfig {
        Objects.requireNonNull(tableName, "Table name cannot be null");
        Objects.requireNonNull(dialect, "Dialect cannot be null");
        if (tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be blank");
        }
    }

    /**
     * Creates a configuration with default values.
     *
     * @return a new JdbcOutboxConfig with defaults
     */
    public static JdbcOutboxConfig defaults() {
        return new JdbcOutboxConfig(DEFAULT_TABLE_NAME, SqlDialect.POSTGRESQL);
    }

    /**
     * Creates a configuration for PostgreSQL with the default table name.
     *
     * @return a new JdbcOutboxConfig for PostgreSQL
     */
    public static JdbcOutboxConfig postgresql() {
        return new JdbcOutboxConfig(DEFAULT_TABLE_NAME, SqlDialect.POSTGRESQL);
    }

    /**
     * Creates a configuration for PostgreSQL with a custom table name.
     *
     * @param tableName the table name
     * @return a new JdbcOutboxConfig for PostgreSQL
     */
    public static JdbcOutboxConfig postgresql(String tableName) {
        return new JdbcOutboxConfig(tableName, SqlDialect.POSTGRESQL);
    }

    /**
     * Creates a configuration for MySQL with the default table name.
     *
     * @return a new JdbcOutboxConfig for MySQL
     */
    public static JdbcOutboxConfig mysql() {
        return new JdbcOutboxConfig(DEFAULT_TABLE_NAME, SqlDialect.MYSQL);
    }

    /**
     * Creates a configuration for MySQL with a custom table name.
     *
     * @param tableName the table name
     * @return a new JdbcOutboxConfig for MySQL
     */
    public static JdbcOutboxConfig mysql(String tableName) {
        return new JdbcOutboxConfig(tableName, SqlDialect.MYSQL);
    }

    /**
     * Creates a configuration for H2 with the default table name.
     *
     * @return a new JdbcOutboxConfig for H2
     */
    public static JdbcOutboxConfig h2() {
        return new JdbcOutboxConfig(DEFAULT_TABLE_NAME, SqlDialect.H2);
    }

    /**
     * Creates a configuration for H2 with a custom table name.
     *
     * @param tableName the table name
     * @return a new JdbcOutboxConfig for H2
     */
    public static JdbcOutboxConfig h2(String tableName) {
        return new JdbcOutboxConfig(tableName, SqlDialect.H2);
    }

    /**
     * Creates a new builder for constructing JdbcOutboxConfig instances.
     *
     * @return a new builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing JdbcOutboxConfig instances.
     */
    public static final class Builder {
        private String tableName = DEFAULT_TABLE_NAME;
        private SqlDialect dialect = SqlDialect.POSTGRESQL;

        private Builder() {}

        /**
         * Sets the outbox table name.
         *
         * @param tableName the table name
         * @return this builder
         */
        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /**
         * Sets the SQL dialect.
         *
         * @param dialect the SQL dialect
         * @return this builder
         */
        public Builder dialect(SqlDialect dialect) {
            this.dialect = dialect;
            return this;
        }

        /**
         * Builds the JdbcOutboxConfig.
         *
         * @return a new JdbcOutboxConfig instance
         */
        public JdbcOutboxConfig build() {
            return new JdbcOutboxConfig(tableName, dialect);
        }
    }
}
