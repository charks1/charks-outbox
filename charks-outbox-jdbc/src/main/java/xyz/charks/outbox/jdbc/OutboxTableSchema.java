package xyz.charks.outbox.jdbc;

import java.util.Objects;

/**
 * DDL schema generator for outbox tables.
 *
 * <p>Generates database-specific CREATE TABLE statements and index definitions
 * based on the configured SQL dialect.
 *
 * <p>Example usage:
 * <pre>{@code
 * OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
 *
 * // Generate full schema
 * String ddl = schema.createTableStatement("outbox_events");
 *
 * // Generate index separately
 * String index = schema.createPendingIndexStatement("outbox_events");
 * }</pre>
 *
 * <p>The generated schema follows this structure:
 * <pre>
 * outbox_events
 * ├── id              UUID PRIMARY KEY
 * ├── aggregate_type  VARCHAR(255) NOT NULL
 * ├── aggregate_id    VARCHAR(255) NOT NULL
 * ├── event_type      VARCHAR(255) NOT NULL
 * ├── topic           VARCHAR(255) NOT NULL
 * ├── partition_key   VARCHAR(255)
 * ├── payload         BYTEA NOT NULL
 * ├── headers         JSONB
 * ├── created_at      TIMESTAMP WITH TIME ZONE NOT NULL
 * ├── status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'
 * ├── retry_count     INTEGER NOT NULL DEFAULT 0
 * ├── last_error      TEXT
 * └── processed_at    TIMESTAMP WITH TIME ZONE
 * </pre>
 */
public final class OutboxTableSchema {

    private final SqlDialect dialect;

    private OutboxTableSchema(SqlDialect dialect) {
        this.dialect = Objects.requireNonNull(dialect, "Dialect cannot be null");
    }

    /**
     * Creates a schema generator for the specified dialect.
     *
     * @param dialect the SQL dialect to use
     * @return a new OutboxTableSchema instance
     * @throws NullPointerException if dialect is null
     */
    public static OutboxTableSchema forDialect(SqlDialect dialect) {
        return new OutboxTableSchema(dialect);
    }

    /**
     * Generates the CREATE TABLE statement for the outbox table.
     *
     * @param tableName the name of the table to create
     * @return the complete CREATE TABLE statement
     * @throws NullPointerException if tableName is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public String createTableStatement(String tableName) {
        validateTableName(tableName);

        return """
                CREATE TABLE %s (
                    id              %s PRIMARY KEY,
                    aggregate_type  VARCHAR(255) NOT NULL,
                    aggregate_id    VARCHAR(255) NOT NULL,
                    event_type      VARCHAR(255) NOT NULL,
                    topic           VARCHAR(255) NOT NULL,
                    partition_key   VARCHAR(255),
                    payload         %s NOT NULL,
                    headers         %s,
                    created_at      %s NOT NULL,
                    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    retry_count     INTEGER NOT NULL DEFAULT 0,
                    last_error      TEXT,
                    processed_at    %s
                )""".formatted(
                tableName,
                dialect.uuidType(),
                dialect.binaryType(),
                dialect.jsonType(),
                dialect.timestampType(),
                dialect.timestampType()
        );
    }

    /**
     * Generates an index statement for efficient pending event queries.
     *
     * <p>Creates a partial index on (status, created_at) filtered to PENDING status
     * for PostgreSQL, or a regular index for other databases.
     *
     * @param tableName the name of the outbox table
     * @return the CREATE INDEX statement
     * @throws NullPointerException if tableName is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public String createPendingIndexStatement(String tableName) {
        validateTableName(tableName);

        String indexName = "idx_" + tableName + "_pending";

        return switch (dialect) {
            case POSTGRESQL, ORACLE -> """
                    CREATE INDEX %s ON %s (status, created_at)
                        WHERE status = 'PENDING'""".formatted(indexName, tableName);
            case MYSQL, SQLSERVER, H2 -> "CREATE INDEX %s ON %s (status, created_at)".formatted(indexName, tableName);
        };
    }

    /**
     * Generates an index statement for aggregate-based queries.
     *
     * @param tableName the name of the outbox table
     * @return the CREATE INDEX statement
     * @throws NullPointerException if tableName is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public String createAggregateIndexStatement(String tableName) {
        validateTableName(tableName);

        String indexName = "idx_" + tableName + "_aggregate";
        return "CREATE INDEX %s ON %s (aggregate_type, aggregate_id)".formatted(indexName, tableName);
    }

    /**
     * Generates the complete schema including table and all indexes.
     *
     * @param tableName the name of the outbox table
     * @return the complete DDL statements separated by semicolons and newlines
     * @throws NullPointerException if tableName is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public String createFullSchema(String tableName) {
        validateTableName(tableName);

        return String.join(";\n\n",
                createTableStatement(tableName),
                createPendingIndexStatement(tableName),
                createAggregateIndexStatement(tableName)
        ) + ";";
    }

    /**
     * Generates a DROP TABLE statement.
     *
     * @param tableName the name of the table to drop
     * @param ifExists if true, adds IF EXISTS clause
     * @return the DROP TABLE statement
     * @throws NullPointerException if tableName is null
     * @throws IllegalArgumentException if tableName is blank
     */
    public String dropTableStatement(String tableName, boolean ifExists) {
        validateTableName(tableName);

        return ifExists
                ? "DROP TABLE IF EXISTS " + tableName
                : "DROP TABLE " + tableName;
    }

    /**
     * Returns the configured SQL dialect.
     *
     * @return the dialect used by this schema generator
     */
    public SqlDialect dialect() {
        return dialect;
    }

    private void validateTableName(String tableName) {
        Objects.requireNonNull(tableName, "Table name cannot be null");
        if (tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be blank");
        }
    }
}
