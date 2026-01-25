package xyz.charks.outbox.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("OutboxTableSchema")
class OutboxTableSchemaTest {

    @Nested
    @DisplayName("forDialect")
    class ForDialectTest {

        @Test
        @DisplayName("creates schema for PostgreSQL")
        void postgresql() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            assertThat(schema.dialect()).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @Test
        @DisplayName("creates schema for MySQL")
        void mysql() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.MYSQL);
            assertThat(schema.dialect()).isEqualTo(SqlDialect.MYSQL);
        }

        @Test
        @DisplayName("creates schema for H2")
        void h2() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.H2);
            assertThat(schema.dialect()).isEqualTo(SqlDialect.H2);
        }

        @Test
        @DisplayName("throws exception for null dialect")
        void nullDialect() {
            assertThatThrownBy(() -> OutboxTableSchema.forDialect(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Dialect cannot be null");
        }
    }

    @Nested
    @DisplayName("createTableStatement")
    class CreateTableStatementTest {

        @Test
        @DisplayName("generates PostgreSQL CREATE TABLE statement")
        void postgresql() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.createTableStatement("outbox_events");

            assertThat(ddl)
                    .contains("CREATE TABLE outbox_events")
                    .contains("id              UUID PRIMARY KEY")
                    .contains("payload         BYTEA NOT NULL")
                    .contains("headers         JSONB")
                    .contains("created_at      TIMESTAMP WITH TIME ZONE NOT NULL")
                    .contains("status          VARCHAR(20) NOT NULL DEFAULT 'PENDING'");
        }

        @Test
        @DisplayName("generates MySQL CREATE TABLE statement")
        void mysql() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.MYSQL);
            String ddl = schema.createTableStatement("my_outbox");

            assertThat(ddl)
                    .contains("CREATE TABLE my_outbox")
                    .contains("id              VARCHAR(36) PRIMARY KEY")
                    .contains("payload         LONGBLOB NOT NULL")
                    .contains("headers         JSON")
                    .contains("created_at      TIMESTAMP(6) NOT NULL");
        }

        @Test
        @DisplayName("includes all required columns")
        void allColumns() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.createTableStatement("outbox_events");

            assertThat(ddl)
                    .contains("aggregate_type")
                    .contains("aggregate_id")
                    .contains("event_type")
                    .contains("topic")
                    .contains("partition_key")
                    .contains("payload")
                    .contains("headers")
                    .contains("created_at")
                    .contains("status")
                    .contains("retry_count")
                    .contains("last_error")
                    .contains("processed_at");
        }

        @Test
        @DisplayName("throws exception for null table name")
        void nullTableName() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            assertThatThrownBy(() -> schema.createTableStatement(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Table name cannot be null");
        }

        @Test
        @DisplayName("throws exception for blank table name")
        void blankTableName() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            assertThatThrownBy(() -> schema.createTableStatement("  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Table name cannot be blank");
        }
    }

    @Nested
    @DisplayName("createPendingIndexStatement")
    class CreatePendingIndexStatementTest {

        @Test
        @DisplayName("generates partial index for PostgreSQL")
        void postgresqlPartialIndex() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.createPendingIndexStatement("outbox_events");

            assertThat(ddl)
                    .contains("CREATE INDEX idx_outbox_events_pending")
                    .contains("ON outbox_events (status, created_at)")
                    .contains("WHERE status = 'PENDING'");
        }

        @Test
        @DisplayName("generates regular index for MySQL")
        void mysqlRegularIndex() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.MYSQL);
            String ddl = schema.createPendingIndexStatement("outbox_events");

            assertThat(ddl)
                    .contains("CREATE INDEX idx_outbox_events_pending")
                    .contains("ON outbox_events (status, created_at)")
                    .doesNotContain("WHERE");
        }
    }

    @Nested
    @DisplayName("createAggregateIndexStatement")
    class CreateAggregateIndexStatementTest {

        @Test
        @DisplayName("generates aggregate index")
        void aggregateIndex() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.createAggregateIndexStatement("outbox_events");

            assertThat(ddl)
                    .contains("CREATE INDEX idx_outbox_events_aggregate")
                    .contains("ON outbox_events (aggregate_type, aggregate_id)");
        }
    }

    @Nested
    @DisplayName("createFullSchema")
    class CreateFullSchemaTest {

        @Test
        @DisplayName("generates complete schema with table and indexes")
        void fullSchema() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.createFullSchema("outbox_events");

            assertThat(ddl)
                    .contains("CREATE TABLE outbox_events")
                    .contains("CREATE INDEX idx_outbox_events_pending")
                    .contains("CREATE INDEX idx_outbox_events_aggregate")
                    .contains(";");
        }
    }

    @Nested
    @DisplayName("dropTableStatement")
    class DropTableStatementTest {

        @Test
        @DisplayName("generates DROP TABLE statement")
        void dropTable() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.dropTableStatement("outbox_events", false);

            assertThat(ddl).isEqualTo("DROP TABLE outbox_events");
        }

        @Test
        @DisplayName("generates DROP TABLE IF EXISTS statement")
        void dropTableIfExists() {
            OutboxTableSchema schema = OutboxTableSchema.forDialect(SqlDialect.POSTGRESQL);
            String ddl = schema.dropTableStatement("outbox_events", true);

            assertThat(ddl).isEqualTo("DROP TABLE IF EXISTS outbox_events");
        }
    }
}
