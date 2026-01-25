package xyz.charks.outbox.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JdbcOutboxConfig")
class JdbcOutboxConfigTest {

    @Nested
    @DisplayName("construction")
    class ConstructionTest {

        @Test
        @DisplayName("creates config with valid parameters")
        void validParameters() {
            JdbcOutboxConfig config = new JdbcOutboxConfig("my_outbox", SqlDialect.MYSQL);

            assertThat(config.tableName()).isEqualTo("my_outbox");
            assertThat(config.dialect()).isEqualTo(SqlDialect.MYSQL);
        }

        @Test
        @DisplayName("throws exception for null table name")
        void nullTableName() {
            assertThatThrownBy(() -> new JdbcOutboxConfig(null, SqlDialect.POSTGRESQL))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Table name cannot be null");
        }

        @Test
        @DisplayName("throws exception for blank table name")
        void blankTableName() {
            assertThatThrownBy(() -> new JdbcOutboxConfig("  ", SqlDialect.POSTGRESQL))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Table name cannot be blank");
        }

        @Test
        @DisplayName("throws exception for null dialect")
        void nullDialect() {
            assertThatThrownBy(() -> new JdbcOutboxConfig("outbox_events", null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("Dialect cannot be null");
        }
    }

    @Nested
    @DisplayName("factory methods")
    class FactoryMethodsTest {

        @Test
        @DisplayName("creates default configuration")
        void defaults() {
            JdbcOutboxConfig config = JdbcOutboxConfig.defaults();

            assertThat(config.tableName()).isEqualTo(JdbcOutboxConfig.DEFAULT_TABLE_NAME);
            assertThat(config.dialect()).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @Test
        @DisplayName("creates PostgreSQL configuration")
        void postgresql() {
            JdbcOutboxConfig config = JdbcOutboxConfig.postgresql();

            assertThat(config.tableName()).isEqualTo(JdbcOutboxConfig.DEFAULT_TABLE_NAME);
            assertThat(config.dialect()).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @Test
        @DisplayName("creates PostgreSQL configuration with custom table")
        void postgresqlWithTable() {
            JdbcOutboxConfig config = JdbcOutboxConfig.postgresql("my_outbox");

            assertThat(config.tableName()).isEqualTo("my_outbox");
            assertThat(config.dialect()).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @Test
        @DisplayName("creates MySQL configuration")
        void mysql() {
            JdbcOutboxConfig config = JdbcOutboxConfig.mysql();

            assertThat(config.tableName()).isEqualTo(JdbcOutboxConfig.DEFAULT_TABLE_NAME);
            assertThat(config.dialect()).isEqualTo(SqlDialect.MYSQL);
        }

        @Test
        @DisplayName("creates MySQL configuration with custom table")
        void mysqlWithTable() {
            JdbcOutboxConfig config = JdbcOutboxConfig.mysql("mysql_outbox");

            assertThat(config.tableName()).isEqualTo("mysql_outbox");
            assertThat(config.dialect()).isEqualTo(SqlDialect.MYSQL);
        }

        @Test
        @DisplayName("creates H2 configuration")
        void h2() {
            JdbcOutboxConfig config = JdbcOutboxConfig.h2();

            assertThat(config.tableName()).isEqualTo(JdbcOutboxConfig.DEFAULT_TABLE_NAME);
            assertThat(config.dialect()).isEqualTo(SqlDialect.H2);
        }

        @Test
        @DisplayName("creates H2 configuration with custom table")
        void h2WithTable() {
            JdbcOutboxConfig config = JdbcOutboxConfig.h2("test_outbox");

            assertThat(config.tableName()).isEqualTo("test_outbox");
            assertThat(config.dialect()).isEqualTo(SqlDialect.H2);
        }
    }

    @Nested
    @DisplayName("builder")
    class BuilderTest {

        @Test
        @DisplayName("builds config with default values")
        void defaultValues() {
            JdbcOutboxConfig config = JdbcOutboxConfig.builder().build();

            assertThat(config.tableName()).isEqualTo(JdbcOutboxConfig.DEFAULT_TABLE_NAME);
            assertThat(config.dialect()).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @Test
        @DisplayName("builds config with custom table name")
        void customTableName() {
            JdbcOutboxConfig config = JdbcOutboxConfig.builder()
                    .tableName("custom_outbox")
                    .build();

            assertThat(config.tableName()).isEqualTo("custom_outbox");
        }

        @Test
        @DisplayName("builds config with custom dialect")
        void customDialect() {
            JdbcOutboxConfig config = JdbcOutboxConfig.builder()
                    .dialect(SqlDialect.MYSQL)
                    .build();

            assertThat(config.dialect()).isEqualTo(SqlDialect.MYSQL);
        }

        @Test
        @DisplayName("builds config with all custom values")
        void allCustomValues() {
            JdbcOutboxConfig config = JdbcOutboxConfig.builder()
                    .tableName("my_events")
                    .dialect(SqlDialect.H2)
                    .build();

            assertThat(config.tableName()).isEqualTo("my_events");
            assertThat(config.dialect()).isEqualTo(SqlDialect.H2);
        }
    }

    @Test
    @DisplayName("DEFAULT_TABLE_NAME constant has expected value")
    void defaultTableNameConstant() {
        assertThat(JdbcOutboxConfig.DEFAULT_TABLE_NAME).isEqualTo("outbox_events");
    }
}
