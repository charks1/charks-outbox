package xyz.charks.outbox.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import xyz.charks.outbox.core.LockMode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("SqlDialect")
class SqlDialectTest {

    @Nested
    @DisplayName("PostgreSQL dialect")
    class PostgreSqlDialectTest {

        private final SqlDialect dialect = SqlDialect.POSTGRESQL;

        @Test
        @DisplayName("returns FOR UPDATE clause for FOR_UPDATE lock mode")
        void lockClauseForUpdate() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE)).isEqualTo(" FOR UPDATE");
        }

        @Test
        @DisplayName("returns FOR UPDATE SKIP LOCKED clause")
        void lockClauseSkipLocked() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE_SKIP_LOCKED))
                    .isEqualTo(" FOR UPDATE SKIP LOCKED");
        }

        @Test
        @DisplayName("returns empty string for NONE lock mode")
        void lockClauseNone() {
            assertThat(dialect.lockClause(LockMode.NONE)).isEmpty();
        }

        @Test
        @DisplayName("returns LIMIT clause")
        void limitClause() {
            assertThat(dialect.limitClause(50)).isEqualTo(" LIMIT 50");
        }

        @Test
        @DisplayName("returns correct SQL types")
        void sqlTypes() {
            assertThat(dialect.uuidType()).isEqualTo("UUID");
            assertThat(dialect.binaryType()).isEqualTo("BYTEA");
            assertThat(dialect.jsonType()).isEqualTo("JSONB");
            assertThat(dialect.timestampType()).isEqualTo("TIMESTAMP WITH TIME ZONE");
        }

        @Test
        @DisplayName("supports SKIP LOCKED")
        void supportsSkipLocked() {
            assertThat(dialect.supportsSkipLocked()).isTrue();
        }
    }

    @Nested
    @DisplayName("MySQL dialect")
    class MySqlDialectTest {

        private final SqlDialect dialect = SqlDialect.MYSQL;

        @Test
        @DisplayName("returns FOR UPDATE clause for FOR_UPDATE lock mode")
        void lockClauseForUpdate() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE)).isEqualTo(" FOR UPDATE");
        }

        @Test
        @DisplayName("returns FOR UPDATE SKIP LOCKED clause")
        void lockClauseSkipLocked() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE_SKIP_LOCKED))
                    .isEqualTo(" FOR UPDATE SKIP LOCKED");
        }

        @Test
        @DisplayName("returns correct SQL types")
        void sqlTypes() {
            assertThat(dialect.uuidType()).isEqualTo("VARCHAR(36)");
            assertThat(dialect.binaryType()).isEqualTo("LONGBLOB");
            assertThat(dialect.jsonType()).isEqualTo("JSON");
            assertThat(dialect.timestampType()).isEqualTo("TIMESTAMP(6)");
        }

        @Test
        @DisplayName("supports SKIP LOCKED")
        void supportsSkipLocked() {
            assertThat(dialect.supportsSkipLocked()).isTrue();
        }
    }

    @Nested
    @DisplayName("Oracle dialect")
    class OracleDialectTest {

        private final SqlDialect dialect = SqlDialect.ORACLE;

        @Test
        @DisplayName("returns FOR UPDATE clause for FOR_UPDATE lock mode")
        void lockClauseForUpdate() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE)).isEqualTo(" FOR UPDATE");
        }

        @Test
        @DisplayName("returns FOR UPDATE SKIP LOCKED clause")
        void lockClauseSkipLocked() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE_SKIP_LOCKED))
                    .isEqualTo(" FOR UPDATE SKIP LOCKED");
        }

        @Test
        @DisplayName("returns FETCH FIRST clause for limit")
        void limitClause() {
            assertThat(dialect.limitClause(50)).isEqualTo(" FETCH FIRST 50 ROWS ONLY");
        }

        @Test
        @DisplayName("returns correct SQL types")
        void sqlTypes() {
            assertThat(dialect.uuidType()).isEqualTo("VARCHAR2(36)");
            assertThat(dialect.binaryType()).isEqualTo("BLOB");
            assertThat(dialect.jsonType()).isEqualTo("CLOB");
            assertThat(dialect.timestampType()).isEqualTo("TIMESTAMP WITH TIME ZONE");
        }

        @Test
        @DisplayName("supports SKIP LOCKED")
        void supportsSkipLocked() {
            assertThat(dialect.supportsSkipLocked()).isTrue();
        }
    }

    @Nested
    @DisplayName("SQL Server dialect")
    class SqlServerDialectTest {

        private final SqlDialect dialect = SqlDialect.SQLSERVER;

        @Test
        @DisplayName("returns UPDLOCK, ROWLOCK hints for FOR_UPDATE lock mode")
        void lockClauseForUpdate() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE)).isEqualTo(" WITH (UPDLOCK, ROWLOCK)");
        }

        @Test
        @DisplayName("returns UPDLOCK, ROWLOCK, READPAST hints for SKIP LOCKED")
        void lockClauseSkipLocked() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE_SKIP_LOCKED))
                    .isEqualTo(" WITH (UPDLOCK, ROWLOCK, READPAST)");
        }

        @Test
        @DisplayName("returns OFFSET/FETCH clause for limit")
        void limitClause() {
            assertThat(dialect.limitClause(50)).isEqualTo(" OFFSET 0 ROWS FETCH NEXT 50 ROWS ONLY");
        }

        @Test
        @DisplayName("returns correct SQL types")
        void sqlTypes() {
            assertThat(dialect.uuidType()).isEqualTo("UNIQUEIDENTIFIER");
            assertThat(dialect.binaryType()).isEqualTo("VARBINARY(MAX)");
            assertThat(dialect.jsonType()).isEqualTo("NVARCHAR(MAX)");
            assertThat(dialect.timestampType()).isEqualTo("DATETIMEOFFSET");
        }

        @Test
        @DisplayName("supports SKIP LOCKED via READPAST")
        void supportsSkipLocked() {
            assertThat(dialect.supportsSkipLocked()).isTrue();
        }
    }

    @Nested
    @DisplayName("H2 dialect")
    class H2DialectTest {

        private final SqlDialect dialect = SqlDialect.H2;

        @Test
        @DisplayName("returns FOR UPDATE for both lock modes")
        void lockClauseFallback() {
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE)).isEqualTo(" FOR UPDATE");
            assertThat(dialect.lockClause(LockMode.FOR_UPDATE_SKIP_LOCKED)).isEqualTo(" FOR UPDATE");
        }

        @Test
        @DisplayName("returns correct SQL types")
        void sqlTypes() {
            assertThat(dialect.uuidType()).isEqualTo("UUID");
            assertThat(dialect.binaryType()).isEqualTo("BYTEA");
            assertThat(dialect.jsonType()).isEqualTo("VARCHAR");
            assertThat(dialect.timestampType()).isEqualTo("TIMESTAMP WITH TIME ZONE");
        }

        @Test
        @DisplayName("does not support SKIP LOCKED")
        void supportsSkipLocked() {
            assertThat(dialect.supportsSkipLocked()).isFalse();
        }
    }

    @Nested
    @DisplayName("fromJdbcUrl")
    class FromJdbcUrlTest {

        @ParameterizedTest
        @ValueSource(strings = {
                "jdbc:postgresql://localhost:5432/mydb",
                "jdbc:postgresql://host/db?ssl=true",
                "jdbc:pgsql://localhost/test"
        })
        @DisplayName("detects PostgreSQL from JDBC URL")
        void detectPostgresql(String url) {
            assertThat(SqlDialect.fromJdbcUrl(url)).isEqualTo(SqlDialect.POSTGRESQL);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "jdbc:mysql://localhost:3306/mydb",
                "jdbc:mariadb://localhost:3306/mydb"
        })
        @DisplayName("detects MySQL/MariaDB from JDBC URL")
        void detectMysql(String url) {
            assertThat(SqlDialect.fromJdbcUrl(url)).isEqualTo(SqlDialect.MYSQL);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "jdbc:h2:mem:testdb",
                "jdbc:h2:file:/data/sample",
                "jdbc:h2:tcp://localhost/~/test"
        })
        @DisplayName("detects H2 from JDBC URL")
        void detectH2(String url) {
            assertThat(SqlDialect.fromJdbcUrl(url)).isEqualTo(SqlDialect.H2);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "jdbc:oracle:thin:@localhost:1521:xe",
                "jdbc:oracle:thin:@//hostname:port/service",
                "jdbc:oracle:oci:@mydb"
        })
        @DisplayName("detects Oracle from JDBC URL")
        void detectOracle(String url) {
            assertThat(SqlDialect.fromJdbcUrl(url)).isEqualTo(SqlDialect.ORACLE);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "jdbc:sqlserver://localhost:1433;databaseName=mydb",
                "jdbc:sqlserver://host;database=db;user=sa",
                "jdbc:jtds:sqlserver://localhost/mydb"
        })
        @DisplayName("detects SQL Server from JDBC URL")
        void detectSqlServer(String url) {
            assertThat(SqlDialect.fromJdbcUrl(url)).isEqualTo(SqlDialect.SQLSERVER);
        }

        @Test
        @DisplayName("throws exception for unsupported database")
        void unsupportedDatabase() {
            assertThatThrownBy(() -> SqlDialect.fromJdbcUrl("jdbc:db2://localhost:50000/mydb"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported database");
        }

        @Test
        @DisplayName("throws exception for null URL")
        void nullUrl() {
            assertThatThrownBy(() -> SqlDialect.fromJdbcUrl(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be null");
        }

        @Test
        @DisplayName("throws exception for blank URL")
        void blankUrl() {
            assertThatThrownBy(() -> SqlDialect.fromJdbcUrl("  "))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot be null or blank");
        }
    }
}
