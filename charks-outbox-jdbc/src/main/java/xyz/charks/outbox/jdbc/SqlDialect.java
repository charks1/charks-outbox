package xyz.charks.outbox.jdbc;

import xyz.charks.outbox.core.LockMode;

/**
 * SQL dialect implementations for different database systems.
 *
 * <p>Each dialect provides database-specific SQL syntax for:
 * <ul>
 *   <li>Row locking clauses</li>
 *   <li>LIMIT/OFFSET syntax</li>
 *   <li>Parameter placeholders</li>
 *   <li>Binary data handling</li>
 * </ul>
 *
 * <p>Supported databases:
 * <ul>
 *   <li>{@link #POSTGRESQL} - PostgreSQL 12+</li>
 *   <li>{@link #MYSQL} - MySQL 8.0+ / MariaDB 10.3+</li>
 *   <li>{@link #H2} - H2 Database (for testing)</li>
 * </ul>
 */
public enum SqlDialect {

    /**
     * PostgreSQL dialect with full feature support.
     *
     * <p>Supports:
     * <ul>
     *   <li>FOR UPDATE SKIP LOCKED</li>
     *   <li>BYTEA for binary data</li>
     *   <li>JSONB for headers</li>
     *   <li>UUID native type</li>
     * </ul>
     */
    POSTGRESQL {
        @Override
        public String lockClause(LockMode mode) {
            return switch (mode) {
                case FOR_UPDATE -> " FOR UPDATE";
                case FOR_UPDATE_SKIP_LOCKED -> " FOR UPDATE SKIP LOCKED";
                case NONE -> "";
            };
        }

        @Override
        public String limitClause(int limit) {
            return " LIMIT " + limit;
        }

        @Override
        public String uuidType() {
            return "UUID";
        }

        @Override
        public String binaryType() {
            return "BYTEA";
        }

        @Override
        public String jsonType() {
            return "JSONB";
        }

        @Override
        public String timestampType() {
            return "TIMESTAMP WITH TIME ZONE";
        }

        @Override
        public boolean supportsSkipLocked() {
            return true;
        }
    },

    /**
     * MySQL dialect with InnoDB features.
     *
     * <p>Supports:
     * <ul>
     *   <li>FOR UPDATE SKIP LOCKED (MySQL 8.0+)</li>
     *   <li>BLOB for binary data</li>
     *   <li>JSON for headers</li>
     *   <li>BINARY(16) or VARCHAR(36) for UUID</li>
     * </ul>
     */
    MYSQL {
        @Override
        public String lockClause(LockMode mode) {
            return switch (mode) {
                case FOR_UPDATE -> " FOR UPDATE";
                case FOR_UPDATE_SKIP_LOCKED -> " FOR UPDATE SKIP LOCKED";
                case NONE -> "";
            };
        }

        @Override
        public String limitClause(int limit) {
            return " LIMIT " + limit;
        }

        @Override
        public String uuidType() {
            return "VARCHAR(36)";
        }

        @Override
        public String binaryType() {
            return "LONGBLOB";
        }

        @Override
        public String jsonType() {
            return "JSON";
        }

        @Override
        public String timestampType() {
            return "TIMESTAMP(6)";
        }

        @Override
        public boolean supportsSkipLocked() {
            return true;
        }
    },

    /**
     * H2 dialect for testing purposes.
     *
     * <p>Note: H2 has limited support for advanced locking.
     * FOR UPDATE SKIP LOCKED is emulated with FOR UPDATE.
     */
    H2 {
        @Override
        public String lockClause(LockMode mode) {
            return switch (mode) {
                case FOR_UPDATE, FOR_UPDATE_SKIP_LOCKED -> " FOR UPDATE";
                case NONE -> "";
            };
        }

        @Override
        public String limitClause(int limit) {
            return " LIMIT " + limit;
        }

        @Override
        public String uuidType() {
            return "UUID";
        }

        @Override
        public String binaryType() {
            return "BYTEA";
        }

        @Override
        public String jsonType() {
            return "VARCHAR";
        }

        @Override
        public String timestampType() {
            return "TIMESTAMP WITH TIME ZONE";
        }

        @Override
        public boolean supportsSkipLocked() {
            return false;
        }
    };

    /**
     * Returns the SQL locking clause for the given lock mode.
     *
     * @param mode the desired locking mode
     * @return the SQL clause including leading space, or empty string for NONE
     */
    public abstract String lockClause(LockMode mode);

    /**
     * Returns the SQL LIMIT clause for result size restriction.
     *
     * @param limit the maximum number of rows
     * @return the SQL LIMIT clause including leading space
     */
    public abstract String limitClause(int limit);

    /**
     * Returns the SQL type for UUID columns.
     *
     * @return the database-specific UUID type
     */
    public abstract String uuidType();

    /**
     * Returns the SQL type for binary data columns.
     *
     * @return the database-specific binary type
     */
    public abstract String binaryType();

    /**
     * Returns the SQL type for JSON columns.
     *
     * @return the database-specific JSON type
     */
    public abstract String jsonType();

    /**
     * Returns the SQL type for timestamp columns.
     *
     * @return the database-specific timestamp type
     */
    public abstract String timestampType();

    /**
     * Indicates whether this dialect supports SKIP LOCKED.
     *
     * @return true if FOR UPDATE SKIP LOCKED is supported
     */
    public abstract boolean supportsSkipLocked();

    /**
     * Detects the appropriate dialect from a JDBC URL.
     *
     * @param jdbcUrl the JDBC connection URL
     * @return the detected dialect
     * @throws IllegalArgumentException if the database is not supported
     */
    public static SqlDialect fromJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new IllegalArgumentException("JDBC URL cannot be null or blank");
        }

        String lowerUrl = jdbcUrl.toLowerCase();
        if (lowerUrl.contains(":postgresql:") || lowerUrl.contains(":pgsql:")) {
            return POSTGRESQL;
        } else if (lowerUrl.contains(":mysql:") || lowerUrl.contains(":mariadb:")) {
            return MYSQL;
        } else if (lowerUrl.contains(":h2:")) {
            return H2;
        }

        throw new IllegalArgumentException("Unsupported database in JDBC URL: " + jdbcUrl);
    }
}
