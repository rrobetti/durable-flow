package io.github.durableflow.persistence.dialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

/**
 * Detects the appropriate {@link DatabaseDialect} from a {@link DataSource} by inspecting
 * JDBC {@link DatabaseMetaData#getDatabaseProductName()}.
 *
 * <p>Supported auto-detected databases:
 * <ul>
 *   <li>PostgreSQL → {@link PostgreSqlDialect}</li>
 *   <li>Oracle → {@link OracleDialect}</li>
 *   <li>MySQL / MariaDB → {@link MySqlDialect}</li>
 *   <li>IBM DB2 → {@link Db2Dialect}</li>
 *   <li>Microsoft SQL Server → {@link SqlServerDialect}</li>
 * </ul>
 *
 * <p>Falls back to {@link PostgreSqlDialect} when the product name does not match any
 * known database. Supply a dialect explicitly via
 * {@link io.github.durableflow.DurableFlowConfig.Builder#dialect(DatabaseDialect)} to override.
 */
public final class DatabaseDialectFactory {

    private static final Logger log = LoggerFactory.getLogger(DatabaseDialectFactory.class);

    private DatabaseDialectFactory() {}

    /**
     * Probes the given {@link DataSource} and returns the matching {@link DatabaseDialect}.
     *
     * @param dataSource the data source to probe
     * @return detected dialect, never {@code null}
     * @throws RuntimeException if the metadata query fails
     */
    public static DatabaseDialect detect(DataSource dataSource) {
        String productName;
        try (Connection conn = dataSource.getConnection()) {
            DatabaseMetaData meta = conn.getMetaData();
            productName = meta.getDatabaseProductName();
        } catch (SQLException e) {
            throw new RuntimeException("Could not detect database dialect from DataSource metadata", e);
        }

        String lower = productName.toLowerCase();
        DatabaseDialect dialect;

        if (lower.contains("postgresql")) {
            dialect = PostgreSqlDialect.INSTANCE;
        } else if (lower.contains("oracle")) {
            dialect = OracleDialect.INSTANCE;
        } else if (lower.contains("mariadb") || lower.contains("mysql")) {
            dialect = MySqlDialect.INSTANCE;
        } else if (lower.contains("db2")) {
            dialect = Db2Dialect.INSTANCE;
        } else if (lower.contains("microsoft sql server") || lower.contains("sql server")) {
            dialect = SqlServerDialect.INSTANCE;
        } else {
            log.warn("Unrecognised database product '{}'; defaulting to PostgreSQL dialect", productName);
            dialect = PostgreSqlDialect.INSTANCE;
        }

        log.info("Database dialect detected: {} (product: {})", dialect.name(), productName);
        return dialect;
    }
}
