package olap.duck;


import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;

public class DuckOutput {

    private static final String DELIMITER = ",";

    public static Path copyOut(
            Connection connection, String tableName, Path targetDirectory
    ) throws SQLException {
        if (!Files.isDirectory(targetDirectory)) {
            throw new IllegalArgumentException("The target is not a directory.");
        }

        var out = targetDirectory.resolve(Path.of(tableName.concat(".csv")));
        if (Files.exists(out)) {
            throw new IllegalStateException(String.format("Output file '%s' already exists.", out));
        }

        try (var stmt = connection.createStatement()) {
            stmt.execute(format("COPY (FROM %s) TO %s WITH (HEADER 1, DELIMITER %s);",
                    stmt.enquoteIdentifier(tableName, true),
                    stmt.enquoteLiteral(out.toString()),
                    stmt.enquoteLiteral(DELIMITER)
            ));
        }

        return out;
    }

    public static List<Path> copyOutAll(
            Connection connection, Collection<String> tables, Path targetDirectory
    ) {
        return tables.stream()
                .map(table -> {
                    try {
                        return copyOut(connection, table, targetDirectory);
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                }).toList();
    }
}