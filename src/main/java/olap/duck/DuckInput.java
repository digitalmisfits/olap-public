package olap.duck;


import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.String.format;

public class DuckInput {

    public static Table copyInAuto(
            Connection connection, Path source
    ) throws SQLException {
        var tableName = basename(source);

        try (var stmt = connection.createStatement()) {
            stmt.execute(format("CREATE TABLE %s AS SELECT * FROM read_csv_auto(%s, all_varchar=true)",
                    stmt.enquoteIdentifier(tableName, true),
                    stmt.enquoteLiteral(source.toString())
            ));
        }

        try (var stmt = connection.createStatement()) {
            var results = stmt.executeQuery(format("DESCRIBE %s", stmt.enquoteIdentifier(tableName, true)));

            var columns = new ArrayList<Table.TableColumn>();
            while (results.next()) {
                columns.add(new Table.TableColumn(
                        results.getString("column_name"),
                        results.getString("column_type")
                ));
            }

            return new Table(tableName, columns);
        }
    }

    public static List<Table> copyInAutoAll(
            Connection connection, Collection<Path> sources
    ) {
        return sources.stream()
                .map(source -> {
                    try {
                        return copyInAuto(connection, source);
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                }).toList();
    }

    public record Table(String name, List<TableColumn> columns) {
        // container

        public record TableColumn(String name, String type) {
            // container
        }
    }

    private static String basename(Path path) {
        var s = path.getFileName().toString();
        var dot = s.lastIndexOf('.');
        if (dot == -1) {
            return s;
        }
        return s.substring(0, dot)
                .replace('.', '_');
    }
}
