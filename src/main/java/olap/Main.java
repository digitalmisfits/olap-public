package olap;

import olap.duck.DuckOutput;
import olap.duck.DuckInput;

import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class Main {

    static {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Failed to load the DuckDB driver", e);
        }
    }

    public static void main(String[] args) {

        var input = List.of(
                Path.of("/Users/epdittmer/Downloads/organizations-2000000.csv"),
                Path.of("/Users/epdittmer/Downloads/organizations-2000000-copy.csv")
        );

        try (var conn = DriverManager.getConnection("jdbc:duckdb:")) {
            var tables = DuckInput.copyInAutoAll(conn, input);
            tables.forEach(table -> {
                System.out.printf("Table: name = '%s'\n", table.name());
                table.columns().forEach(col -> {
                    System.out.printf("Column: name = '%s', type = '%s'\n", col.name(), col.type());
                });
            });

            var tableNames = tables.stream()
                    .map(DuckInput.Table::name)
                    .toList();

            var path = DuckOutput.copyOutAll(conn, tableNames, Path.of("/Users/epdittmer/Downloads/test/"));
            System.out.printf("Output: path = '%s'\n", path);

        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}