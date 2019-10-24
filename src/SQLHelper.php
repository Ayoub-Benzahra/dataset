<?php namespace Dataset;

trait SQLHelper
{
    private function checkIfTableExists ($table) {
        $tables = app('db')->connection()->getDoctrineSchemaManager()->listTableNames();

        return in_array($table, $tables);
    }

    private function getTableColumns ($table) {
        return app('db')->getSchemaBuilder()->getColumnListing($table);
    }

    protected function queryBuilder (array $fields, $table = '') {
        $table = $table ?: $this->table;

        if (empty($table)) {
            throw new DatasetException("Table name is required.");
        }

        $query = sprintf("INSERT INTO %s (%s) VALUES (%s)", $table, implode(", ", array_values($fields)), implode(", ", array_fill(0, count($fields), "?")));

        return $query;
    }

    protected function checkIfTableColumnsExist (array $updateColumns, $table = '') {
        $table = $table ?: $this->table;

        if (empty($table)) {
            throw new DatasetException("Table name is required.");
        }

        $query = "SHOW COLUMNS from " . $table;
        $statement = $this->connection->getPDO()->prepare($query);
        $statement->execute();
        $tableColumns = array_map(function ($column) {
            return $column['Field'];
        }, $statement->fetchAll(\PDO::FETCH_ASSOC));
        if (count(array_diff($tableColumns, $updateColumns)) !== (count($tableColumns) - count($updateColumns))) {
            $difference = array_diff($updateColumns, $tableColumns);
            throw new DatasetException(sprintf("Unknown %s `%s` in table - `%s`", count($difference) == 1 ? "column" : "columns", implode(", ", $difference), $table));
        }

        return true;
    }
}
