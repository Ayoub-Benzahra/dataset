<?php namespace Dataset;

trait SQLHelper
{
    private function checkIfTableExists ($table) {
        $tables = $this->dbManager()->connection()->getDoctrineSchemaManager()->listTableNames();

        return in_array($table, $tables);
    }

    private function getTableColumns ($table) {
        return $this->dbManager()->getSchemaBuilder()->getColumnListing($table);
    }

    protected function queryBuilder (array $fields, $table = '') {
        $table = $table ?: $this->table;

        if (empty($table)) {
            throw new DatasetException("Table name is required.");
        }

        $query = sprintf("INSERT INTO %s (%s) VALUES (%s)", $table, implode(", ", array_values($fields)), implode(", ", array_fill(0, count($fields), "?")));

        return $query;
    }
}
