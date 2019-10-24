<?php namespace Dataset;

use Illuminate\Container\Container;
use Illuminate\Database\DatabaseManager;
use League\Csv\Reader;

abstract class Dataset
{
    use SQLHelper;

    const TAG = 'dataset::';
    /* @var  Reader */
    private $reader = null;
    private $container = null;

    public function __construct (Container $container = null) {
        $this->container = $container ?: new Container;
    }

    # ---------------------- CSV Properties -------------- #
    // csv delimited by
    protected function delimiter () {
        return ',';
    }

    // csv value enclosure
    protected function enclosure () {
        return '"';
    }

    // csv escape character
    protected function escapeCharacter () {
        return '\\';
    }

    # ---------------------- Reader -------------- #

    private function prepareReader () {
        // https://csv.thephpleague.com/9.0/connections/#os-specificity
        if (!ini_get("auto_detect_line_endings")) {
            ini_set("auto_detect_line_endings", '1');
        }

        /**
         * uses string
         *     ? create from string
         *     : (
         *           uses resource stream
         *               ? create from stream
         *               : create from the file
         *       )
         */

        if ($s = $this->string()) {
            $reader = Reader::createFromString($s);
            $from = 'string';
        } elseif ($resource = $this->stream()) {
            $reader = Reader::createFromStream($resource);
            $from = 'stream';
        } else {
            $reader = Reader::createFromPath($this->file());
            $from = 'file';
        }

        $this->reader = $reader->setDelimiter($this->delimiter())
                               ->setEnclosure($this->enclosure())
                               ->setEscape($this->escapeCharacter());

        return $from;
    }

    # ---------------------- DB Manager -------------- #

    // database manager < app('db') / Illuminate\Database\DatabaseManager >
    protected function dbManager () : DatabaseManager {
        if ($this->container->bound('db')) {
            return $this->container['db'];
        }

        throw new DatasetException('DatabaseManager should be implemented.');
    }

    # ---------------------- Table & Class behaviors -------------- #

    // assume table name
    protected function table () {
        return morph_class_name($this);
    }

    // assume file name
    protected function file () {
        return morph_class_name($this) . ".csv";
    }

    // create from stream
    protected function stream () {
        return null;
    }

    // create from string
    protected function string () {
        return '';
    }

    // csv header name to table field mapper
    protected function mapper () {
        return [];
    }

    // omit table header row, true by default
    protected function omitHeader () {
        return true;
    }

    // additional fields while inserting data. 'key' => 'value' pair, closure can be passed as value
    protected function additionalFields () {
        return [];
    }

    // csv columns to ignore before inserting data
    protected function ignoredColumns () {
        return [];
    }

    // csv columns name === table field name
    protected function headerAsTableField () {
        return false;
    }

    # ---------------------- Events -------------- #

    public function getEventDispatcher () {
        if ($this->container->bound('events')) {
            return $this->container['events'];
        }
    }

    private function broadcastEvent ($event, ...$args) {
        // TODO: delete the echo when complete
        echo sprintf("EVENT BROADCAST: %s == Args: <%s>%s", $event, implode(", ", $args), PHP_EOL);
        if ($dispatcher = $this->getEventDispatcher()) {
            $modifiedEvent = self::TAG . $event;
            $dispatcher->dispatch(...array_merge([ $modifiedEvent ], $args));
        }
    }

    # ---------------------- Handlers -------------- #

    private function getCsvToTableColumns () {
        $columns = [];
        if ($this->headerAsTableField()) {
            // this will exclude the header
            $this->reader->setHeaderOffset(0);
            // get the csv headers & make it an associative array, before TRIM the headers
            $headers = array_map('trim', $this->reader->getHeader());
            $columns = array_combine($headers, $headers);
        }

        // get the mapper by user
        if ($mapper = $this->mapper()) {
            /*// if the mapper is not an associative array, make it an assoc array
            if (false === is_multidimensional_array($mapper)) {
                $mapper = array_combine($mapper, $mapper);
            }*/

            $columns = array_merge($columns, $mapper);
        }

        // check if any columns is said to ignore/won't insert into database
        // flushing ignored columns will lead to bugs cause count(csv_columns) > count(finalized_columns)
        /*if ($ignored = $this->ignoredColumns()) {
            $ignored = array_combine($ignored, $ignored);
            $columns = array_diff_key($columns, $ignored);
        }*/

        return $columns;
    }

    private function validateCSVParser ($columns) {
        // to keep the position of columns in place
        $positionalColumns = [];
        // STRUCTURE:
        // [
        //      'csv_column1' => 'table_column1', ~ 'csv_column1' => ['table_column1', null],
        //      'csv_column2' => false, // invalid
        //      'csv_column3' => [ 'table_column3', function($row) { return 'result'; } ], ~ 'csv_column3' => ['table_column3', 'callback']
        //      'csv_column4' => function ($row) { return 'result'; }, ~ 'csv_column4 => ['csv_column4', 'callback'],
        //      'csv_column5' => [ function ($row) { return 'result'; }], // invalid
        //];
        // 1. ['user_name' => 'name', 'user_email' => 'email'];
        // 2. ['username' => 'name', 'password' => function($row){ return hash($row['password']); }]
        // 3. ['username' => 'name', 'password' => false, 'email', 'first_name' => function($row){ return $row['first_name'] . " " . $row['last_name'];}, 'last_name' => false];
        // 4. ['name', 'first_name', 'last_name', 'email'];
        // TABLE FIELDS VARIABLE STRUCTURE
        // [ 'csv_column' => ['table_column', null], 'csv_column3' => ['table_column3', function($row){ return 'result'; }]];
        foreach ( $columns as $csvColumn => $value ) {
            // Before set the key on variable, trim the column name
            if (is_string($value)) {
                $value = trim($value);
            }
            if (is_numeric($csvColumn) && is_string($value)) { // "EXAMPLE: 3, email", "EXAMPLE 4: FULL ARRAY"
                // cases: 1. FROM header, 2: From mapper as flat element
                // unset the element by key,
                // set the value as the new key
                // unset($columns[$csvColumn]);
                $positionalColumns[$value] = [ $value, null ];
            } elseif (is_string($csvColumn) && is_string($value)) { // "EXAMPLE 3: username"
                $positionalColumns[$csvColumn] = [ $value, null ];
            } elseif (is_string($csvColumn) && is_array($value)) { // "STRUCTURE: csv_column3"
                // forcing to follow ['csv_column3' => ['table_column3', function ($row) { return 'value'; }]]
                if (2 != count($value) || !is_string($value[0]) || !is_callable($value[1])) {
                    // empty table column name on $value[0] will raise exception on database
                    $message = sprintf('Should have exactly two elements [string, callable]. `%s` on %s::$mapper.', is_string($csvColumn) ? $csvColumn : (string) $value, get_class($this));
                    throw new DatasetException($message);
                }
                // TODO: change the hardcoded string
                $positionalColumns[$csvColumn] = [ $value[0], 'callback' /*$value[1]*/ ];
            } elseif (is_string($csvColumn) && is_callable($value)) { // "EXAMPLE: 3, password"
                // TODO: change the hardcoded string
                $positionalColumns[$csvColumn] = [ $csvColumn, 'callback' /*$value*/ ];
            } else {
                $message = sprintf('Invalid `%s` of type <%s> on %s::$mapper.', is_string($csvColumn) ? $csvColumn : (is_callable($value) ? 'CALLABLE on ' . $csvColumn : (string) $value), gettype($value), get_class($this));
                throw new DatasetException($message);
            }
        }

        return $positionalColumns;
    }

    // start importing csv to db
    public function import () {
        $this->broadcastEvent('starting', get_class($this));
        $this->broadcastEvent('prepared-reader', $this->prepareReader());

        $table = $this->table();
        // TODO: uncomment table existence check
        /*if (!$this->checkIfTableExists($table)) {
            throw new DatasetException('Table ' . $table . ' does not exist.');
        }*/
        $this->broadcastEvent('table-exists', $table);

        // check if constant fields exists, and not associative array
        if (!empty($additionalFields = $this->additionalFields()) && !is_multidimensional_array($additionalFields)) {
            throw new DatasetException("Additional fields must be associative array.");
        }

        // check if ignored csv column is flat array or not
        if (($ignoredColumns = $this->ignoredColumns()) && is_multidimensional_array($ignoredColumns)) {
            throw new DatasetException("Ignored CSV Columns cannot be associative array.");
        }

        $columns = $this->getCsvToTableColumns();
        if (empty($columns)) {
            throw new DatasetException("Table fields could not be decided from Headers & Mapper.");
        }

        $columns = $this->validateCSVParser($columns);
        if (empty($columns)) {
            throw new DatasetException("Nothing to import from CSV.");
        }

        var_dump($columns);

        /*
        // insertable table fields are going to be the fields that has NOT FALSE VALUES
        // 1. filter the values if any $mapper value has NOT FALSE values
        $filteredMap = array_filter($mapper);
        // 2. get the database table fields for those csv columns
        $tableColumns = array_map(function ($row) {
            return $row[0];
        }, $filteredMap);
        // 3. Merge those with the constant field values
        $insertAbleTableFields = array_merge(array_values($tableColumns), array_keys($this->getAdditionalFields()));
        // check if the table has fields
        $this->checkIfTableColumnsExist($insertAbleTableFields);
        // build the query with those values
        $this->query = $this->queryBuilder($insertAbleTableFields);

        // prepare the pdo statement
        $statement = $this->connection->getPDO()->prepare($this->query);

        $pagination = 100;
        $current = 0;
        $headerOffset = $this->getExcludeHeader() ? 1 : 0;
        $shouldContinue = true;
        $errorOccurred = false;
        do {
            $totalOffset = $current * $pagination + $headerOffset;
            $resultSet = $this->getReader()
                              ->setOffset($totalOffset)
                              ->setLimit($pagination)
                              ->fetchAssoc(array_keys($mapper));

            // increment the current page to +1
            ++$current;

            // should grab next chunk if the found data set greater than the pagination value
            // fetchAssoc returns an iterator
            $iterator_item_count = iterator_count($resultSet);
            if (0 === $iterator_item_count) {
                break;
            }
            echo sprintf("Loaded %5d%s %d rows.\n", $current, $this->inflector->ordinal($current), $iterator_item_count);

            // row counter for the result
            $onCurrentPageResultCount = 0;

            // loop over the result set
            foreach ( $resultSet as $result ) {
                // get the fields those are required to be taken from
                $matchedKeys = array_intersect_key(array_keys($filteredMap), array_keys($result));
                $values = [];
                ++$onCurrentPageResultCount;
                $currentRowNumber = $totalOffset + $onCurrentPageResultCount;
                $stopCurrentRow = false;
                foreach ( $matchedKeys as $key ) {
                    // ['csv_column' => ['table_column', 'transformer()']]; @ position 1
                    // transform values if required
                    if (is_callable($mapper[$key][1])) {
                        $returnedValue = call_user_func($mapper[$key][1], ...[
                            $result,
                            $currentRowNumber,
                        ]);
                        // user explicitly returned false from callback
                        if (false === $returnedValue) {
                            $stopCurrentRow = true;
                            $errorMessage = sprintf("`{$key}` from %s::\$mapper property explicitly returned bool(false)", get_class($this));
                            $this->errorAlert($errorMessage, $currentRowNumber, array_combine(array_slice($insertAbleTableFields, 0, count($values)), $values));
                            break;
                        }
                        $values[] = $returnedValue;
                    } else {
                        $values[] = $result[$key];
                    }
                }
                if (true === $stopCurrentRow) {
                    continue;
                }

                // check if the user wants to modify any value from current rows from constant fields
                foreach ( $this->getAdditionalFields() as $tableField => $operation ) {
                    // if the user has set callback in constant field
                    // call the function or set the value
                    if (is_callable($operation)) {
                        $returnedValue = call_user_func($operation, ...[
                            $result,
                            $currentRowNumber,
                        ]);
                        // check if user explicitly returned false
                        if (false === $returnedValue) {
                            $stopCurrentRow = true;
                            $errorMessage = sprintf("`{$tableField}` from %s::\$additionalFields property explicitly returned bool(false)", get_class($this));
                            $this->errorAlert($errorMessage, $currentRowNumber, array_combine(array_slice($insertAbleTableFields, 0, count($values)), $values));
                            break;
                        }
                        $values[] = $returnedValue;
                    } else {
                        $values[] = $operation;
                    }
                }
                if (true === $stopCurrentRow) {
                    continue;
                }

                if (!$statement->execute($values)) {
                    $this->errorAlert($statement->errorInfo()[2], $currentRowNumber, array_combine($insertAbleTableFields, $values));
                    $errorOccurred = true;
                    break;
                }
            }
            if ($errorOccurred) {
                break;
            }
        } while ( $shouldContinue );
        echo sprintf("-------------------------------- Import Finished from %s --------------------------------\n", get_class($this));

        return $errorOccurred;*/
    }

    private function errorAlert ($message = '', $row = 0, array $data = []) {
        if ($message) {
            echo sprintf("MESSAGE: %s\n", $message);
        }
        if ($row) {
            echo sprintf("CSV ROW: %d\n", $row);
        }
        if ($data) {
            echo sprintf("VALUES: %s\n", json_encode($data));
        }
        echo PHP_EOL;
    }
}
