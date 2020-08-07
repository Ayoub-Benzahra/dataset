<?php

use Illuminate\Database\Capsule\Manager;

class CsvStorageTest extends BaseTestClass
{
    protected function tearDown () : void {
        parent::tearDown();
        // delete the generated csv files
        $files = [
            __DIR__ . '/Providers/company.csv',
            __DIR__ . '/Providers/companies.csv',
            __DIR__ . '/Providers/members.csv',
        ];
        foreach ( $files as $file ) {
            if (file_exists($file)) {
                unlink($file);
            }
        }

        // reset states
        BaseCsvStorageProvider::$TYPE = '';
        BaseCsvStorageProvider::$HEADER_OFFSET = 0;
        BaseCsvStorageProvider::$SKIP_EMPTY = true;
        BaseCsvStorageProvider::$EXIT_ON_ERROR = true;
        BaseCsvStorageProvider::$STREAM_FILTERS = [];
        BaseCsvStorageProvider::$LIMIT = 20;
        BaseCsvStorageProvider::$USE_TRANSACTION = true;
        BaseCsvStorageProvider::$ENTRIES = [];
        BaseCsvStorageProvider::$HEADERS = [];
        BaseCsvStorageProvider::$CONNECTION = 'default';
        BaseCsvStorageProvider::$TABLE = '';
        BaseCsvStorageProvider::$FILENAME = '';
        BaseCsvStorageProvider::$DELIMITER = ',';
        BaseCsvStorageProvider::$EXCEPTION_RECEIVED = false;
        BaseCsvStorageProvider::$FILE_OPEN_MODE = 'r';
    }

    protected function getCompanyProvider () {
        return (new CompanyProvider($this->container))->addMutation(function ($record) {
            return [ 'slug' => preg_replace('/[^a-z0-9]/i', '-', $record['address']) ];
        })->addFilter(function ($record) {
            unset($record['address']);

            return $record;
        });
    }

    protected function getMemberProvider () {
        return new Member($this->container);
    }

    protected function generateCompaniesData (array $config = []) {
        $filename = $config['name'] ?? __DIR__ . '/Providers/companies.csv';
        $delimiter = $config['delimiter'] ?? ',';
        $emptyLine = $config['empty_line'] ?? false;
        $modulo = $config['modulo'] ?? 8;
        $lines = $config['lines'] ?? 10;
        $skipHeader = $config['skip_header'] ?? false;
        $locale = $config['locale'] ?? 'en_US';
        $faker = $this->getFaker($locale);

        $headers = [ 'name', 'address', 'image_url' ];
        if ($skipHeader) {
            $data = [];
            $rows = [];
        } else {
            $data = [ $headers ];
            $rows = [ implode($delimiter, $headers) ];
        }
        foreach ( range(1, $lines) as $i ) {
            $row = [
                $faker->name,
                $faker->streetAddress,
                $faker->imageUrl(),
            ];

            $data[] = $row;
            $rows[] = implode($delimiter, $row);
            if ($emptyLine && ($i % $modulo === 0)) {
                $data[] = [];
                $rows[] = '';
            }
        }

        file_put_contents($filename, implode(PHP_EOL, $rows));

        return $data;
    }

    protected function generateMembersData (array $config = []) {
        $filename = $config['name'] ?? __DIR__ . '/Providers/members.csv';
        $delimiter = $config['delimiter'] ?? ',';
        $emptyLine = $config['empty_line'] ?? false;
        $modulo = $config['modulo'] ?? 8;
        $lines = $config['lines'] ?? 10;
        $skipHeader = $config['skip_header'] ?? true;
        $locale = $config['locale'] ?? 'en_US';
        $faker = $this->getFaker($locale);

        $headers = [ 'name', 'age', 'company', 'address', 'phone', 'email', ];
        if ($skipHeader) {
            $data = [];
            $rows = [];
        } else {
            $data = [ $headers ];
            $rows = [ implode($delimiter, $headers) ];
        }
        foreach ( range(1, $lines) as $i ) {
            $row = [
                $faker->name,
                $faker->randomNumber(2),
                $faker->company,
                $faker->streetAddress,
                $faker->e164PhoneNumber,
                $faker->companyEmail,
            ];

            $data[] = $row;
            $rows[] = implode($delimiter, $row);
            if ($emptyLine && ($i % $modulo === 0)) {
                $data[] = [];
                $rows[] = '';
            }
        }

        file_put_contents($filename, implode(PHP_EOL, $rows));

        return $data;
    }

    public function testEventDispatcherIsWorking () {
        $this->addEventListener('test-true', function ($eventName) {
            return $eventName === 'test-true';
        });

        $this->addEventListener('test-count', function (...$payload) {
            return count($payload) === 3;
        });

        $this->assertTrue($this->dispatcher->until('test-true', 'test-true'));
        $this->assertTrue($this->dispatcher->until('test-count', [ 1, true, 'false' ]));
        $this->assertFalse($this->dispatcher->until('test-count'));
        $this->assertTrue(is_null($this->dispatcher->until('no-event-listener')));
    }

    public function testProvidingEventDispatcherFromOutside () {
        $company = $this->getCompanyProvider();
        $company->setEventDispatcher($this->dispatcher);
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });

        // call the import method to start execution
        $result = $company->import();

        $this->assertTrue(false === $result);
    }

    public function testCheckIfEventsAreFired () {
        // return false exists program early
        $this->addEventListener('dataset.reader.starting', function () {
            return false;
        });
        $received = false;

        // as returned false, it'll shoot exiting
        $this->addEventListener('dataset.reader.exiting', function () use (&$received) {
            $received = true;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($received);
    }

    public function testRenamingEventNames () {
        // change the type of the event
        BaseCsvStorageProvider::$TYPE = 'reading-type';
        // return false exists program early
        $receivedNewType = false;
        $this->addEventListener('dataset.reading-type.starting', function () use (&$receivedNewType) {
            $receivedNewType = true;
        });

        $this->addEventListener('dataset.reading-type.preparing_reader', function () {
            return false;
        });

        // call the import method to start execution
        $result = $this->getCompanyProvider()->import();

        $this->assertTrue(false === $result);
        $this->assertTrue($receivedNewType);
    }

    public function testDifferentCsvFilename () {
        $config = [
            'name' => __DIR__ . '/test_company.csv',
        ];

        $this->generateCompaniesData($config);
        BaseCsvStorageProvider::$FILENAME = $config['name'];
        $event = $this->formatEventName('preparing_reader');
        $filenameMatches = false;
        $this->addEventListener($event, function (...$payload) use (&$filenameMatches, $config) {
            if (isset($payload[1]) && $payload[1] == $config['name']) {
                $filenameMatches = true;
            }
        });

        $result = $this->getCompanyProvider()->import();

        $this->assertTrue($filenameMatches);
        $this->assertTrue($result);

        // delete file
        unlink($config['name']);
    }

    public function testDifferentTable () {
        $count = 15;
        $this->generateCompaniesData([ 'lines' => $count ]);
        $table = 'company';
        BaseCsvStorageProvider::$TABLE = $table;
        $result = $this->getCompanyProvider()->import();
        $this->assertTrue($result);

        $this->assertTrue(Manager::table($table)->count() == $count);
    }

    public function testDifferentDBConnection () {
        $count = 25;
        $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$CONNECTION = 'sqlite';
        $this->getCompanyProvider()->import();

        $this->assertTrue(Manager::connection('sqlite')->table('companies')->count() == $count);
    }

    public function testReadDifferentCsvDelimiter () {
        $count = 10;
        $this->generateCompaniesData([ 'lines' => $count, 'delimiter' => '|' ]);
        BaseCsvStorageProvider::$DELIMITER = '|';
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [
                'slug' => str_replace(' ', '-', strtolower($this->getFaker()->sentence())),
            ];
        })->import();

        $this->assertTrue(Manager::table('companies')->count() == $count);
    }

    public function testStreamFilters () {
        $count = 10;
        $data = $this->generateCompaniesData([ 'lines' => $count ]);
        BaseCsvStorageProvider::$STREAM_FILTERS = [ 'string.toupper' ];
        $this->getCompanyProvider()->addMutation(function ($record) {
            return [ 'slug' => preg_replace('/[^a-z0-9]/i', '-', $record['ADDRESS']) ];
        })->addFilter(function ($record) {
            return [
                'name'      => $record['NAME'],
                'image_url' => $record['IMAGE_URL'],
                'slug'      => $record['NAME'],
            ];
        })->import();

        $this->assertTrue(Manager::table('companies')->find(1)->name == strtoupper($data[1][0]));
    }

    public function testFileOpenMode () {
        $this->generateCompaniesData();
        BaseCsvStorageProvider::$FILE_OPEN_MODE = 'r+';

        $this->assertTrue($this->getCompanyProvider()->import());
    }
}