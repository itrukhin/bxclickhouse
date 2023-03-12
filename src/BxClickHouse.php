<?php
declare(strict_types=1);
namespace App;

use Bitrix\Main\Data\Connection;
use Bitrix\Main\DB\SqlQueryException;
use Bitrix\Main\Diag;
use ClickhouseClient\Client\Client;
use ClickhouseClient\Client\Config;
use ClickhouseClient\Connector\Connector;

/**
 * @property Client $resource
 */
class BxClickHouse extends Connection {
    
    const DEFAULT_PORT = '8123';

    protected $host;
    protected $port;
    protected $timeout;
    protected $database;
    protected $login;
    protected $password;

    public function __construct(array $configuration) 
    {
        parent::__construct($configuration);

        $this->host = $configuration['host'] ?? '';
        $this->port = $configuration['port'] ?? self::DEFAULT_PORT;
        $this->timeout = $configuration['timeout'] ?? 0;
        $this->database = $configuration['database'] ?? '';
        $this->login = $configuration['login'] ?? '';
        $this->password = $configuration['password'] ?? '';
    }

    protected function connectInternal() {

        if($this->isConnected) {
            return;
        }

        $config = new Config(
            // basic connection information
            ['host' => $this->host, 'port' => $this->port, 'protocol' => 'http'],
            // settings
            ['database' => $this->database],
            // credentials
            ['user' => $this->login, 'password' => $this->password],
            // set curl options
            [CURLOPT_TIMEOUT => $this->timeout]
        );

        $this->resource = new Client($config);
        $this->isConnected = true;
    }

    protected function disconnectInternal() {

    }
}