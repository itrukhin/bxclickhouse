<?php
declare(strict_types=1);
namespace App;

use Bitrix\Main\Data\Connection;
use Hyvor\Clickhouse\Clickhouse;

/**
 * @property Clickhouse $resource
 */
class BxClickHouse extends Connection {
    
    const DEFAULT_PORT = '8123';

    protected string $host;
    protected string $port;
    protected string $timeout;
    protected string $database;
    protected string $login;
    protected string $password;
    protected array $options;

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

        $this->resource = new Clickhouse(
            host: $this->host,
            port: $this->port,
            user: $this->login,
            password: $this->password,
            database: $this->database
        );

        $this->isConnected = true;
    }

    protected function disconnectInternal() {

    }
}