<?php
declare(strict_types=1);
namespace App;

use Bitrix\Main\Data\Connection;
use Hyvor\Clickhouse\Clickhouse;
use Hyvor\Clickhouse\Exception\ClickhouseException;
use Hyvor\Clickhouse\Result\ResultSet;

/**
 * @property Clickhouse $resource
 */
class BxClickHouse extends Connection {

    const DEFAULT_PORT = '8123';

    protected string $host;
    protected string $port;
    protected string $database;
    protected string $login;
    protected string $password;
    protected array $options;

    public function __construct(array $configuration)
    {
        parent::__construct($configuration);

        $this->host = $configuration['host'] ?? '';
        $this->port = $configuration['port'] ?? self::DEFAULT_PORT;
        $this->database = $configuration['database'] ?? '';
        $this->login = $configuration['login'] ?? '';
        $this->password = $configuration['password'] ?? '';

        $this->connectInternal();
    }

    public function ping() {

        $this->connectInternal();
        return $this->resource->ping();
    }

    public function pingThrow() {

        $this->connectInternal();
        $this->resource->pingThrow();
    }

    /**
     * @param string $table
     * @param array<string, string> $columns
     * @param array<string, mixed> ...$rows
     * @return mixed
     * @throws \Hyvor\Clickhouse\Exception\ClickhousePingException
     */
    public function insert(string $table, array $columns, ...$rows) : mixed {

        $this->connectInternal();
        $this->resource->pingThrow();

        $args = [];
        $args[] = $table;
        $args[] = $columns;
        foreach($rows as $row) {
            $args[] = $row;
        }

        return call_user_func_array([$this->resource, 'insert'], $args);
    }

    /**
     * @param string $table
     * @param array $columns
     * @param array $rows
     * @return mixed
     * @throws \Hyvor\Clickhouse\Exception\ClickhousePingException
     */
    public function insertArray(string $table, array $columns, array $rows) : mixed {

        $this->connectInternal();
        $this->resource->pingThrow();

        $args = [];
        $args[] = $table;
        $args[] = $columns;
        foreach($rows as $row) {
            $args[] = $row;
        }

        return call_user_func_array([$this->resource, 'insert'], $args);
    }

    /**
     * @param string $query
     * @param array<string, mixed> $bindings
     * @return ResultSet
     * @throws \Hyvor\Clickhouse\Exception\ClickhouseHttpQueryException
     * @throws \Hyvor\Clickhouse\Exception\ClickhousePingException
     */
    public function select(string $query, array $bindings = []) : ResultSet {

        $this->connectInternal();
        $this->resource->pingThrow();

        return $this->resource->select($query, $bindings);
    }

    public function query(string $query, array $bindings = []) : mixed {

        $this->connectInternal();
        $this->resource->pingThrow();

        return $this->resource->query($query, $bindings);
    }

    public function sessionId() : string {

        return $this->resource->sessionId();
    }

    protected function connectInternal() {

        if($this->isConnected) {
            return;
        }

        $this->resource = new Clickhouse(
            host: $this->host,
            port: (int) $this->port,
            user: $this->login,
            password: $this->password,
            database: $this->database
        );

        $this->isConnected = true;
    }

    protected function disconnectInternal() {

    }
}