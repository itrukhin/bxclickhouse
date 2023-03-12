<?php
declare(strict_types=1);
namespace App;

use Bitrix\Main\Data\Connection;
use Bitrix\Main\DB\SqlQueryException;
use Bitrix\Main\Diag;
use GuzzleHttp\Client;
use GuzzleHttp\Handler\CurlHandler;
use GuzzleHttp\HandlerStack;

/**
 * @property Client $resource
 */
class BxClickkHouse extends Connection {
    
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

        $handler = new CurlHandler();
        $stack = HandlerStack::create($handler);

        $httpClientSettings = [
            'base_uri' => $this->host . ':' . $this->port,
            'timeout' => $this->timeout,
            'handler' => $stack,
        ];

        if (null !== $this->login) {
            $httpClientSettings['auth'] = [$this->login, $this->password];
        }

        $this->resource = new Client($httpClientSettings);
    }

    protected function disconnectInternal() {

    }

    protected function queryInternal($sql, array $binds = null, Diag\SqlTrackerQuery $trackerQuery = null)
    {
        $this->connectInternal();

        if ($trackerQuery != null)
        {
            $trackerQuery->startQuery($sql, $binds);
        }

        $response = $this->resource->request('POST', null, [
            'body' => $sql,
        ]);
        $result = $response->getBody()->getContents();

        if ($trackerQuery != null)
        {
            $trackerQuery->finishQuery();
        }

        $this->lastQueryResult = $result;

        if (!$result)
        {
            throw new SqlQueryException('ClickHouse query error', $this->getErrorMessage(), $sql);
        }

        return $result;
    }

    protected function getErrorMessage()
    {
        return "";
    }
}