<?php


namespace Zhlin160\Elasticsearch;



class Elastic
{
    protected $prams = [];

    protected $config = [
        "hosts" => [
            "http://127.0.0.1:9200"
        ],
        "username" => "",
        "password" => "",
//        'logging' => [
//            'enabled' => false,
//            'level' => 'all',
//            'location' => '/logs/elasticsearch.log'
//        ],
    ];

    protected function __construct()
    {
    }

    public static function create()
    {
        return new static();
    }

    public function setConfig(array $config)
    {
        $this->config = array_merge($config, $this->config);
        return $this;
    }

    public function setHosts(array $hosts)
    {
        $this->config["hosts"] = $hosts;
        return $this;
    }

    public function setAuth(string $username, string $password )
    {
        $this->config["username"] = $username;
        $this->config["password"] = $password;
        return $this;
    }

    public function setLogging(array $logging)
    {
        $this->config["logging"] = $logging;
        return $this;
    }

    public function setId(string $id)
    {
        $this->prams["id"] = $id;
        return $this;
    }

    public function setIndex(string $index)
    {
        $this->prams["index"] = $index;
        return $this;
    }

    /**
     * @return ElasticSearch
     * @throws \Exception
     */
    public function build()
    {
        $elastic = new ElasticSearch();
        if (empty($this->config)) {
            throw new \Exception("请设置ElasticSearch配置");
        }
        return $elastic->make($this->config, $this->prams);
    }
}