<?php


namespace Zhlin160\Elasticsearch;


use Elastic\Elasticsearch\ClientBuilder;
use Monolog\Handler\StreamHandler;
use Monolog\Logger;
use Zhlin160\Elasticsearch\Support\Arr;
use Zhlin160\Elasticsearch\Support\Collection;

class ElasticSearch
{
    protected $model;
    protected $elasticsearch;
    protected $_index = 'goods';
    protected $_id = 'id';
    protected $_type = '_doc';
    protected $limit = 15;
    protected $sort = [];
    protected $ignores = [];
    protected $attributesToHighlight = [];
    protected $searchable = [];
    protected $facetsDistribution = [];
    protected $query = '';
    protected $flush_index = true;//立即刷新索引
    protected $fuzzy = true;//开启模糊搜索
    protected $auto_synonyms = true; //开启自动同义词搜索功能

    protected $wheres = [];
    protected $page = 1; //记录第几页

    /**
     * 初始化
     * @param $options
     * @param $other
     * @return \Zhlin160\Elasticsearch\ElasticSearch
     */
    public function make($config, $other)
    {
        if(isset($other['id'])){
            $this->_id = $other['id'];
        }
        if(isset($other['index'])){
            $this->_index = $other['index'];
        }
        if(isset($other['type'])){
            $this->_type = $other['type'];
        }
        // Instantiate a new ClientBuilder
        $clientBuilder = ClientBuilder::create();
        $clientBuilder->setHosts($config["hosts"]);

        if (!empty($config["logging"])) {
            $clientBuilder = $this->configureLogging($clientBuilder, $config);
        }

        if (isset($config["username"]) && isset($config["password"])) {
            $clientBuilder = $clientBuilder->setBasicAuthentication($config["username"], $config["password"]);
        }
        // Build the client object
        $connection = $clientBuilder->build();
        $this->elasticsearch = $connection;
        return $this;
    }

    /**
     * 设置模型
     * @param $model
     * @return $this
     */
    public function model($model)
    {
        $this->model = $model;
        return $this;
    }

    /**
     * 返回实例
     * @return mixed
     */
    public function us()
    {
        return $this->elasticsearch;
    }

    /**
     * @param ClientBuilder $clientBuilder
     * @param array $config
     * @return ClientBuilder
     */
    private function configureLogging(ClientBuilder $clientBuilder, array $config)
    {
        if (Arr::get($config, 'logging.enabled')) {
            $logger = new Logger('name');
            $logger->pushHandler(new StreamHandler(Arr::get($config, 'logging.location'), Arr::get($config, 'logging.level', 'all')));
            $clientBuilder->setLogger($logger);
        }
        return $clientBuilder;
    }

    /**
     * @param $name
     * @return $this
     */
    public function index($name)
    {
        $this->_index = $name;
        return $this;
    }

    /**
     * 创建索引
     * @param array $body
     * eg:[
        'settings' => [
            'number_of_shards' => 3,
            'number_of_replicas' => 2
            ],
        'mappings' => [
            '_source' => [
            'enabled' => true
            ],
            'properties' => [
            'first_name' => [
            'type' => 'keyword'
            ],
            'age' => [
            'type' => 'integer'
            ]
        ]
        ]
    ]
     * @return mixed
     */
    public function createIndex(array $body)
    {
        return $this->elasticsearch->indices()->create($body)->asArray();
    }

    /**
     * 更新设置
     * @param array $body
     * eg :
     *  [
        'settings' => [
                'number_of_replicas' => 0,
                'refresh_interval' => -1
            ]
        ]
     * @return mixed
     * @throws \Exception
     */
    public function updateSettings(array $body)
    {
        if (!isset($body["settings"]) || !empty($body["settings"])) {
            throw new \Exception("settings 设置不能为空");
        }
        $params = [
            'index' => $this->_index,
            'body' => $body
        ];
        return $this->elasticsearch->indices()->putSettings($params)->asArray();
    }

    /**
     * 获取设置
     * @param string|array $index
     * @return mixed
     */
    public function getSettings($index = "")
    {
        if ("" == $index) {
            $index = $this->_index;
        }
        $params = [
            "index" => $index
        ];
        return $this->elasticsearch->indices()->getSettings($params)->asArray();
    }

    /**
     * 设置属性映射
     * @param array $properties
     * eg:
     *    [
            'first_name' => [
            'type' => 'text',
            'analyzer' => 'standard'
            ],
            'age' => [
            'type' => 'integer'
            ]
    ]
     * @return mixed
     */
    public function putMapping(array $properties)
    {
        $params = [
            "index" => $this->_index,
            'body' => [
                '_source' => [
                    'enabled' => true
                ],
                'properties' => $properties
            ]
        ];
        return $this->elasticsearch->indices()->putMapping($params)->asArray();
    }

    public function getMapping($index = "")
    {
        if ("" == $index) {
            $index = $this->_index;
        }
        $params = [
            "index" => $index
        ];
        return $this->elasticsearch->indices()->getMapping($params)->asArray();
    }

    /**
     * 设置模糊搜索
     * @param $val
     * @return $this
     */
    public function fuzzy($val = true)
    {
        $this->fuzzy = $val;
        return $this;
    }

    /**
     * 查询关键词
     * @param $keywords
     * @return ElasticSearch
     */
    public function q($keywords)
    {
        $this->query = $keywords;
        return $this;
    }

    /**
     * 设置查询显示的属性
     * @param string[] $attributes
     */
    public function select($attributes = [])
    {
        $this->searchable = $attributes;
        return $this;
    }

    /**
     * 设置排序
     * @param $column
     * @param $rank
     * @return $this
     */
    public function orderBy($column, $rank = 'asc')
    {
        $this->sort[] = [$column => $rank];
        return $this;
    }

    /**
     * 新增更新文档
     * @param $data
     * @return void
     */
    public function create($data)
    {
        if (!is_array($data)) {
            throw new \Exception('你的索引参数不是一个数组');
        }
        if (isset($data[0]) && is_array($data[0])) {
            $list = new Collection([]);
            // 多维数组
            foreach ($data as $v) {
                $params = [
                    'index' => $this->_index,
                    'id' => $v[$this->_id],
                    'client' => ['ignore' => $this->ignores],
                    'body' => $v
                ];
                $created =  $this->elasticsearch->index($params)->asArray();
                $list->push($created);
            }
            return $list;
        } else {
            // 一维数组
            $params = [
                'index' => $this->_index,
                'id' => $data[$this->_id],
                'client' => ['ignore' => $this->ignores],
                'body' => $data
            ];
            return $this->elasticsearch->index($params)->asArray();
        }
    }

    /**
     * 更新文档
     * @param $data
     * @return void
     * @throws \Exception
     */
    public function update($data)
    {
        if (!is_array($data)) {
            throw new \Exception('你的索引参数不是一个数组');
        }
        if (isset($data[0]) && is_array($data[0])) {
            $list = new Collection([]);
            // 多维数组
            foreach ($data as $v) {
                $params = [
                    'index' => $this->_index,
                    'id' => $v[$this->_id],
                    'client' => ['ignore' => $this->ignores],
                    'body' => ['doc' => $v],
                ];
                $updated =  $this->elasticsearch->update($params)->asArray();
                $list->push($updated);
            }
            return $list;
        } else {
            // 一维数组
            $params = [
                'index' => $this->_index,
                'id' => $data[$this->_id],
                'client' => ['ignore' => $this->ignores],
                'body' => ['doc' => $data],
            ];
            return $this->elasticsearch->update($params)->asArray();
        }
    }

    /**
     * 删除指定文档
     * @param $ids
     * @return array
     * @throws \Exception
     */
    public function destroy($ids)
    {
        if (!$ids) {
            throw new \Exception('索引主键不能为空');
        }
        if(is_array($ids)){ //批量删除
            foreach ($ids as $id){
                $params = [
                    'index' => $this->_index,
                    'id'    => $id
                ];
                $this->elasticsearch->delete($params);
            }
        }else{ //单个删除
            $params = [
                'index' => $this->_index,
                'id'    => $ids
            ];
            return $this->elasticsearch->delete($params)->asArray();
        }
    }


    /**
     * 清除索引内容
     */
    public function clear()
    {
        $params = [
            'index' => $this->_index,
            'client' => ['ignore' => $this->ignores]
        ];
        return $this->elasticsearch->indices()->delete($params)->asArray();
    }

    /**
     * 设置查询数量
     * @param $limit
     * @return $this
     */
    public function limit(int $limit)
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * 高亮查询
     * @param array $attributes
     * @return $this
     */
    public function highlight(array $attributes = [])
    {
        $this->attributesToHighlight = $attributes;
        return $this;
    }

    public function facets(array $attributes = ['*'])
    {
        $this->facetsDistribution = $attributes;
        return $this;
    }

    /**
     * 获取数据
     * @return void
     */
    public function get()
    {
        return $this->performSearch();
    }

    /**
     * 获取指定编号文档
     * @param $id
     * @return mixed
     */
    public function first($id)
    {
        $params = [
            'index' => $this->_index,
            'id'    => $id
        ];
        return  $this->getFirst($this->elasticsearch->get($params)->asArray());
    }

    /**
     * Perform the given search on the engine.
     *
     * @param int $page
     * @return mixed
     */
    public function paginate(int $page = 1)
    {
        $filters = [
            'offset' => ($page - 1) * $this->limit,
        ];
        $this->page = $page;
        return $this->performSearch($filters);
    }

    /**
     * 获取建议词
     * @return void
     */
    public function suggest($showNum = false)
    {
        $query = [
            'index' => $this->_index,
            'type'=> $this->_type,
            'suggest_field' => 'goods_name',
            'suggest_text' => $this->query,
            'suggest_mode' => 'always',
            'suggest_size' => 20,
//            'body' => [
//                'suggest' => [
//                    'song-suggest' => [
//                        'prefix' => $this->query,
//                        'completion' => [
//                          'field'=>'goods_mame.py'
//                        ]
//                    ]
//                ]
//            ]
        ];
        $result = $this->elasticsearch->search($query);
        return $result->asArray();
    }

    /**
     * Perform the given search on the engine.
     *
     * @return mixed
     */
    protected function performSearch(array $searchParams = [])
    {
        $query = [
            'index' => $this->_index,
            'type' => $this->_type,
        ];

        $query["body"] = $this->filters();
        $query["from"] = $searchParams['offset'] ?? 0;
        $query["size"] = $this->limit;

        if (count($this->ignores)) {
            $query["client"] = ['ignore' => $this->ignores];
        }


        //  $query["search_type"] = '';
        // $query["scroll"] = $scroll;

        $result = $this->elasticsearch->search($query);
        return $this->getAll($result->asArray());
    }
    /**
     * Retrieve only first record
     * @param array $result
     */
    protected function getFirst($result = [])
    {

        if (array_key_exists("_source", $result)) {
            $model = new Collection($result["_source"]);
            // match earlier version
            $model->_index = $result["_index"];
            $model->_type = $result["_type"];
            $model->_id = $result["_id"];
            $model->_score = $result["_score"] ?? [];
            $model->_highlight = isset($result["highlight"]) ? $result["highlight"] : [];
            $new = $model;
        } else {
            $new = NULL;
        }

        return $new;
    }
    /**
     * 数据整理
     * @param $result
     */
    protected function getAll($result = []){
        if (array_key_exists("hits", $result)) {
            $new = [];
            foreach ($result["hits"]["hits"] as $row) {
                $model = new Collection($row["_source"]);
                // match earlier version
                $model->_index = $row["_index"];
                $model->_type = isset($row["_type"]) ? $row["_type"] : 'doc';
                $model->_id = $row["_id"];
                $model->_score = $row["_score"];
                $model->_highlight = isset($row["highlight"]) ? $row["highlight"] : [];
                $new[] = $model;
            }
            $collect = new Collection([]);
            $collect->items = $new;
            $total = $result["hits"]["total"];
            $collect->total = is_array($total) ? $total["value"] : $total;
            $collect->page = $this->page ?? 1;
            $collect->max_score = $result["hits"]["max_score"];
            $collect->took = $result["took"];
            $collect->timed_out = $result["timed_out"];
            $collect->scroll_id = isset($result["_scroll_id"]) ? $result["_scroll_id"] : NULL;
            $collect->shards = (object)$result["_shards"];
            return $collect;
        } else {
            return new Collection([]);
        }
    }


    /**
     * 可以是=, !=, >, >=, <, 或<=
     */
    public function where($column, $operator = null, $value = null, string $boolean = 'AND')
    {
        if ($this->invalidOperator($operator)) {
            [$value, $operator] = [$operator, '='];
        }
        $type = 'Basic';
        $this->wheres[] = compact('type', 'column', 'operator', 'value', 'boolean');
        return $this;
    }

    /**
     * @param $column
     * @param $operator
     * @param $value
     * @return $this
     */
    public function orWhere($column, $operator = null, $value = null)
    {
        return $this->where($column, $operator, $value, 'OR');
    }

    /**
     * 区间
     * @param $column
     * @param array $values
     * @param string $boolean
     * @return $this
     */
    public function whereBetween($column,array $values, string $boolean = 'AND')
    {
        $type = 'Between';
        $this->wheres[] = compact('type', 'column', 'values', 'boolean');
        return $this;
    }

    /**
     * @param $column
     * @param array $values
     * @param string $boolean
     * @param bool $not
     * @return $this
     */
    public function whereIn($column,array $values, string $boolean = 'AND', bool $not = false)
    {
        $type = $not ? 'NotIn' : 'In';

        $this->wheres[] = compact('type', 'column', 'values', 'boolean');
        return $this;
    }
    /**
     *
     * @param string $column
     * @param  mixed  $values
     * @return $this
     */
    public function orWhereIn(string $column, array $values)
    {
        return $this->whereIn($column, $values, 'OR');
    }

    /**
     * Add a "where not in" clause to the query.
     *
     * @param string $column
     * @param  mixed  $values
     * @param string $boolean
     * @return $this
     */
    public function whereNotIn(string $column,array $values, string $boolean = 'AND')
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    /**
     * Add an "or where not in" clause to the query.
     *
     * @param string $column
     * @param  mixed  $values
     * @return $this
     */
    public function orWhereNotIn(string $column, $values)
    {
        return $this->whereNotIn($column, $values, 'OR');
    }
    /**
     * Add a raw where clause to the query.
     *
     * @param  string  $sql
     * @param  mixed  $bindings
     * @param  string  $boolean
     * @return $this
     */
    public function whereRaw($sql,  $boolean = 'AND')
    {
        $this->wheres[] = ['type' => 'Raw', 'sql' => $sql, 'boolean' => $boolean];
        return $this;
    }

    /**
     *
     * @param $operator
     * @return bool
     */
    protected function invalidOperator($operator)
    {
        return ! is_string($operator) || ! in_array(strtolower($operator), ['=', '!=', '>', '>=', '<', '<=','like'], true);
    }


    /**
     * 数据整理
     * @return array|string
     */
    protected function filters()
    {

        $body = [];
        //collect($this->wheres)->map(function ($item, $key) use (&$body) {
        (new Collection($this->wheres))->map(function ($item, $key) use (&$body) {
            switch ($item['type']) {
                case 'Basic':
                    if ($item['operator'] == "=") {
                        $body["query"]["bool"]["filter"][] = ["term" => [$item['column'] => $item['value']]];
                    }
                    if ($item['operator'] == ">") {
                        $body["query"]["bool"]["filter"][] = ["range" => [$item['column'] => ["gt" => $item['value']]]];
                    }
                    if ($item['operator'] == ">=") {
                        $body["query"]["bool"]["filter"][] = ["range" => [$item['column'] => ["gte" => $item['value']]]];
                    }
                    if ($item['operator'] == "<") {
                        $body["query"]["bool"]["filter"][] = ["range" => [$item['column'] => ["lt" => $item['value']]]];
                    }
                    if ($item['operator'] == "<=") {
                        $body["query"]["bool"]["filter"][] = ["range" => [$item['column'] => ["lte" => $item['value']]]];
                    }
                    if ($item['operator'] == "like") {
                        $body["query"]["bool"]["must"][] = ["match" => [$item['column'] => $item['value']]];
                    }
                    if ($item['operator'] == "exists") {
                        if (!$item['value']) {
                            $body["query"]["bool"]["must"][] = ["exists" => ["field" => $item['column']]];
                        } else {
                            $body["query"]["bool"]["must_not"][] = ["exists" => ["field" => $item['column']]];
                        }
                    }
                    break;
                case 'In':
                    $body["query"]["bool"]["filter"][] = ["terms" => [$item['column'] => $item['value']]];
                    break;
                case 'NotIn':
                    $body["query"]["bool"]["must_not"][] = ["terms" => [$item['column'] => $item['value']]];
                    break;
                case 'Raw':

                    break;
                case 'Between':
                    $body["query"]["bool"]["filter"][] = ["range" => [$item['column'] => ["gte" => $item['values'][0], "lte" => $item['values'][1]]]];
                    break;
                case 'NotBetween':
                    $body["query"]["bool"]["must_not"][] = ["range" => [$item['column'] => ["gte" => $item['values'][0], "lte" => $item['values'][1]]]];
                    break;

            }
        });
        if (count($this->searchable)) {
            $_source = array_key_exists("_source", $body) ? $body["_source"] : [];
            $body["_source"] = array_merge($_source, $this->searchable);
        }
        if(!empty($this->query)){
            $body["query"]["bool"]["must"][] = [
                "query_string" => ["query" => $this->query]
            ];
        }
        $body["query"] = isset($body["query"]) ? $body["query"]: [];
        if(count($body["query"]) == 0){
            unset($body["query"]);
        }

        if (count($this->sort)) {
            $sortFields = array_key_exists("sort", $body) ? $body["sort"] : [];
            $body["sort"] = array_unique(array_merge($sortFields, $this->sort), SORT_REGULAR);
        }
        return $body;
    }
}