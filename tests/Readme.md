### 使用 docker-compose 来测试 ESM
- 1.有 es_src + es_dst 两个集群, 因此可以很方便的单独重启初始化
  - start es 
    ```shell
       docker-compose -f src/src-cluster-es7.yml up
       docker-compose -f src/src-single-es5.yml up
       docker-compose -f src/src-single-es7.yml up
    
       docker-compose -f dst/dst-cluster-es7.yml up
       docker-compose -f dst/dst-single-es7.yml up
    ```
  
  - 启动以后可访问:
    - src: http://localhost:9201
    - dst: http://localhost:19201

- 2.src 中添加数据(TODO: 分区数)
  ```shell
    curl -H "Content-Type: application/json" -X PUT http://localhost:9201/account -d "{\"settings\":{\"number_of_shards\":2}}"
  ```
  
  - es5:
    ```shell
      curl -H "Content-Type: application/json" -X POST "http://localhost:9201/account/account/_bulk?pretty" --data-binary  "@tests/accounts.json"
    ```
  - es7:
    ```shell
      curl -H "Content-Type: application/json" -X POST "http://localhost:9201/account/_doc/_bulk?pretty" --data-binary  "@tests/accounts.json"
    ```
  
- 3.使用 esm 进行同步
  - es5 to es7
  ```shell
    ..\esm --sync --ssort=account_number -c 10000 -v debug -s http://127.0.0.1:9201 -x account -d http://127.0.0.1:19201 -y account -u "_doc"
       --source_proxy=http://localhost:8888 --dest_proxy=http://localhost:8888
  ```  

  - es7 to es7
  ```shell
    ..\esm --sync --shards=2 -c 10000 -v debug --copy_settings --copy_mappings 
      -s http://127.0.0.1:9201 -x account -d http://127.0.0.1:19201 -y account -u "_doc"
      --source_proxy=http://localhost:8888 --dest_proxy=http://localhost:8888
  ```
  - 导出数据进行比较
  ```shell
    esm --source=http://localhost:9201 --ssort=account_number --src_indexes=account --truncate_output --skip=_index,_type,sort --output_file=src.json
    esm --source=http://localhost:19201 --ssort=account_number --src_indexes=account --truncate_output --skip=_index,_type,sort --output_file=dst.json
    diff -W 200 -ry --suppress-common-lines src.json dst.json
  ```
   

### 常见问题:
- Fielddata access on the _uid field is disallowed
  - 参考: https://stackoverflow.com/questions/45427887/how-to-get-the-maximum-id-value-in-elasticsearch
  - 原因: ES5 中似乎不允许通过 _id 排序, 必须通过 _uid 或自定义的 field 来排序? 通过 --ssort=_uid 或 其他字段. es7 后只能用 _id . 6 呢?
-