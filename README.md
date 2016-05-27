kue-jcli
===================================
基于kue的json-api的cli

```
  Options:

    -h, --help            output usage information
    -V, --version         output the version number
    -d --database <root>  json api root
    -s --state [name]     active inactive failed complete delayed
    -t --type [name]      kue type name
    --slice [x..y]        default 0..999
    -q --query [json]     after get jobs, filter eg. {"data.uid": 12312321}
    --delete              after query, delete job by id
    --change [state]      after query, change job state
    --copy [url]          clone job to other kue(endWith "/job")
```
