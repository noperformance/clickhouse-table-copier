# clickhouse-table-copier


##why 

If you need to copy one clickhouse table to another server with DIFFERENT schema

##requirements

source table must have  partition  key

## how to
- env GOOS=linux GOARCH=amd64 go build
- ./clickhouse-table-copier -h

#configs

```
Usage of ./clickhouse-table-copier:
-c, --config string   Path to config file (default "config.yaml") // config file path
-d, --debug           Enable debug // doesnt work atm
-i, --info            Enable information mode // dry-run checks only count/hashes
-s, --sync            Enable copymode // copy mode 
-v, --version         Get version
```

```
source: // source connection from you want to copy
  user: "default"  
  password: ""
  host: "172.18.0.4" //required
  port: 9000 // required
  database: "billing" // required
  table: "new" // required
  skip_verify: true
  secure: false
  key_filename: ""
destination: // destination connection to you  want to copy
  user: "default"
  password: ""
  host: "172.18.0.3"
  port: 9000
  database: "billing"
  table: "cap"
  skip_verify: true
  secure: false
  key_filename: ""

worker_pool: // doesnt work
  num_workers: 10
  num_retry: 50
  chan_len: 100

debug: false // sql debug mode
check_hashes: false  // check by hash or row count
```

## TODO
- [ ] tests
