# clickhouse-table-copier



## краткая инфа

Необходим бинарь и config файл

```
Usage of ./clickhouse-table-copier:
-c, --config string   Path to config file (default "config.yaml") // путь к конфигу
-d, --debug           Enable debug // пока нерабочая функция
-i, --info            Enable information mode // драй ран покажет хэши партишенов и скажет какие нужно копировать или удалять
-s, --sync            Enable copymode // режим копирования данных
-v, --version         Get version
```

```
source: // подключение откуда будем копировать данные
  user: "default"  
  password: ""
  host: "172.18.0.4" //обязательное
  port: 9000 // обязательное
  database: "billing" // обязательное
  table: "new" // обязательное
  skip_verify: true
  secure: false
  key_filename: ""
destination: // подключение куда копируем 
  user: "default"
  password: ""
  host: "172.18.0.3"
  port: 9000
  database: "billing"
  table: "cap"
  skip_verify: true
  secure: false
  key_filename: ""

worker_pool: // в стадии разработки, пока ничего не вносит
  num_workers: 10
  num_retry: 50
  chan_len: 100

debug: false //включает sql debug
```

## TODO
- [x] реализация реконекта при обрыве соединения
- [x] основной функционал синка таблиц по партициям
- [x] дработка режима dry-run
- [ ] написание тестов
