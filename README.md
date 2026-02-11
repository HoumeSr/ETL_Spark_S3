# ETL с Spark и Minio
В первую очередь требуется скачать модули для работы Spark и Minio
```bash
make jars-download
```
Или можно скачать по сслыкам \
[aws](https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar) \
[hadoop](https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar)\
И вручную перенести в корневую папку jars\
Чтобы поднять инфраструктуру можно использовать:
```bash
make up
```
Можно запустить тест на работу SQL-запроса командой:
```bash
make test
```

Одной из команд ниже можно сгенерировать данные для таблиц order, store и user. 
Это делается, чтобы быстро можно было проверить всё необходимое для работы ETL-процесса.

```bash
make generate
```

Они должны находиться в директории  
```s3a://warehouse/parquet/```

Чтобы создать таблицу результата, требуется написать
```bash
make etl
```
Она будет находится в той же директории, что и исходные данные.

Можно посмотреть логи работы генерации таблиц и создания таблицы result
```bash
make generate_log
make etl_log
```