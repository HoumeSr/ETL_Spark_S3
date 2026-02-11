up:
	docker compose up -d

down:
	docker compose down --volumes --remove-orphans

generate:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
 	 --master spark://spark-master:7077 \
 	 /opt/spark/src/generate.py

generate_log:
	docker exec spark-master cat /opt/spark/work-dir/generate.log

etl:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
  	--master spark://spark-master:7077 \
  	/opt/spark/src/main.py

etl_log:
	docker exec spark-master cat /opt/spark/work-dir/etl.log

jars-download:
	bash ./jar-downloader.sh

test:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/tests/tests.py