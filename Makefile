up:
	docker compose up -d

down:
	docker compose down --volumes --remove-orphans

generate:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
 	 --master spark://spark-master:7077 \
 	 /opt/spark/src/generate.py

get_result:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
  	--master spark://spark-master:7077 \
  	/opt/spark/src/main.py

jars-download:
	bash ./jar-downloader.sh