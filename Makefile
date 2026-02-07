# Get jar filenames from local jars directory and prepend /opt/extra-jars path
JARS = $(shell ls jars/*.jar | sed 's|jars/|/opt/extra-jars/|g' | tr '\n' ',' | sed 's/,$$//')

down:
	docker compose down --volumes --remove-orphans

run:
	make down && docker compose up

stop:
	docker compose stop

generate:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
 	 --master spark://spark-master:7077 \
 	 /opt/spark/src/generate.py

test:
	docker exec -it spark-master /opt/spark/bin/spark-submit \
  	--master spark://spark-master:7077 \
  	/opt/spark/src/main.py

jars-download:
	bash ./jar-downloader.sh