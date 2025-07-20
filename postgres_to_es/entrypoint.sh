#!/bin/bash

# Проверка соединений с серверами БД и ES.
echo Check database is up...
while ! nc -z $POSTGRES_HOST $PGPORT; do
      sleep 0.1
done 

echo Check ES is up...
while ! nc -z $ELASTIC_HOST $ELASTIC_PORT; do
      sleep 0.1
done 

echo Create ES index...

# Функция для создания индекса в Elasticsearch.
create_es_index() {
    local index_name="$1"
    local index_file="$2"
    
    echo "Processing index: $index_name"

    if [ ! -f "$index_file" ]; then
        echo "Error: Index definition file $index_file not found!"
        return 1
    fi

    local max_retries=5
    local retry_delay=3
    local attempt=1
    local http_status

    ES_URL="http://$ELASTIC_HOST:$ELASTIC_PORT"

    while [ $attempt -le $max_retries ]; do
        echo "Creating index '$index_name' (attempt $attempt/$max_retries)..."
        
        http_status=$(curl -s -o /dev/null -w "%{http_code}" \
            -XPUT "$ES_URL/$index_name" \
            -H 'Content-Type: application/json' \
            --data-binary "@$index_file")
        
        case $http_status in
            200)
                echo "Index '$index_name' created successfully!"
                return 0
                ;;
            400)
                echo "Index '$index_name' already exists"
                return 0
                ;;
            *)
                echo "Failed to create index '$index_name' (HTTP $http_status). Retrying in $retry_delay seconds..."
                sleep "$retry_delay"
                attempt=$((attempt + 1))
                ;;
        esac
    done

    echo "Error: Failed to create index '$index_name' after $max_retries attempts!"
    return 1
}

# Создаем индексы через функцию
echo "Creating ES indexes..."
create_es_index "movies" "./etl_data/movies_index.json" || exit 1
create_es_index "genres" "./etl_data/genres_index.json" || exit 1
create_es_index "persons" "./etl_data/persons_index.json" || exit 1

exec "$@"