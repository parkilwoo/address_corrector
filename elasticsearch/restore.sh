#!/bin/bash
INDEX_NAME="addr" 

# Elasticsearch 시작
/usr/local/bin/docker-entrypoint.sh eswrapper &

# 백그라운드에서 Elasticsearch가 완전히 시작될 때까지 기다림 (예: 20초)
sleep 20

# Restore script
curl -X PUT "localhost:9200/_snapshot/addr_backup" -H 'Content-Type: application/json' -d'
{
    "type": "fs",
    "settings": {
        "location": "/usr/share/elasticsearch/addr_backup"
    }
}' &&

curl -X POST "localhost:9200/_snapshot/addr_backup/snapshot_1/_restore" -H 'Content-Type: application/json' -d'
{
    "indices": "addr"
}'

while true; do
    RECOVERY_SHARD_LIST=$(curl -s "localhost:9200/$INDEX_NAME/_recovery" | jq '.[].shards')
    RECOVERY_DONE_LENGTH=$(echo "$RECOVERY_SHARD_LIST" | jq '.[] | select(.stage == "DONE") | .stage' | wc -l)
    if [ "$RECOVERY_DONE_LENGTH" -eq $(echo "$RECOVERY_SHARD_LIST" | jq '. | length') ]; then
        echo "All shards restore completed"
        break
    fi


    for ((i=0; i<$(echo $RECOVERY_SHARD_LIST | jq '. | length'); i++)); do
        id=$(echo $RECOVERY_SHARD_LIST | jq ".[$i].id")
        percent=$(echo $RECOVERY_SHARD_LIST | jq ".[$i].index.size.percent")
        echo "ShardNumber $id ---------------------- restoration progress: $percent"        
    done

    sleep 5
done


curl -X GET "localhost:9200/_cat/indices?pretty=true"
curl -X GET "localhost:9200/addr/_settings?pretty=true"
curl -X GET "localhost:9200/addr/_mapping?pretty=true"
curl -X GET "localhost:9200/addr/_count?pretty=true"

# Elasticsearch가 종료될 때까지 대기
wait $!