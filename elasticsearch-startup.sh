#!/bin/bash

# Elasticsearch installation script for Ubuntu
# This will be used as startup script for the VM

# Update system
apt-get update

# Install Java (required for Elasticsearch)
apt-get install -y openjdk-11-jdk

# Download and install Elasticsearch
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | apt-key add -
echo "deb https://artifacts.elastic.co/packages/8.x/apt stable main" | tee /etc/apt/sources.list.d/elastic-8.x.list
apt-get update
apt-get install -y elasticsearch

# Configure Elasticsearch for single-node cluster (minimal setup)
cat > /etc/elasticsearch/elasticsearch.yml << EOF
cluster.name: bhi-search
node.name: node-1
path.data: /var/lib/elasticsearch
path.logs: /var/log/elasticsearch
network.host: 0.0.0.0
http.port: 9200
discovery.type: single-node

# Security settings (minimal for development)
xpack.security.enabled: false
xpack.security.enrollment.enabled: false
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false

# Memory settings for small instance
bootstrap.memory_lock: false
EOF

# Set JVM heap size for small instance (512MB)
cat > /etc/elasticsearch/jvm.options.d/heap.options << EOF
-Xms512m
-Xmx512m
EOF

# Start and enable Elasticsearch
systemctl daemon-reload
systemctl enable elasticsearch
systemctl start elasticsearch

# Wait for Elasticsearch to start
sleep 30

# Create the complaints index with proper mapping
curl -X PUT "localhost:9200/complaints-000001" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "knn": true
    }
  },
  "mappings": {
    "properties": {
      "event_id": {"type": "keyword"},
      "brand_id": {"type": "keyword"},
      "source": {"type": "keyword"},
      "ts_event": {"type": "date"},
      "text": {"type": "text", "analyzer": "standard"},
      "topics": {"type": "keyword"},
      "sentiment": {"type": "float"},
      "severity": {"type": "float"},
      "dense_vec": {"type": "dense_vector", "dims": 768, "index": true, "similarity": "cosine"},
      "sparse_vec": {"type": "rank_features"},
      "url": {"type": "keyword"},
      "geo_country": {"type": "keyword"},
      "content_hash": {"type": "keyword"},
      "metadata": {"type": "object"}
    }
  }
}
'

# Create alias for easier access
curl -X POST "localhost:9200/_aliases" -H 'Content-Type: application/json' -d'
{
  "actions": [
    {
      "add": {
        "index": "complaints-000001",
        "alias": "complaints"
      }
    }
  ]
}
'

echo "Elasticsearch installation completed"
