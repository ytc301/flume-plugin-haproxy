
```
$ mvn clean package
```

### HAProxyLogAvroEventSerializer

Flume serializer for parsing HAProxy http logs

Parsing is done using regex so if your logformat is different it's easy to adapt.
All parts of the logline are separated into distinct Acro fields for easy querying

Example config:

```
agent.sinks.hdfssink.type=hdfs
agent.sinks.hdfssink.channel=mem-channel
agent.sinks.hdfssink.hdfs.path=/user/cloudera/log
agent.sinks.hdfssink.hdfs.fileType=DataStream
agent.sinks.hdfssink.serializer=nl.telegraaf.hadoop.flume.serialization.HAProxyLogAvroEventSerializer$Builder
```


## Hive table for avro output
```
DROP TABLE IF EXISTS haproxy_avro;

CREATE EXTERNAL TABLE haproxy_avro
PARTITIONED BY (ymd STRING, hour STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs:///real-time/haproxy'
TBLPROPERTIES ('avro.schema.url'='hdfs:///user/<USERNAME>/nl.telegraaf.flume.HAProxyEvent.avsc');

MSCK REPAIR TABLE haproxy_avro;
```

## nl.telegraaf.flume.HAProxyEvent.avsc
```
{
        "type":"record",
        "name":"HAProxyEvent",
        "namespace": "nl.telegraaf.flume",
        "fields": [
                {"name": "headers", "type": { "type": "map", "values": "string" } },
                {"name": "original", "type": "string" },
                {"name": "ip", "type": "string" },
                {"name": "time", "type": "string" },
                {"name": "frontend", "type": "string" },
                {"name": "backend", "type": "string" },
                {"name": "server", "type": "string" },
                {"name": "tq", "type": "int" },
                {"name": "tw", "type": "int" },
                {"name": "tc", "type": "int" },
                {"name": "tr", "type": "int" },
                {"name": "tt", "type": "int" },
                {"name": "statuscode", "type": "int" },
                {"name": "bytesread", "type": "string" },
                {"name": "requestcookie", "type": "string" },
                {"name": "responsecookie", "type": "string" },
                {"name": "terminationstate", "type": "string" },
                {"name": "actconn", "type": "int" },
                {"name": "feconn", "type": "int" },
                {"name": "beconn", "type": "int" },
                {"name": "srvconn", "type": "int" },
                {"name": "retries", "type": "int" },
                {"name": "srvqueue", "type": "int" },
                {"name": "backendqueue", "type": "int" },
                {"name": "requestheaders", "type": "string" },
                {"name": "responseheaders", "type": "string" },
                {"name": "method", "type": "string" },
                {"name": "uri", "type": "string" },
                {"name": "protocol", "type": "string" }
        ]
}
```
