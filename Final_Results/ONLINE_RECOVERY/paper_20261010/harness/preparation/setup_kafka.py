#!/usr/bin/env python3
"""Start only new campaign Kafka instances; existing instances are untouched."""
from pathlib import Path
import subprocess
import shlex
import time

B='/home/neel/ariabc_data/paper_20261010_recov'
hosts=['10.129.148.247','10.129.148.246','10.129.148.248']
java='if ! command -v java >/dev/null; then export JAVA_HOME=/home/neel/Desktop/usr/lib/jvm/java-21-openjdk-amd64; export PATH=$JAVA_HOME/bin:$PATH; fi; '
khome='/home/neel/Desktop/kafka_2.13-3.7.0'
def ssh(host,cmd,**kw):
    return subprocess.run(['ssh','-o','BatchMode=yes','neel@'+host,cmd],check=True,**kw)
identity=ssh(hosts[0],java+khome+'/bin/kafka-storage.sh random-uuid',capture_output=True,text=True).stdout.strip().splitlines()[-1]
Path(B+'/provenance/kafka_cluster_id.txt').write_text(identity+'\n')
for idx,host in enumerate(hosts):
    ssh(host,"set -e; test -z \"$(ss -ltnH | awk '$4 ~ /:(9192|9193)$/ {print}')\"; mkdir -p "+B+'/kafka '+B+'/provenance '+B+'/scratch')
    config=f'''process.roles={'broker,controller' if idx==0 else 'broker'}
node.id={idx+1}
controller.quorum.voters=1@10.129.148.247:9193
listeners=PLAINTEXT://0.0.0.0:9192{',CONTROLLER://0.0.0.0:9193' if idx==0 else ''}
advertised.listeners=PLAINTEXT://{host}:9192
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
num.network.threads=8
num.io.threads=8
socket.send.buffer.bytes=4194304
socket.receive.buffer.bytes=4194304
socket.request.max.bytes=104857600
log.dirs={B}/kafka/data
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
log.retention.hours=168
log.segment.bytes=1073741824
log.retention.check.interval.ms=300000
'''
    ssh(host,'cat > '+B+'/kafka/config.properties',input=config,text=True)
    ssh(host,'set -e; '+java+f'export TMPDIR={B}/scratch; export LOG_DIR={B}/kafka/service_logs; mkdir -p "$LOG_DIR"; '
        +khome+'/bin/kafka-storage.sh format -t '+shlex.quote(identity)+' -c '+B+'/kafka/config.properties; '
        +'export KAFKA_HEAP_OPTS="-Xms512m -Xmx512m"; '
        +'nohup '+khome+'/bin/kafka-server-start.sh '+B+'/kafka/config.properties < /dev/null > '+B+'/kafka/console.log 2>&1 & echo $! > '+B+'/kafka/broker.pid')
    ssh(host,'sha256sum '+khome+'/libs/kafka_2.13-3.7.0.jar > '+B+'/provenance/kafka_library.sha256')
for attempt in range(60):
    try:
        for host in hosts:
            ssh(host,java+'timeout 15 '+khome+'/bin/kafka-topics.sh --bootstrap-server localhost:9192 --list',capture_output=True,text=True)
        print('isolated Kafka ready',identity,flush=True)
        break
    except subprocess.CalledProcessError:
        time.sleep(2)
else:
    raise RuntimeError('isolated Kafka not ready')
