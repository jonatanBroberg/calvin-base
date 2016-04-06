#rand=`python -c "import random; print random.randint(30, 90)"`
rands=($(python -c "import numpy as np;
mu=5; sigma=1;
s = np.random.normal(mu, sigma, 1000)
for n in s:
        print n
"))
i=0

n=$1
if [ -z "$n" ]; then
    n=1
fi
port=$((5000+$n))
controlport=$(($port+1))

host=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`
while true; do
    rand=${rands[i]}
    i=$((i+1))
    echo "random: $rand"

    #[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
    #echo "Starting runtime on port $port - $controlport"
    #csruntime --host `hostname -A | awk '{print $1}'` --port $port --controlport $controlport --keep-alive -l ERROR -l 'calvin.runtime.north.lost_node_handler:DEBUG' -l 'calvin.runtime.north.replicator:DEBUG' -l 'calvin.runtime.north.portmanager:DEBUG' &
	./start.sh $port $controlport &
    PID=$!
    echo "PID: $PID"

    sleep 1
    #echo `ps aux | grep -E "(csruntime|py.test)"`

    #echo "cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port"
    #while true;
    #do
    #    echo "adding node"
    #    cscontrol http://gru.nefario:5002 nodes list
    #done
    nodes_add=`cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port`
    echo "$nodes_add"
    sleep $rand

    #pkill -9 -f ".*csruntime --host $host --port $port"
    #echo "BEFORE: $before"
    #pstree -p

    printf "\n\n"
    date +"%Y-%m-%d %H:%M:%S.%N"
    printf "\n\n"
    echo "pkill -9 -P $PID"
    pkill -9 -P $PID
    sleep 1
    #kill -9 -P $PID

    #kill -9 $PID
    #echo `ps aux | grep -E "(csruntime|py.test)" | awk '{ print $2 }'`
    #echo `ps aux | grep -E "(csruntime|py.test)"`
    #before=`ps aux | grep -E "(csruntime.*--host $host.*--port $port --controlport $controlport)"`
    #ids=($(ps aux | grep -E "(csruntime.*--host $host.*--port $port --controlport $controlport)" | awk '{ print $2 }'))

    #echo "IDS: $ids"
    #echo "\n$nodes_add\nDATE: `date +\"%Y-%m-%d %H:%M:%S.%N\"`"
    #for ID in $ids
    #do
    #    echo "kill -9 $ID"
    #    kill -9 $ID
    #    kill -9 $ID
    #done
    #sleep 1
    #after=`ps aux | grep -E "(csruntime.*--host $host.*--port $port --controlport $controlport)"`
    #echo "AFTER: $after"
    #echo "\n\n"
done
