[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
#hostname=`hostname -A | awk '{print $1}'`
#IP=`getent hosts $hostname | awk '{ print $1 }'`
#IP=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`
host=`ip route get 8.8.8.8 | awk '{print $NF; exit}'`

n=$1
if [ -z "$n" ]; then
    n=1
fi
port=$((5000+$n))
controlport=$(($port+1))
echo "Starting runtime on port $host:$port - $controlport"

csruntime --host $host --port $port --controlport $controlport --keep-alive -l 'calvin.runtime.north.lost_node_handler:DEBUG' -l 'calvin.runtime.north.replicator:DEBUG' & # -l 'calvin.runtime.north.portmanager:DEBUG' -l 'calvin.runtime.north.calvin_network:DEBUG'

sleep 2

nodes_add="null"
while [[ $nodes_add == *"null"* ]];
do
    echo "adding node"
    #cscontrol http://gru.nefario:5002 nodes list
    nodes_add=`cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port`
    echo $nodes_add
done
nodes_add="null"
while [[ $nodes_add == *"timeout"* ]];
do
    echo "adding node"
    #cscontrol http://gru.nefario:5002 nodes list
    nodes_add=`cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port`
    echo $nodes_add
done
