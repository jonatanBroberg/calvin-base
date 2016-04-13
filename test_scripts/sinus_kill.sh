#rand=`python -c "import random; print random.randint(30, 90)"`
rands=($(python -c "import numpy as np;
mu=18.9; sigma=1.0;
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

extra=0

pi=`awk "BEGIN {print atan2(0, -1)}"`
start=`date +%s`

while true; do
    rand=${rands[i]}
	echo $rands
    extra=0
    i=$((i+1))

    now=`date +%s`
    time=`awk "BEGIN {print ($now - $start)}"`

    rand=`awk "BEGIN {print $rand + 10 * sin(2 * $pi * $time / 120)}"`
    rand=`awk "BEGIN {print ($rand - $extra)}"`

    echo "sleep: $rand"

	./start.sh $port $controlport &
    PID=$!
    #echo "PID: $PID"

    sleep 0.5
    #echo `ps aux | grep -E "(csruntime|py.test)"`

    #echo "cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port"
    nodes_add="null"
    while [[ $nodes_add == *"null"* ]];
    do
        #echo "adding node"
        #cscontrol http://gru.nefario:5002 nodes list
        nodes_add=`cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port`
        echo $nodes_add
    done

    sleep $rand

    printf "\n\n"
    date +"%Y-%m-%d %H:%M:%S.%N"
    printf "\n\n"

    nodes=`cscontrol http://gru.nefario:5002 nodes list`
    n_nodes=$(grep -o " " <<< "$nodes" | wc -l)
    while [ $n_nodes -lt 5 ]
    do
        echo "not enough nodes, sleeping..."
        echo $n_nodes
        nodes=`cscontrol http://gru.nefario:5002 nodes list`
        n_nodes=$(grep -o " " <<< "$nodes" | wc -l)
        sleep 0.2
        extra=`awk "BEGIN {print ($extra + 0.2)}"`
    done
    pkill -9 -P $PID
    sleep 0.2
done
