#rand=`python -c "import random; print random.randint(30, 90)"`
rands=($(python -c "import numpy as np;
mu=18; sigma=5;
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

host=`hostname -A | awk '{print $1}'`
while true; do
    rand=${rands[i]}
    i=$((i+1))
    echo "random: $rand"
	./start.sh $port $controlport &
    PID=$!
    sleep 1

    #echo "cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port"
    cscontrol http://gru.nefario:5002 nodes add calvinip://$host:$port &
    wait $!
    sleep $rand
    kill $PID

    pkill -f ".*csruntime --host $host --port $port" &
    wait $!
    sleep 1
done
