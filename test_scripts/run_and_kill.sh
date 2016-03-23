#rand=`python -c "import random; print random.randint(30, 90)"`
rands=($(python -c "import numpy as np;
mu=28; sigma=10;
s = np.random.normal(mu, sigma, 1000)
for n in s:
        print n
"))
i=0

host=`hostname -A | awk '{print $1}'`
while true; do
    rand=${rands[i]}
    i=$((i+1))
    echo "random: $rand"
	./start.sh &
    PID=$!
    sleep 1

    #echo "cscontrol http://gru.nefario:5002 nodes add calvinip://$host:5001"
    cscontrol http://gru.nefario:5002 nodes add calvinip://$host:5001 &
    wait $!
    sleep $rand
    kill $PID

    pkill -f ".*csruntime --host $host --port 5001" &
    wait $!
    sleep 1
done
