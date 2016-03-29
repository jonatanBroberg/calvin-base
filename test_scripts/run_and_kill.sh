rands=($(python -c "import numpy as np;
mu=28; sigma=10;
s = np.random.normal(mu, sigma, 1000)
for n in s:
        print n
"))
i=0

host=localhost
while true; do
    rand=${rands[i]}
    i=$((i+1))
    echo "random: $rand"
	./test_scripts/start5001.sh &
    PID=$!
    sleep 1

    cscontrol http://localhost:5004 nodes add calvinip://$host:5001 &
    cscontrol http://localhost:5002 nodes add calvinip://$host:5005 &
    wait $!
    sleep $rand
    kill $PID

    pkill -f ".*csruntime --host $host --port 5001" &
    wait $!
    sleep 1
done
