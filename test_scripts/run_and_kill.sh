rand=`python -c "import random; print random.randint(30, 90)"`
host=`hostname -A | awk '{print $1}'`
while true; do
    echo "random: $rand"
	./start.sh &
    PID=$!
    sleep 1

    echo "cscontrol http://gru.nefario:5002 nodes add calvinip://$host:5001"
    cscontrol http://gru.nefario:5002 nodes add calvinip://$host:5001 &
    wait $!
    sleep $rand
    kill -9 $PID

    pkill -9 -f ".*csruntime --host $host --port $5001" &
    wait $!
done
