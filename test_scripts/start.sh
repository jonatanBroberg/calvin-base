[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
csruntime --host `hostname -A | awk '{print $1}'` --port 5001 --controlport 5002 --keep-alive -f "`hostname -A | awk '{print $1}'`.log"
