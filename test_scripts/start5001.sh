[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
csruntime --host 127.0.0.1 --port 5001 --controlport 5002 --keep-alive -f "localhost5001.log"
