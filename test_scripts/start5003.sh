[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
csruntime --host 127.0.0.1 --port 5003 --controlport 5004 --keep-alive -f "localhost5003.log"
