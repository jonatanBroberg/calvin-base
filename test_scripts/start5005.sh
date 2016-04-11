[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
csruntime --host 127.0.0.1 --port 5005 --controlport 5006 --keep-alive -f "localhost5005.log"
