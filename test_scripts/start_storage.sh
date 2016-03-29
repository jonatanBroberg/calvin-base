[ -f calvin.conf ] && mv calvin.conf calvin.conf.bak
csruntime --host localhost --port 4999 --controlport 5000 --keep-alive -s
