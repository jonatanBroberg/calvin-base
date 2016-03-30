[ -f calvin.conf ] && mv calvin.conf calvin.conf.bak
csruntime --host `hostname -A | awk '{print $1}'` --port 4999 --controlport 5000 --keep-alive -s
