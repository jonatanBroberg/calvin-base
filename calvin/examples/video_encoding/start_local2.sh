mv calvin.conf.bak calvin.conf
csruntime --host localhost --port 5003 --controlport 5004 --keep-alive -l INFO
mv calvin.conf calvin.conf.bak
