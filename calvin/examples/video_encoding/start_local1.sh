mv calvin_local.conf.bak calvin_local.conf
csruntime --host localhost --port 5001 --controlport 5002 --keep-alive -l INFO
mv calvin_local.conf calvin_local.conf.bak
