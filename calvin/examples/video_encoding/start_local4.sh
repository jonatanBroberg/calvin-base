mv calvin_local.conf.bak calvin_local.conf
csruntime --host localhost --port 5007 --controlport 5008 --keep-alive -l INFO
mv calvin_local.conf calvin_local.conf.bak
