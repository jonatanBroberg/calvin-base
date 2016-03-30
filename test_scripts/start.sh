[ -f calvin.conf.bak ] && mv calvin.conf.bak calvin.conf
echo "Starting runtime on port $1 - $2"
csruntime --host `hostname -A | awk '{print $1}'` --port $1 --controlport $2 --keep-alive -l 'calvin.runtime.north.replicator:DEBUG' -l 'calvin.runtime.north.lost_node_handler:DEBUG' -f "`hostname -A | awk '{print $1}'`.log" 
# -l 'calvin.runtime.north.scheduler:DEBUG' # -l 'calvin.actor.actor:DEBUG'
#-l ERROR
