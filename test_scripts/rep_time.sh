DEP=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
NODE=`cscontrol http://gru.nefario:5002 nodes add calvinip://dave.nefario:5003` && NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=dave.nefario:5003': \[u').*?(?=')}"`
echo 'A=`cscontrol http://gru.nefario:5002 actor replicate $src_id $NODE_ID && cscontrol http://dave.nefario:5004 actor delete $A`'
