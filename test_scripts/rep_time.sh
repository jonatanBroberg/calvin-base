DEP=`cscontrol http://localhost:5002 deploy calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
NODE=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5003` && NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=localhost:5003': \[u').*?(?=')}"`
A=`cscontrol http://localhost:5002 actor replicate $src_id $NODE_ID`
#echo 'A=`cscontrol http://localhost:5002 actor replicate $src_id $NODE_ID && cscontrol http://localhost:5004 actor delete $A`'
