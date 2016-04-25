DEP=`cscontrol http://localhost:5002 deploy ../calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
echo $DEP


NODE=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5003`
echo $NODE
echo $NODE_ID
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=localhost:5003': \[u').*?(?=')}"`
echo "A=\`cscontrol http://localhost:5002 actor replicate $src_id $NODE_ID\` && cscontrol http://localhost:5004 actor delete $A"
