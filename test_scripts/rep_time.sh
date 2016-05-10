DEP=`cscontrol http://10.3.60.43:5002 deploy ../calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
echo $DEP


NODE=`cscontrol http://10.3.60.43:5002 nodes add calvinip://10.3.60.43:5003`
echo $NODE
echo $NODE_ID
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=10.3.60.43:5003': \[u').*?(?=')}"`

a="A=\`cscontrol http://10.3.60.43:5002 actor replicate $src_id $NODE_ID\`"
#&& cscontrol http://localhost:5001 actor delete $A"


echo "for i in \`seq 20\`; do $a && sleep 1 && cscontrol http://10.3.60.43:5004 actor delete \$A && sleep 1; done"
#echo "$a && sleep 1 && cscontrol http://localhost:5004 actor delete \$A"
