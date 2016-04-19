DEP=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
echo $DEP
=`cscontrol http://gru.nefario:5002 actor replicate fe6d3656-cf6e-4715-b0fa-6eecd06c3817 aad65162-f48b-4efa-8e8b-a3e9ccc71867` && cscontrol http://dave.nefario:5001 actor delete


NODE=`cscontrol http://gru.nefario:5002 nodes add calvinip://dave.nefario:5000`
echo $NODE
echo $NODE_ID
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=dave.nefario:5000': \[u').*?(?=')}"`

a="A=\`cscontrol http://gru.nefario:5002 actor replicate $src_id $NODE_ID\`"
#&& cscontrol http://dave.nefario:5001 actor delete $A"

echo "for i in \`seq 11\`; do $a && sleep 1 && cscontrol http://dave.nefario:5001 actor delete \$A && sleep 1; done"
