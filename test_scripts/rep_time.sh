DEP=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/actions.calvin` && src_id=`echo $DEP | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
echo $DEP
=`cscontrol http://gru.nefario:5002 actor replicate fe6d3656-cf6e-4715-b0fa-6eecd06c3817 aad65162-f48b-4efa-8e8b-a3e9ccc71867` && cscontrol http://dave.nefario:5001 actor delete

NODE=`cscontrol http://gru.nefario:5002 nodes add calvinip://dave.nefario:5000`

echo $NODE
echo $NODE_ID
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=dave.nefario:5000': \[u').*?(?=')}"`

NODE_SRC=`cscontrol http://dave.nefario:5001 nodes add calvinip://gru.nefario:5001`
echo $NODE_SRC
SRC_NODE_ID=`echo $NODE_SRC | perl -nle"print $& if m{(?<=gru.nefario:5001': \[u').*?(?=')}"`
echo $SRC_NODE_ID
echo $src_id


#snk_id=`echo $DEP | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
#echo "migrating"
#sleep 3
#cscontrol http://gru.nefario:5002 actor migrate $snk_id $NODE_ID
#echo "done first."
#sleep 2
#echo "done second"
#cscontrol http://dave.nefario:5001 actor migrate $snk_id $SRC_NODE_ID
#sleep 1
#echo "done migrating"

a="A=\`cscontrol http://gru.nefario:5002 actor replicate $src_id $NODE_ID\`"
#&& cscontrol http://dave.nefario:5001 actor delete $A"

echo "for i in \`seq 3\`; do $a && sleep 2 && cscontrol http://dave.nefario:5001 actor delete \$A && sleep 2; done"
