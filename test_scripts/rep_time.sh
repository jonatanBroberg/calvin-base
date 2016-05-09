DEP=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/rep_time.calvin` && size_id=`echo $DEP | perl -nle"print $& if m{(?<=size': u').*?(?=')}"`
echo $DEP
echo ""
echo $size_id
echo "here"

NODE=`cscontrol http://gru.nefario:5002 nodes add calvinip://dave.nefario:5000`

echo $NODE
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=10.11.12.50:5000': \[u').*?(?=')}"`
echo $NODE_ID

a="A=\`cscontrol http://gru.nefario:5002 actor replicate $size_id $NODE_ID\`"

echo ""
echo ""
echo "for i in \`seq 3\`; do $a && sleep 2 && cscontrol http://dave.nefario:5001 actor delete \$A && sleep 2; done"
