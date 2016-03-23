node=`cscontrol http://gru.nefario:5002 nodes add calvinip://dave.nefario:5001`
a=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/actions.calvin`
echo $a
echo ""
echo $node
echo ""

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`

node_id=`echo $node | perl -nle"print $& if m{(?<=dave.nefario:5001': \[u').*?(?=')}"`
echo $node_id
cscontrol http://gru.nefario:5002 actor migrate $src_id $node_id
