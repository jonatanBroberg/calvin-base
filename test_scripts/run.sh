node=`cscontrol http://localhost:5004 nodes add calvinip://localhost:5001`

a=`cscontrol http://localhost:5004 deploy calvin/examples/sample-scripts/actions.calvin`
echo $a
echo ""
echo $node
echo ""
echo `cscontrol http://localhost:5006 nodes add calvinip://localhost:5001`
echo `cscontrol http://localhost:5006 nodes add calvinip://localhost:5003`


snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`

node_id=`echo $node | perl -nle"print $& if m{(?<=localhost:5001': \[u').*?(?=')}"`
echo $node_id
cscontrol http://localhost:5004 actor migrate $src_id $node_id
