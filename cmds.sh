cscontrol http://localhost:5002 nodes add calvinip://localhost:5003
a=`cscontrol http://localhost:5002 deploy calvin/examples/sample-scripts/actions.calvin`
echo $a

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
nodes=`cscontrol http://localhost:5002 nodes list`
node_id=`echo $nodes | perl -nle"print $& if m{(?<=').*(?=')}"`

echo "\nreplicate snk:"
echo "cscontrol http://localhost:5002 actor replicate $snk_id $node_id"
echo "\nreplicate src:"
echo "cscontrol http://localhost:5002 actor replicate $src_id $node_id"
echo "\nmigrate snk"
echo "cscontrol http://localhost:5002 actor migrate $snk_id $node_id"
echo "\ndelete snk"
echo "cscontrol http://localhost:5004 actor delete $snk_id"

