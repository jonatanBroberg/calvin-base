cscontrol http://localhost:5002 nodes add calvinip://localhost:5003
cscontrol http://localhost:5002 nodes add calvinip://localhost:5005
cscontrol http://localhost:5004 nodes add calvinip://localhost:5005

a=`cscontrol http://localhost:5002 deploy calvin/examples/DyingPrint.calvin`
echo $a

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
nodes=`cscontrol http://localhost:5002 nodes list`
node_id=`echo $nodes | perl -nle"print $& if m{(?<=').*(?=')}"`

echo "cscontrol http://localhost:5002 actor replicate $snk_id $node_id"
echo "cscontrol http://localhost:5002 actor replicate $src_id $node_id"

