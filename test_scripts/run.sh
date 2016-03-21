a=`cscontrol http://gru.nefario:5002 deploy ../calvin/examples/sample-scripts/actions.calvin`
echo $a

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
src_id=`echo $a | perl -nle"print $& if m{(?<=src': u').*?(?=')}"`
echo $a

node_names=( "dave" "kevin" "mark" "phil" "jerry" "mark" "tim" )
nodes=`cscontrol http://gru.nefario:5002 nodes list`
echo $node_names
echo $nodes
for node in "${node_names[@]}"
do
	node_id=`echo $nodes | perl -nle"print $& if m{(?<=$node': \[u').*?(?=')}"`
	echo $node_id
	echo "replicate src to $node"
	echo "cscontrol http://gru.nefario:5002 actor replicate $src_id $node_id"
	echo "replicate snk src to $node"
	echo "cscontrol http://gru.nefario:5002 actor replicate $snk_id $node_id"
done
