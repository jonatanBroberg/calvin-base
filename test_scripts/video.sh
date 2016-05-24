node=`cscontrol http://10.11.12.1:5002 nodes add calvinip://10.11.12.50:5001`
#node2=`cscontrol http://10.11.12.1:5002 nodes add calvinip://10.11.12.50:5004`
a=`cscontrol http://10.11.12.1:5002 deploy ../calvin/examples/video_encoding/video_encoding.calvin --reqs args`
echo $a
echo ""
echo $node
echo ""
echo $node_2

snk_id=`echo $a | perl -nle"print $& if m{(?<=snk': u').*?(?=')}"`
encoder_id=`echo $a | perl -nle"print $& if m{(?<=encoder': u').*?(?=')}"`

node_id=`echo $node | perl -nle"print $& if m{(?<=10.11.12.50:5001': \[u').*?(?=')}"`
node_id_2=`echo $node_2 | perl -nle"print $& if m{(?<=10.11.12.50:5004': \[u').*?(?=')}"`

echo "node"
echo $node_id
echo "encoder"
echo $encoder_id

#cscontrol http://10.11.12.1:5002 actor migrate $encoder_id $node_id
#cscontrol http://10.11.12.1:5002 actor migrate $encoder_id $node_id
