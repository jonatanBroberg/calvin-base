nodes=`cscontrol http://localhost:5002 nodes add calvinip://localhost:5003 calvinip://localhost:5005 calvinip://localhost:5007 calvinip://localhost:5009`
a=`cscontrol http://localhost:5002 deploy video_encoding.calvin`
echo $a
reader_id=`echo $a | perl -nle"print $& if m{(?<=reader': u').*?(?=')}"`
encoder_id=`echo $a | perl -nle"print $& if m{(?<=encoder': u').*?(?=')}"`
echo $encoder_id
info=`cscontrol http://localhost:5002 actor info $encoder_id`
node_id=`echo $info | perl -nle"print $& if m{(?<=node_id': u').*?(?=')}"`
echo $info
node_5003=`echo $nodes | perl -nle"print $& if m{(?<=5003': \[u').*?(?=')}"`
node_5005=`echo $nodes | perl -nle"print $& if m{(?<=5005': \[u').*?(?=')}"`
node_5007=`echo $nodes | perl -nle"print $& if m{(?<=5007': \[u').*?(?=')}"`

echo "encoder id: $encoder_id"
echo "reader id: $reader_id"
echo "5003: $node_5003"
echo "5005: $node_5005"
echo "5007: $node_5007"

cscontrol http://localhost:5002 actor replicate $encoder_id $node_5003
cscontrol http://localhost:5002 actor replicate $encoder_id $node_5005
#cscontrol http://localhost:5002 actor replicate $encoder_id $node_5007
#cscontrol http://localhost:5002 actor replicate $reader_id $node_5003
echo ":)"
#cscontrol http://localhost:5002 actor replicate $reader_id $node_5003
#cscontrol http://localhost:5002 actor replicate $encoder_id $node_id
#cscontrol http://localhost:5002 actor replicate $encoder_id $node_id
echo ":)"
#cscontrol http://localhost:5002 actor delete $encoder_id
cscontrol http://localhost:5002 actor migrate $encoder_id $node_5007
#cscontrol http://localhost:5002 actor migrate $reader_id $node_5005
