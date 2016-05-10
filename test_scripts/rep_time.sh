DEP=`cscontrol http://10.3.60.43:5002 deploy ../calvin/examples/sample-scripts/rep_time.calvin` && size_id=`echo $DEP | perl -nle"print $& if m{(?<=size': u').*?(?=')}"`
echo $DEP
echo ""
#echo $size_id
#echo ""

NODE=`cscontrol http://10.3.60.43:5002 nodes add calvinip://10.3.60.43:5003`
echo $NODE
#NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=10.11.12.50:5000': \[u').*?(?=')}"`
#echo $NODE_ID
NODE_ID=`echo $NODE | perl -nle"print $& if m{(?<=10.3.60.43:5003': \[u').*?(?=')}"`

a="A=\`cscontrol http://10.3.60.43:5002 actor replicate $size_id $NODE_ID\`"
#&& cscontrol http://localhost:5001 actor delete $A"

n=$1
if [ -z "$n" ]; then
    n=3
fi

echo "for i in \`seq $n\`; do $a && sleep 1 && cscontrol http://10.3.60.43:5004 actor delete \$A && sleep 1; done"
#echo "$a && sleep 1 && cscontrol http://localhost:5004 actor delete \$A"
