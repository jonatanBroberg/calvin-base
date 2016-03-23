apps=`cscontrol http://gru.nefario:5002 applications list`
echo $apps
app_id=`echo $apps | perl -nle"print $& if m{(?<=').*?(?=')}"`
echo $app_id
cscontrol http://gru.nefario:5002 applications delete $app_id

