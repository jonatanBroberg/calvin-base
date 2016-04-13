apps=`cscontrol http://gru.nefario:5002 applications list`
app_id=`echo $apps | perl -nle"print $& if m{(?<=u').*?(?=')}"`
cscontrol http://gru.nefario:5002 applications delete $app_id
