import datetime
import json
from collections import defaultdict

uris = {
    "10.11.12.1:5001": "gru",
    "10.11.12.50:5001": "dave1",
    "10.11.12.50:5004": "dave2",
    "10.11.12.51:5001": "kevin1",
    "10.11.12.51:5004": "kevin2",
    "10.11.12.53:5001": "jerry1",
    "10.11.12.53:5004": "jerry2",
    "10.11.12.54:5001": "mark1",
    "10.11.12.54:5004": "mark2",
    "10.11.12.55:5001": "tim1",
    "10.11.12.55:5004": "tim2",
}

with open("app_info", "r") as f:
    lines = f.readlines()

delta = datetime.timedelta(seconds=1)

first = datetime.datetime.strptime(lines[0][:23], "%Y-%m-%d %H:%M:%S,%f")
i = 0

reps = []
rels = []
rep_times = []

all_rels = []
diff = []

under_zero = 0

print first

node_reliabilities = defaultdict(list)
current_node_counts = defaultdict(list)
node_usages = defaultdict(list)

while i < len(lines):
    line = lines[i]
    time_str = line[:23]
    timestamp = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S,%f")
    time = (timestamp - first).total_seconds()

    data = line[34:]
    data = map(lambda x: x.replace("[", ""), data.split("] ["))

    node_rels = data[2].replace("[[", "").replace("]]", "")
    node_rels = map(lambda x: x.replace("(", "").replace("'", "").split(", "), node_rels.split("), ("))
    for node_rel in node_rels:
        uri = node_rel[0].replace("u", "")
        node_reliabilities[uri].append((time, float(node_rel[1])))

    replicas = int(data[0])


    current_nodes = data[1].replace("[[", "").replace("]]", "")
    current_nodes = map(lambda x: x.replace("(u'", "").replace("'", "").replace(")", "").split(", "), current_nodes.split("), ("))
    current_nodes = [x[1] for x in current_nodes]

    current_node_counts[time] = defaultdict(int)
    for uri in current_nodes:
        uri = uri.replace("u", "").replace("]", "")
        current_node_counts[time][uri] = 1 # .append((time, 1))
    #for uri in node_reliabilities:
    #    uri = uri.replace("u", "").replace("]", "")
    #    if uri not in current_nodes:
    #        current_node_counts[uri].append((time, 0))

    reliability = float(data[3])
    replication_time = int(data[5].replace("]", ""))
    required = float(data[4])
    usages = data[7].replace("[", "").replace("]", "").replace("': ", "': '").replace(", '", "', '").replace("}", "'}").replace("'", '"')
    usages = json.loads(usages)

    for uri in usages:
        uri = uri.replace("u", "")
        node_usages[uri].append((time, float(usages[uri])))
        length = len(node_usages[uri])
    for uri in node_reliabilities.keys():
        if len(node_usages[uri]) < length:
            node_usages[uri].append((time, float(0)))

    all_rels.append(reliability)
    diff = reliability - required

    if reliability - required < 0:
        under_zero += 1

    diff = "%.10f" % (diff * 1000)
    csv = map(lambda x: str(x), [time, reliability, replication_time, replicas, diff])
    print ";".join(csv).replace(".", ",")

    i += 1

    #first = first + delta

print under_zero
print float(sum(all_rels[1:])) / len(all_rels[1:])
print timestamp

node_rels = [["0"]]
for uri in node_reliabilities:
    node_rels[0].append(uris[uri.replace("u", "")])
    for i, (time, rel) in enumerate(node_reliabilities[uri]):
        if i > len(node_rels) - 2:
            node_rels.append([str(time).replace(".", ",")])
        node_rels[i + 1].append(str(rel).replace(".", ","))

lines = [";".join(node_rel) for node_rel in node_rels]
with open("node_rels.csv", "w") as f:
    f.writelines("\n".join(lines))

node_counts = [["0"]]
for uri in node_reliabilities.keys():
    node_counts[0].append(uris[uri])
for i, time in enumerate(sorted(current_node_counts.keys())):
    node_counts.append([str(time).replace(".", ",")])
    for uri in node_reliabilities.keys():
        node_counts[i + 1].append(str(current_node_counts[time][uri]))

#for uri in current_node_counts:
#    node_counts[0].append(uri.replace("u", ""))
#    for i, (time, count) in enumerate(current_node_counts[uri]):
#        if i > len(node_counts) - 2:
#            node_counts.append([str(time).replace(".", ",")])
#        node_counts[i + 1].append(str(count))

lines = [";".join(node_count) for node_count in node_counts]
with open("node_counts.csv", "w") as f:
    f.writelines("\n".join(lines))


usages = [["0"]]
for uri in node_reliabilities.keys():
    usages[0].append(uris[uri.replace("u", "")])
    for i, (time, rel) in enumerate(node_usages[uri]):
        if i > len(usages) - 2:
            usages.append([str(time).replace(".", ",")])
        #print str(time), str(rel), str(rel).replace(".", ",")
        usages[i + 1].append(str(rel).replace(".", ","))

lines = [";".join(node_rel) for node_rel in usages]
#print usages
#print "\n".join(lines)
with open("usages.csv", "w") as f:
    f.writelines("\n".join(lines))
