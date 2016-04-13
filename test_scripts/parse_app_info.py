import datetime

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
while i < len(lines):
    line = lines[i]
    #print line
    time_str = line[:23]
    timestamp = datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S,%f")
    if timestamp < first + delta:
        data = line[34:]
        data = map(lambda x: x.replace("[", ""), data.split("] ["))
        replicas = int(data[0])
        reliability = float(data[3])
        replication_time = int(data[5].replace("]", ""))
        required = float(data[4])

        reps.append(replicas)
        rels.append(reliability)
        rep_times.append(replication_time)

        #print "adding: ", reliability
        all_rels.append(reliability)
        diff.append(reliability - required)
        #if reliability < required:
        #    print "LESS: ", required, reliability
        #    print reliability - required
        i += 1
    else:
        if len(reps) > 0:
            avg_reps = float(sum(reps)) / len(reps)
            avg_rep_time = float(sum(rep_times)) / len(rep_times)
            avg_rel = float(sum(rels)) / len(rels)
            avg_diff = float(sum(diff)) / len(diff)
            #if avg_diff < 0:
            #    print "\n" * 5
            #    print float(sum(diff)), len(diff)
            #    print "\n" * 5

            reps = []
            rels = []
            rep_times = []
            diff = []

            if avg_diff < 0:
                under_zero += 1

            avg_diff = "%.10f" % (avg_diff * 1000)
            csv = map(lambda x: str(x), [avg_rel, avg_reps, avg_rep_time, avg_diff])
            print ";".join(csv).replace(".", ",")

        first = first + delta

print under_zero
print float(sum(all_rels[1:])) / len(all_rels[1:])
print timestamp
