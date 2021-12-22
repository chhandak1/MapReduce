import psutil
import random, os, time, sys

time.sleep(int(sys.argv[2]))
processName = sys.argv[1] #'mapreduce.exe'
pid_list = []
# get list of pid
for proc in psutil.process_iter():
    pinfo = proc.as_dict(attrs=['pid', 'name', 'create_time'])
    if processName.lower() in pinfo['name'].lower():
        pid_list.append(pinfo['pid'])
print("Children Process IDs found: ", pid_list[1:])
# print(pinfo['pid'], " ", pinfo['create_time'])

# if length greater than one, then at least one child process has been created
if len(pid_list) > 1:
    pid_to_kill = random.sample(pid_list[1:], 1)
    os.system("kill " + str(pid_to_kill[0]))
    print("Killed ", str(pid_to_kill[0]))