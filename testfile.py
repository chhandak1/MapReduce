import os
import sys
from multiprocessing import Pool

def run_process(process):                                                             
    os.system(process)

kill_process_bool=int(sys.argv[1]) #Test by killing a mapper and a reducer if the value is 1. Don't kill any process if the value is 0. 

if kill_process_bool:
	#kill_process takes two command line arguments, processname to kill and initial sleep time
	print("\nTesting with fault tolerance\n")
	processes = ('./mapreduce.exe', 'python3 kill_process.py mapreduce.exe 5','python3 kill_process.py mapreduce.exe 17')
	
else:
	print("\nTesting without fault tolerance\n")

#compile c++ code
os.system('g++ -o mapreduce.exe src/master_fault_tolerance.cpp src/worker.cpp src/UDF1.cpp src/UDF2.cpp src/UDF3.cpp')

#test with different UDFs
for i in range(1,4):

	#write in config file	
	N = 3
	print("\n\t\tRunning UDF",str(i),"\n")
	config_file_content="app.inputfilename=inputFile"+str(i)+"\napp.outputfilename=output_dir/outputFile"+str(i)+"\napp.N="+str(N)+"\napp.class_name=UDF"+str(i)	
	with open('config.txt','w') as f:
		f.write(config_file_content)
	f.close()

	if kill_process_bool:
		#concurrent execution of mapreduce.exe and kill_process.py
		pool = Pool(processes=2)                                                        
		pool.map(run_process, processes)
	else:
		os.system('./mapreduce.exe')

	print("\nUDF",str(i)," completed\n")

	#compare outputFile with true outputs
	#read output of reducer in dict1
	dict1 = {}
	for n in range(1, N+1):
		with open('output_dir/outputFile'+str(i)+str(n)+'.txt', 'r') as f:
			contents = f.read()
		lines = contents.split('\n')
		for line in lines:
			if len(line) >1:
				key = line.split('\t')[0]
				value = line.split('\t')[1].strip()
				if key not in dict1:
					dict1[key] = value
				else:
					dict1[key] = str(int(dict1[key]) + int(value))

	#read true output in dict2
	dict2 = {}
	with open('true_outputFile'+str(i)+'.txt', 'r') as f:
		contents = f.read()
	lines = contents.split('\n')
	for line in lines:
		if len(line) >1:
			key = line.split('\t')[0]
			value = line.split('\t')[1].strip()
			dict2[key] = value

	#display difference
	if dict1 == dict2:
		print("Test case passed for UDF"+str(i))
	else:
		print("Test case did not pass for UDF"+str(i))
		print("Mismatches:")
		for key in dict1:
			if key not in dict2 or dict1[key] != dict2[key]:
				print(key, "->", dict1[key])