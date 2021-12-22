#include <sys/wait.h>
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <map>
#include <string>
#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include <list>
#include<vector>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <regex>
#include <unordered_map>
#include "UDF1.h"
#include "UDF2.h"
#include "UDF3.h"
#include "worker.h"

//#include"UDFInterface.h"
using namespace std;

class Master{

	string inputfilename;
	string outputfilename;
	string udf_name;
	string N;

	public:
		Master(string configFile){
			cout<<"Master called\n";
			string line,inputfilename1,outputfilename1,N1,udf_name1,token;
			string filePath="./"+configFile+".txt";
			ifstream configuration (filePath);
			if (configuration.is_open()){

			 		getline (configuration,line);
					inputfilename1=line;
				 	inputfilename=inputfilename1.substr(inputfilename1.find('=')+1,inputfilename1.length());
		    		inputfilename.erase(std::remove(inputfilename.begin(),inputfilename.end(),' '),inputfilename.end());

		    		getline (configuration,line);
					outputfilename1=line;
				 	outputfilename=outputfilename1.substr(outputfilename1.find('=')+1,outputfilename1.length());
				 	outputfilename.erase(std::remove(outputfilename.begin(),outputfilename.end(),' '),outputfilename.end());
		    		
		    		getline (configuration,line);
					N1=line;
				 	N=N1.substr(N1.find('=')+1,N1.length());
		    		
		    		getline (configuration,line);
					udf_name1=line;
				 	udf_name=udf_name1.substr(udf_name1.find('=')+1,udf_name1.length());
				 	
		    	configuration.close();
			}
			else{
				cout<<"Cannot open "<<configFile<<".txt"<<endl;
				abort();
			}

			//split file into N splits
			string inputfilepath="./"+inputfilename+".txt";
			ifstream f (inputfilepath);
			vector<string> line_list;
			line="";
			
			//read lines into list
			if (f.is_open()){
				 	while(getline (f,line)){
				 		line_list.push_back(line);
				 	}
				f.close();
			}
			else{
				cout<<"Could not open "<<inputfilename;
				abort();
			}

			cout << "Total lines: " << line_list.size() << endl;
			cout << "#Splits: "<< N << endl;
			int num = stoi(N);
			int q = line_list.size()/num; //lines in each split
			int i = 1;
			while(i <= num){

				ofstream if_split ("./"+inputfilename+to_string(i)+".txt");
				if (if_split.is_open()){ 
					int j = (i-1)*q;
					while(j < i*q && j < line_list.size()){
						if_split<<line_list[j]<<'\n'; 
						j++;
					}
					if_split.close();
				}
				else{
						cout<<"Could not write split file";
						abort();
				}
				i++;	
			}
			//write remaining lines
			ofstream if_split("./"+inputfilename+to_string(num)+".txt", std::ios_base::app);
			if (if_split.is_open()){  //append mode
				int j = num*q;
				while(j < line_list.size()){
					if_split<<line_list[j]<<'\n'; 
					j++;
				}
				if_split.close();
			}
			else{
					cout<<"Could not write split file";
					abort();
			}

		}

		string handle_dead_mapper(int i, int N1){ 

			char intermediatefilename_parent_dead_mapper[100000];
			int pipefd[2];
			pid_t cpid;
			char buf;
			const char* intermediatefilename;
			string ret_val;
			std::vector<char> intermediatefilename_vector;
			pipe(pipefd);

			cpid=fork(); //Creating a process for running a mapper
			
			//usleep(5 * microsecond);
			if (cpid==0){

				close(pipefd[0]);  // close the read-end of the pipe
				cout<<"Restarting Mapper "<<to_string(i)<<endl;
				
				Worker w(inputfilename,outputfilename,udf_name, to_string(N1));
					
				ret_val = w.mapper(i);
				intermediatefilename = ret_val.c_str();
				write(pipefd[1], intermediatefilename, strlen(intermediatefilename)); // send the contents to the reader
           		close(pipefd[1]); // close the write-end of the pipe, thus sending EOF to the reader
           		cout<<"Restarted mapper "<<to_string(i)<<" completed"<<endl;

           		_exit(EXIT_SUCCESS);
			}
			else {
				close(pipefd[1]); // close the write-end of the pipe
				
           		while (read(pipefd[0], &buf, 1) > 0){
           			strncat(intermediatefilename_parent_dead_mapper, &buf, 1);	// concatenate return values of each worker to get location of all intermediate files
           		} // read while EOF
           		close(pipefd[0]);
           		wait(NULL);
			}

			std::string s(intermediatefilename_parent_dead_mapper);

			return s;
		}

		void handle_dead_reducer(string intermediatefilename, int i) {
				pid_t cpid;
				
				// String matching parameters for intermediate file names
				string intermediatefilename_tmp=intermediatefilename;
				string intermediatefilename_to_reducer="";
				string regex_string="\\b(i[0-9]+"+to_string(i)+")([^ ]*)";
				std::smatch m;
			  	std::regex e (regex_string);

				for(int i=1;i<=stoi(N);i++){
					(std::regex_search (intermediatefilename,m,e));
					intermediatefilename_to_reducer=intermediatefilename_to_reducer+m.str()+" ";
					intermediatefilename = m.suffix().str();
				}

				cpid=fork(); //Creating a process for running a reducer
				
				if (cpid==0){ // Child process
					cout<<"Retarting Reducer "<<to_string(i)<<endl;
					Worker w(inputfilename,outputfilename,udf_name,N);
					// cout<<"to restarted reducer "<<i<<" file name : "<<intermediatefilename_to_reducer<<endl;

					w.reducer(i,intermediatefilename_to_reducer);
					cout<<"Restarted reducer "<<to_string(i)<<" completed"<<endl;
	           		_exit(EXIT_SUCCESS);
				} else {
					/*
						waitpid(pid, status, options) keeps a check on the child processes. 
						pid - process id
						status - variable that stores status
						options - 0 - no flags set for the wait check
						
						WIFEXITED(status) - Evaluates to a non-zero value if status was returned for a child process that terminated normally
					*/
					int status;
					waitpid(cpid, &status, 0);
					if (!WIFEXITED(status)){
						cout<<"Restarted worker failed"<<endl;
					}
				}
		}

		string call_mapper(){
			unsigned int microsecond = 1000000;
			int pipefd[2];
			pid_t cpid;
			char buf;
			int dead_mapper;
			int is_mapper_dead=0;
			int status;
			int status_arr[stoi(N)]={};
			unordered_map<pid_t, int> process_to_mapper;
			const char* intermediatefilename;
			string ret_val;
			std::vector<char> intermediatefilename_vector;
 			char intermediatefilename_parent[100000];
 			string intermediatefilename_from_dead_mapper;
			pipe(pipefd);

			for (int i=1;i<=stoi(N);i++){
				cpid=fork(); //Creating a process for running a mapper
				
				process_to_mapper[cpid]=i;

				if (cpid==0){

					close(pipefd[0]);  // close the read-end of the pipe
					cout<<"Starting Mapper "<<to_string(i)<<endl;
					usleep(10 * microsecond); // Delaying the mapper by 10 seconds so that the Python script can kill it to test fault tolerance
					Worker w(inputfilename,outputfilename,udf_name,N);
					ret_val = w.mapper(i);
					intermediatefilename = ret_val.c_str();
					write(pipefd[1], intermediatefilename, strlen(intermediatefilename)); // send the contents to the reader
	           		close(pipefd[1]); // close the write-end of the pipe, thus sending EOF to the reader
	           		cout<<"Mapper "<<to_string(i)<<" completed"<<endl;

	           		_exit(EXIT_SUCCESS);
				}

			}
			//Parent prcoess
			for (int i=1;i<=stoi(N);i++){
					close(pipefd[1]); // close the write-end of the pipe
					
	           		while (read(pipefd[0], &buf, 1) > 0){
	           			strncat(intermediatefilename_parent, &buf, 1);	// conactenate return values of each worker to get location of all intermediate files
	           		} // read while EOF
	           		close(pipefd[0]);

	           		/*
	           		waitpid() keeps a check on the child processes. 
	           		Returns 0 if child process is completed. 
	           		Returns a value greater than 0 if child process is unexpectedly terminated.
	           		The return value is stored in the status variable.
	           		*/
				}
			
			for (std::pair<pid_t,int> process_mapper_pair : process_to_mapper){
					/*
	           		waitpid(pid, status, options) keeps a check on the child processes. 
	           		pid - process id
					status - variable that stores status
					options - 0 - no flags set for the wait check
					
					WIFEXITED(status) - Evaluates to a non-zero value if status was returned for a child process that terminated normally
					*/
				waitpid(process_mapper_pair.first, &status, 0);
           		status_arr[process_mapper_pair.second] =  status;
				if (!WIFEXITED(status)){
           			cout<<"Mapper "<<process_mapper_pair.second<<" got killed"<<endl;
           			dead_mapper=process_mapper_pair.second;
           			is_mapper_dead=1;
           			
           		}
			}
			if (is_mapper_dead){
				int N1 = stoi(N);

				intermediatefilename_from_dead_mapper=handle_dead_mapper(dead_mapper, N1); // Restart the reducer that was terminated unsuccessfully
			}
			
			std::string s(intermediatefilename_parent);
			s=s+intermediatefilename_from_dead_mapper; // Add location of intermediate files for the mapper that died and was restarted

			// returning string with locations of splits (filepaths) separated by tabs
			return s; 
			
		}

		void call_reducer(string intermediatefilename){
			int status;	
			int status_arr[stoi(N)]={};
			unordered_map<pid_t, int> process_to_reducer;
			unsigned int microsecond = 1000000;

			int pipefd[2];
			pid_t cpid;
			char buf;
			string regex_string="";
			string intermediatefilename_tmp=intermediatefilename;
			string intermediatefilename_to_dead_reducer = intermediatefilename;
			string intermediatefilename_to_reducer="";
			std::smatch m;
		  	std::regex e (regex_string);   
			for(int i=1;i<=stoi(N);i++){
				(std::regex_search (intermediatefilename,m,e));
				intermediatefilename = m.suffix().str();
			}
			pipe(pipefd);
			for (int i=1;i<=stoi(N);i++){
				intermediatefilename=intermediatefilename_tmp;
				regex_string="\\b(i[0-9]+"+to_string(i)+")([^ ]*)";

				string intermediatefilename_to_reducer="";
				std::smatch m;
			  	std::regex e (regex_string);

				for(int i=1;i<=stoi(N);i++){
					(std::regex_search (intermediatefilename,m,e));
					intermediatefilename_to_reducer=intermediatefilename_to_reducer+m.str()+" ";
					intermediatefilename = m.suffix().str();
				}

				cpid=fork(); //Creating a process for running a reducer
				
				process_to_reducer[cpid]=i;

				if (cpid==0){ // Child process
					cout<<"Starting Reducer "<<to_string(i)<<endl;
					usleep(10 * microsecond); // Delaying the reducer by 10 seconds so that the Python script can kill it to test fault tolerance
					Worker w(inputfilename,outputfilename,udf_name,N);
					// cout<<"to reducer "<<i<<" file name : "<<intermediatefilename_to_reducer<<endl;

					w.reducer(i,intermediatefilename_to_reducer);
					cout<<"Reducer "<<to_string(i)<<" completed"<<endl;
	           		_exit(EXIT_SUCCESS);
				}
			}
			//Parent prcoess
			for (std::pair<pid_t,int> process_reducer_pair : process_to_reducer){
				/*
	           		waitpid(pid, status, options) keeps a check on the child processes. 
	           		pid - process id
					status - variable that stores status
					options - 0 - no flags set for the wait check
					
					WIFEXITED(status) - Evaluates to a non-zero value if status was returned for a child process that terminated normally
				*/
				waitpid(process_reducer_pair.first, &status, 0);
           		
				status_arr[process_reducer_pair.second] =  status;
				if (!WIFEXITED(status)){
           			cout<<"Reducer "<<process_reducer_pair.second<<" got killed"<<endl;
           			handle_dead_reducer(intermediatefilename_to_dead_reducer, process_reducer_pair.second); // Restart the reducer that was terminated unsuccessfully
           		}
			}
			// _exit(EXIT_SUCCESS);
			// cout<<"exiting call_reducer"<<endl;
			
		}

		void execute(){
			string intermediatefilename_string, inputfilename_string;

			unsigned int microsecond = 1000000;

			cout<<"-------------------------Starting Map Phase--------------------------"<<endl;
	
			intermediatefilename_string = call_mapper();

			cout<<"-------------------------Map Phase Completed-------------------------"<<endl;

			usleep(5*microsecond);

			cout<<"------------------------Starting Reduce Phase------------------------"<<endl;
			call_reducer(intermediatefilename_string);

			cout<<"-----------------------Reduce Phase Completed------------------------"<<endl;

			
			for(int i=1;i<=stoi(N);i++){
				for(int j=1;j<=stoi(N);j++){
					intermediatefilename_string="./i"+to_string(i)+to_string(j)+".txt";
					remove(intermediatefilename_string.c_str())!=0;
				}
			}
			
			cout<<"Intermediate files successfully deleted"<<endl;

			for(int j=1;j<=stoi(N);j++){
				inputfilename_string=inputfilename+to_string(j)+".txt";
				remove(inputfilename_string.c_str())!=0;
			}
			cout<<"Input file partitions successfully deleted"<<endl;
	
		}
};


int main(){
	string file="config";
	cout<<"Starting\n";
	Master m(file);
	m.execute();
	//return 0;
} 	