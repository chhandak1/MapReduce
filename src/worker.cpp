#include <sys/wait.h>
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
#include <vector>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <iterator>
#include <sstream>
#include "worker.h"
#include "UDF1.h"
#include "UDF2.h"
#include "UDF3.h"

Worker::Worker(string inputfilename_param,string outputfilename_param,string udf_name_param,string N_param){
    inputfilename=inputfilename_param;
    outputfilename=outputfilename_param;
    udf_name=udf_name_param;
    N=N_param;
}

void Worker::reducer(int reducer_number,string intermediatefilename_to_reducer){

    int pos1;
    list<string> text;
    string intermediatefilepath;
    string path="./";
    string line, first,second;
    map<string, list<string>>::iterator it;
    list<pair<string,string>>::iterator logit;
    string const delims{ " .,:;!?\n\t\"" };
    list< pair<string, string> > intermediate, outputvalues;
    istringstream iss(intermediatefilename_to_reducer);

    vector<string> tokens;
    copy(istream_iterator<string>(iss),
         istream_iterator<string>(),
         back_inserter(tokens));

    vector<string>:: iterator string_it;

    for ( string_it = tokens.begin() ; string_it != tokens.end() ; string_it++ ) {
            intermediatefilepath="./"+*string_it;
            ifstream intermediatefile (intermediatefilepath);
            if (intermediatefile.is_open()){
                while(getline (intermediatefile,line)){
                    text.push_back(line);
                }
                intermediatefile.close();
            }
            else{
                cout<<"Could not open intermediate file "<<*string_it;
            }
    }

    string outputfilepath="./"+outputfilename+to_string(reducer_number)+".txt";
    ofstream outputfile (outputfilepath);

    
    for(list<string>::const_iterator i = text.begin(); i != text.end(); ++i){
        line = i->c_str();

        //split at tab to get word and doc_id
        pos1 = line.find_first_of('\t');
        first = line.substr(0, pos1);
        second = line.substr(pos1 + 1);
        intermediate.push_back(make_pair(first,second));
    }

    switch(udf_name[3]){
        case '1': 
            UDF1 udf1; //Creating an object for UDF1
            outputvalues=udf1.reduce_func(intermediate); //Calling UDF1's reduce function. 
            break;

        case '2': 
            UDF2 udf2; //Creating an object for UDF2
            outputvalues=udf2.reduce_func(intermediate); //Calling UDF2's reduce function. 
            break;

        case '3': 
            UDF3 udf3; //Creating an object for UDF3
            outputvalues=udf3.reduce_func(intermediate); //Calling UDF3's reduce function.
            break;
    }
    if (outputfile.is_open()){
        for ( logit = (outputvalues).begin() ; logit != (outputvalues).end() ; logit++ ) {
            outputfile<<(*logit).first<<'\t'; 
            outputfile<< (*logit).second << endl; 
        }
    }
    else{
        cout<<"Could not open "<<outputfilename;
        abort();
    }
}


string Worker::mapper(int mapper_number){
    string line;
    string text="";
    list< pair<string, string>> intermediateValues;
    list< pair<string, string>> outputValues;
    
    list<pair<string,string>>::iterator logit;
    const char* intermediatefilename="intermediateFile";
    string intermediatefilepath_new;
    string inputfilepath="./"+inputfilename+std::to_string(mapper_number)+".txt";

    string intermediatefilepath="./"+string(intermediatefilename)+".txt";
    ifstream inputfile (inputfilepath);

    string word;
    int sum_ascii;
    int hash_val;

    //reading inputfile spilt
    if (inputfile.is_open()){
            while(getline (inputfile,line)){
                if(text.empty())
                    text = line;
                else{
                    text += '\n';
                    text += line;
                }
            }
        inputfile.close();
    }
    else{
        cout<<"Could not open "<<inputfilename;
        abort();
    }

    switch(udf_name[3]){
        case '1': 
            UDF1 udf1; //Creating an object for UDF1
            intermediateValues=udf1.map_func(0, text); //Calling UDF1's map function. Returns (word,doc)
            break;

        case '2': 
            UDF2 udf2; //Creating an object for UDF2
            intermediateValues=udf2.map_func(0, text); //Calling UDF2's map function. Returns (word,1)
            break;

        case '3': 
            UDF3 udf3; //Creating an object for UDF3
            intermediateValues=udf3.map_func(0, text); //Calling UDF3's map function. Returns (kmer, 1)
            break;
    }

    // creating N splits for N reducers
    // append filenames to return value
    string all_names("");
    for(int i=1; i<=stoi(N); i++){
        intermediatefilepath_new="./i"+std::to_string(mapper_number)+std::to_string(i)+".txt";
        ofstream intermediatefile (intermediatefilepath_new); 
        all_names = all_names + intermediatefilepath_new + " ";
    }
    
    for ( logit = (intermediateValues).begin() ; logit != (intermediateValues).end() ; logit++ ) {
        sum_ascii=0;
        word=(*logit).first;
        word.erase(std::remove(word.begin(),word.end(),'\r'),word.end()); //map_func is returning a '\r'. Removing it manually.
        hash_val = compute_hash(word, stoi(N))+1;
        // writing to different splits based on hash value
        intermediatefilepath_new="./i"+std::to_string(mapper_number)+std::to_string(hash_val)+".txt";
        ofstream intermediatefile (intermediatefilepath_new, std::ios_base::app);
        if (intermediatefile.is_open()){
            intermediatefile<<word<<'\t';
            intermediatefile<< (*logit).second << endl;
            intermediatefile.close();
        }
        else{	
            cout<<"Could not open "<<intermediatefilepath_new<<endl;
        }
            
    }
    // returning filepaths of all splits separated by \t
    return all_names;
}

int Worker::compute_hash(string key, int N){
    int hash_value = 0;
    int prime_power = 1;
    int prime_val = 31;
    for(char character : key) {
        hash_value = (hash_value + (character * prime_power))%N;
        prime_power = (prime_power * prime_val) % N;
    }
    return hash_value;
}

