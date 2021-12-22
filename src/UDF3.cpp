#include<map> 
#include<string>
#include<string.h>
#include<algorithm>
#include<list>
#include<iostream>
// #include"UDFInterface.h"
#include "UDF3.h"

using namespace std;

// UDF 3 : Finding 3-Mers in a Genome Sequence and aggregating count

// Mapper function : Finds the existence of different 3-mers
// Adds the 3-mers to intermediate file 
list< pair<string, string>> UDF3::map_func(int key, string val){
    list<string> lines;
    
    int pos, pos1;
    while ((pos = val.find_first_of('\n')) != string::npos){
        lines.push_back(val.substr(0,pos));
        val.erase(0, pos+1);
    }
    lines.push_back(val);

    list< pair<string, string>> intermediate;
    string line;
    string str_one = "1";
    //iterate through lines to get doc-id and words
    for(list<string>::const_iterator i = lines.begin(); i != lines.end(); ++i){
        line = i->c_str();

        //find substrings substrings of size 3 for genome sequence
        for(int j=0; j<line.size()-2;j++){
            string kmer = line.substr(j,3);
            intermediate.push_back(make_pair(kmer, str_one));
        }
    }
    return intermediate;
}

// Reducer : Aggregates occurrence of 3-mers
list< pair<string, string>> UDF3::reduce_func(list< pair<string, string>> intermediate){
    intermediate.sort();
    list< pair<string, string>> reducer_res;
    string key("");
    int kmer_count = 1;

    //iterate through sorted keys, once the key changes insert the previous key to list
    for (auto itr : intermediate){
        if (itr.first == key){
            // cout<< word.compare(key);
            kmer_count += stoi(itr.second);
        }
        else{
            if(! key.empty()){
                reducer_res.push_back(make_pair(key, to_string(kmer_count)));
            }
            key = itr.first;
            kmer_count = stoi(itr.second);
        }
    }
    reducer_res.push_back(make_pair(key, to_string(kmer_count)));

    return reducer_res;
}
