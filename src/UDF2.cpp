#include<map> 
#include<string>
#include<algorithm>
#include<list>
#include<iostream>
// #include"UDFInterface.h"
#include"UDF2.h"

using namespace std;


// word-count problem
//the map function return a list of pair<key, value> which will be later written in an intermediate file

list< pair<string, string>> UDF2::map_func(int key, string val){

    transform(val.begin(), val.end(), val.begin(), ::tolower);
    //split lines at space tabs and newlines to get words
    string const delims{ " .,:;!?\n\t\"" };
    size_t beg, pos = 0;
    string str_one = "1";
    string word;
    list< pair<string, string>> intermediate;
    while ((beg = val.find_first_not_of(delims, pos)) != string::npos)
    {
        pos = val.find_first_of(delims, beg + 1);
        word = val.substr(beg, pos - beg);
        //check if single character is not alhphanumeric
        if(word.size() == 1 && !isalnum(word[0]))
        continue;
        intermediate.push_back(make_pair(word, str_one));
    }

    return intermediate; 
}

//reduce function, sorts input list of pairs according to key
//iterates and groups output per key
list< pair<string, string>> UDF2::reduce_func(list< pair<string, string>> intermediate){

    intermediate.sort();
    list< pair<string, string>> reducer_res;
    string key("");
    int word_count = 1;

    //iterate through sorted keys, once the key changes insert the previous key (and doc-ids) to list
    for (auto itr : intermediate){
        if (itr.first == key){
            word_count += stoi(itr.second);
        }
        else{
            if(! key.empty()){
                reducer_res.push_back(make_pair(key, to_string(word_count)));
            }
            key = itr.first;
            word_count = stoi(itr.second);
        }
    }
    reducer_res.push_back(make_pair(key, to_string(word_count)));

    return reducer_res;
}
