#include<map> 
#include<string>
#include<algorithm>
#include<list>
#include<iostream>
// #include"UDFInterface.h"
#include"UDF1.h"

using namespace std;


// inverted index problem
//the map function return a list of pair<key, value> which will be later written in an intermediate file

list< pair<string, string>> UDF1::map_func(int key, string val){

    //split lines and store in list
    list<string> lines;
    int pos, pos1;
    while ((pos = val.find_first_of('\n')) != string::npos){
        lines.push_back(val.substr(0,pos));
        val.erase(0, pos+1);
    }
    lines.push_back(val);

    string line, doc_id, contents, word;
    list< pair<string, string>> intermediate;
    map<string, list<string>>::iterator it;
    string const delims{ " .,:;!?\n\t\"" };

    //iterate through lines to get doc-id and words
    for(list<string>::const_iterator i = lines.begin(); i != lines.end(); ++i){
        line = i->c_str();
        //split at tab to get doc-id and words
        pos1 = line.find_first_of('\t');
        doc_id = line.substr(0, pos1);
        contents = line.substr(pos1 + 1);
        //convert to lower-case
        transform(contents.begin(), contents.end(), contents.begin(), ::tolower);

        //split at delims
        size_t beg, pos_delim = 0;
        while ((beg = contents.find_first_not_of(delims, pos_delim)) != string::npos)
        {
            pos_delim = contents.find_first_of(delims, beg + 1);
            word = contents.substr(beg, pos_delim - beg);
            if(word.size() == 1 && !isalnum(word[0]))
                continue;
            intermediate.push_back(make_pair(word, doc_id));            
        }


    }

    return intermediate; 
}

//reduce function, sorts input list of pairs according to key
//iterates and groups output per key
list< pair<string, string>> UDF1::reduce_func(list< pair<string, string>> intermediate){


    intermediate.sort();
    list< pair<string, string>> reducer_res;
    string key("");
    string doc_id_list("");

    //iterate through sorted keys, once the key changes insert the previous key (and doc-ids) to list
    for (auto itr : intermediate){
        if (itr.first == key){
            doc_id_list += " ";
            doc_id_list += itr.second;
        }
        else{
            if(! key.empty() && ! doc_id_list.empty()){
                reducer_res.push_back(make_pair(key, doc_id_list));
            }
            key = itr.first;
            doc_id_list = itr.second;
        }
    }
    reducer_res.push_back(make_pair(key, doc_id_list));

    return reducer_res;
}
