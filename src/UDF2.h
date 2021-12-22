// #include<map> 
#include<string>
#include<list>
// #include"UDFInterface.h"

using namespace std;

class UDF2{ //: public UDFInterface{
    public:
        // UDF2();
        list< pair<string, string>> map_func(int key, string val);
        list< pair<string, string>> reduce_func(list< pair<string, string>> intermediate);
};