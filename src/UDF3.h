// #include<map> 
#include<string>
#include<list>
// #include"UDFInterface.h"

using namespace std;

class UDF3{ //: public UDFInterface{
    public:
        // UDF3();
        list< pair<string, string>> map_func(int key, string val);
        list< pair<string, string>> reduce_func(list< pair<string, string>> intermediate);
};