// #include<map>
#include<list>
#include<string>

using namespace std;
//use pure vitual functions => class is abstract
class UDFInterface{
    public:
        virtual list< pair<string, string>> map_func(int key, string val) = 0;
        virtual list< pair<string, string>> reduce_func(list< pair<string, string>> intermediate) = 0;
};