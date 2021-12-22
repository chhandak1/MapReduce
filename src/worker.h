#include<string>

using namespace std;

class Worker{
    string inputfilename;
    string outputfilename;
    string udf_name;
    string N;
    
    public:
        Worker(string inputfilename_param,string outputfilename_param,string udf_name_param,string N_param);
        void reducer(int reducer_number,string intermediatefilename_to_reducer);
        // const char* mapper(int mapper_number);
        string mapper(int mapper_number);
        int compute_hash(string key, int N);
};