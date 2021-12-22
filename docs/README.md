## Design Document
**Design Document.pdf** contains a brief explanation of our implementaion and some design trade-offs.


## Terminal Output
**terminal_output.txt** contains the expected terminal output on running ```python testfile.py 0```, i.e. testing the three UDFs with N = 3 without fault tolerance.

**terminal_output_ft.txt** contains the expected terminal output on running ```python testfile.py 1```, i.e. testing the three UDFs with N = 3 with fault tolerance.


## Expected Output & Ground Truth
**outputfile11.txt**, **outputfile12.txt**, and **outputfile13.txt** contains the results generated on performing mapreduce with UDF1 for N=3. **true_outputFile1.txt** contains the ground truth values for UDF1 on inputFile1.txt.

**outputfile21.txt**, **outputfile22.txt**, and **outputfile33.txt** contains the results generated on performing mapreduce with UDF2 for N=3. **true_outputFile2.txt** contains the ground truth values for UDF2 on inputFile2.txt.

**outputfile31.txt**, **outputfile32.txt**, and **outputfile33.txt** contains the results generated on performing mapreduce with UDF3 for N=3. **true_outputFile3.txt** contains the ground truth values for UDF3 on inputFile3.txt.
