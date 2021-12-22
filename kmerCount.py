import io
import collections

hashmap = {}
inputFile = open("inputFile3.txt","r")

for line in inputFile:
    line = line.rstrip()
    i=0
    while i < len(line)-2:
        kmer = line[i:i+3]
        hashmap[kmer] = hashmap.get(kmer,0)+1
        i+=1

hashmap=collections.OrderedDict(sorted(hashmap.items()))

with io.open("true_outputFile3.txt",'w',encoding="utf-8") as f:
    for kmer,count in hashmap.items():
        f.write(str(kmer)+"\t"+str(count))
        f.write("\n")