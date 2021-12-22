import io, re

def data_preprocessing(text):
    non_alphabetical_characters = '''!()-[]{};:'"\,<>./?@#$%^&*_~'''
    for non_alphabetical_character in non_alphabetical_characters:
        text = text.lower().replace(non_alphabetical_character,'')
    return text

hashmap = {}
inputFile = open("inputFile1.txt","r")
for line in inputFile:
    line = line.rstrip()
    
    data = line.split("\t")
    doc_id = data[0]
    words = data[1:]
    
    for word in re.split('[;,.\-\%\s]',words[0]):
        word = data_preprocessing(word)
        if word.strip() == "":
            continue
        doc_ids=[]
        if word in hashmap:
            doc_ids = hashmap.get(word)
        doc_ids = list(set(doc_ids))
        doc_ids.append(doc_id)
        hashmap.update({word:list(set(doc_ids))})
with io.open("true_outputFile1.txt",'w',encoding="utf-8") as f:
    for word,doc_id in hashmap.items():
        line_to_write = str(word)+"\t"
        doc_id.sort()
        for i in range(len(doc_id)):
            line_to_write += str(doc_id[i])+" " 
        f.write(line_to_write)
        f.write("\n")