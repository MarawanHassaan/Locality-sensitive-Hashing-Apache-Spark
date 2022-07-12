from HashGenerator import HashGenerator
import time
import hashlib
from pyspark import SparkContext


#A function used to compute Jaccard Similarity
def SetJaccard(s1 , s2):
    i = s1.intersection(s2)
    u = s1.union(s2)

    return len(i)/len(u)

#A function to find the Jaccard between signatures
def jaccard(min1, min2):
    pairs = zip(min1, min2)
    objs = 0.0

    if len(min1) != len(min2):
        return objs

    for s1, s2 in pairs:
        if (s1 == s2):
            objs += 1

    return objs/len(min1)


#A function is used to generate the shingles set
def compute_single(element, k = 10):
    text = element[0]
    index = element[1]

    hashes = [int(hashlib.sha1(text[i:i+k].encode('utf-8')).hexdigest()[-16:],16) for i in range(0, len(text)-k+1)]

    return (index+1, set(hashes))

#A function to compute the signature
def compute_minhash(shingles, gen, size):
    ret = []
    for i in range(size):
        hashf = gen[i]
        
        minm = ""
        
        for shingle in shingles:
            minm = hashf(shingle)
            break
        for shingle in shingles:
            minm = min(minm, hashf(shingle))
        ret.append(minm)
    return ret 


#Finding the pairs with similarity >= 0.8
def filter_pairs(tuple, sim):
    s1 = tuple[0]
    s2 = tuple[1]

    return s1[0] < s2[0] and SetJaccard(s1[1], s2[1]) >= sim

#Returns pairs of docs that are similar
def pairs(tuple):
    s1 = tuple[0]
    s2 = tuple[1]

    return (s1[0],s2[0])

def flatten_bars(tuple, b, r):
    signature = tuple[1]

    return [(((i//r), int(hashlib.sha1(str(signature[i:(i+r)]).encode('utf-8')).hexdigest()[-16:], 16) ),tuple) for i in range(0, b*r, r)]

def flatten_buckets(tuple):
    docs = tuple[1]

    return [(docs[i][0], docs[j][0], jaccard(docs[i][1], docs[j][1])) for i in range(len(docs)) for j in range(i+1, len(docs))]

def compute_duplicates(tuple, shingles_dict):
    doc_i = tuple[0]
    shingles_i=tuple[1]
    res=[]

    for doc_j in range(doc_i+1, len(shingles_dict)+1):
        if (SetJaccard(shingles_dict[doc_i],shingles_dict[doc_j])>=0.8):
            res.append((doc_i,doc_j))
    return res

#Compute the shingles of a text line
def Shingles(f_name, Context, k=10):

    textFile = Context.textFile(f_name)
    return textFile.map(lambda line: line.split('\t')[1].strip())\
                    .zipWithIndex()\
                    .map(compute_single)

def ComputePairs(Shingles, sim = 0.8):
    return Shingles.cartesian(Shingles).filter(lambda x: filter_pairs(x, sim)).map(pairs)

#Returns the min hash signature of shingles set
def MinHash(Shingles, size):
    generator = HashGenerator()
    hashFoos = []
    for i in range(size):
        hashFoos.append(generator.HashFamily(i))

    return Shingles.map(lambda x: (x[0], compute_minhash(x[1], hashFoos, size)))

#To compute the locality sensitve hashing between documents
def LSH(MinHash, b, r):
    return MinHash.flatMap(lambda x: flatten_bars(x,b,r))\
                    .groupByKey()\
                    .filter(lambda x: len(x[1]) > 1)\
                    .mapValues(list)\
                    .flatMap(flatten_buckets)\
                    .filter(lambda x: x[2] >= 0.8)\
                    .map(lambda x: (x[0], x[1]))\
                    .distinct()




################################################ Main ##################################################################
docs_file = "apartments.tsv"
sc = SparkContext("local[*]", "LSH")        

b = 9
r = 12

Shingles = Shingles(docs_file, sc)
Shingles_dictionary = Shingles.collectAsMap()

############################################### LSH ####################################################################
MinHashes = MinHash(Shingles, b*r)
t0 = time.time()
LSH_pairs = LSH(MinHashes, b, r).collect()
t1 = time.time()
print("LSH - Found duplicates: {} - Elapsed Time: {}".format(len(LSH_pairs), t1-t0))


########################################## Brute Force #################################################################
t0 = time.time()
pairs = Shingles.flatMap(lambda x: compute_duplicates(x, Shingles_dictionary)).collect()
t1 = time.time()
print("Brute-force - Found duplicates: {} - Elapsed Time: {}".format(len(pairs), t1-t0))


######################################### Intersection pairs ###########################################################
print("Intersection pairs: ", len(set(LSH_pairs).intersection(pairs)))
