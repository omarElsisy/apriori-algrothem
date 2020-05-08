
import sys

from itertools import chain, combinations
from itertools import islice
from collections import defaultdict
from optparse import OptionParser
import operator 
import numpy as np
import pandas as pd  
"""the Block1 in code is to adpat the database to aprior algrothem it need to run if you 
do not have the upgraded_data.csv in your directory """
"""Block 1"""
df = pd.read_csv("dataset_bigdata.csv") 
df['Income 75-122.000'] = df['Income 75-122.000'].map({0:'122>income>75', 1:'85>income>75', 2:'95>income>85', 3:'105>income>95', 4:'115>income>105',5:'122>income>115'}) 
df['Income >123.000']=df['Income >123.000'].map({0:"imcome<123",1:"income is bit bigger than 123",2:"income is bigger than 123",3:"income is much bigger than 123",4:"income is too much bigger than 123"})
df['Average income']=df['Average income'].map({0:"no income",1:"too bad income",2:"bad income",3:"not so good income",4:"good income",5:"very good income",6:"too good income",7:"high income",8:"too high income",9:"over rated income"})
df['Purchasing power class']=df['Purchasing power class'].map({0:"under Poverty line",1:"poverty",2:"above poverty line",3:"middle class",4:"above middle class",5:"high class",6:"top high class",7:"golden class",8:"dimamond class",9:"over rated class"})
df['Contribution private third party insurance']=df['Contribution private third party insurance'].map({0:"f 0",1:"f 1 - 49",2:"f 50 - 99",3:"f 100 - 199",4:"f 200 - 499",5:"f 500 - 999",6:"f 1000 - 4999",7:"f 5000 - 9999",8:"f 10.000 - 19.999",9:"f 20.000 - ?"})
df['Contribution car policies']=df['Contribution car policies'].map({0:"no Contribution car policies",5:"partial Contribution car policies",6:"full Contribution car policies",7:"Contribution car policies by Limits"})
df['Contribution fire policies']=df['Contribution fire policies'].map({0:"no Contribution fire policies",1:"The total value of the property:Rs. 50,00,000",2:"Sum Insured Under each Policy: Rs. 50,00,000",3:"Contribution by Limits fire policies",4:"Contribution by Equal Shares fire policies",5:"Proportion of Property covered by insurance: 100%",6:"Total loss payable: [100%] loss i.e. Rs. 500,000",7:"Total Insurers covering the asset: 4"})
df['AWAPART Number of private third party insurance 1 - 12']=df['AWAPART Number of private third party insurance 1 - 12'].map({0:"no private third party insurance ",1:"1 private third party insurance ",2:"2 private third party insurance ",3:"3 private third party insurance ",4:"4 private third party insurance ",5:"5 private third party insurance ",6:"6 private third party insurance ",7:"7 private third party insurance ",8:"8private third party insurance ",9:"9 private third party insurance ",10:"10 private third party insurance ",11:"11 private third party insurance ",12:"12 private third party insurance "})
df['Number of car policies']=df['Number of car policies'].map({0:"no car policies",1:"1 car policies",2:"2 car policies",3:"3 car policies",4:"4 car policies",5:"5 car policies"})
df['Number of fire policies']=df['Number of fire policies'].map({0:"no fire policies",1:"1 fire policies",2:"2 fire policies",3:"3 fire policies",4:"4 fire policies",5:"5 fire policies"})
df['Customer Subtype']=df['Customer Subtype'].map({
1:"High Income, expensive child"         
,2:"Very Important Provincials"
,3:"High status seniors"
,4:"Affluent senior apartments"
,5:"Mixed seniors"
,6:"Career and childcare"
,7:"Dinki's (double income no kids)"
,8:"Middle class families"
,9:"Modern, complete families"
,10:"Stable family"
,11:"Family starters"
,12:"Affluent young families"
,13:"Young all american family"
,14:"Junior cosmopolitan"
,15:"Senior cosmopolitans"
,16:"Students in apartments"
,17:"Fresh masters in the city"
,18:"Single youth"
,19:"Suburban youth"
,20:"Etnically diverse"
,21:"Young urban have-nots"
,22:"Mixed apartment dwellers"
,23:"Young and rising"
,24:"Young, low educated "
,25:"Young seniors in the city"
,26:"Own home elderly"
,27:"Seniors in apartments"
,28:"Residential elderly"
,29:"Porchless seniors: no front yard"
,30:"Religious elderly singles"
,31:"Low income catholics"
,32:"Mixed seniors"
,33:"Lower class large families"
,34:"Large family, employed child"
,35:"Village families"
,36:"Couples with teens 'Married with children'"
,37:"Mixed small town dwellers"
,38:"Traditional families"
,39:"Large religous families"
,40:"Large family farms"
,41:"Mixed rurals"     
})
df['Number of houses ']=df['Number of houses '].map({0:"have no houses",1:"have 1 house",2:"have 2 houses",3:"have 3 houses",4:"have 4 houses",5:"have 5 houses",6:"have 6 houses",7:"have 7 houses",8:"have 8 houses",9:"have 9 houses",10:"have 10 houses"})
df['Avg size household '] = df['Avg size household '].map({0:"no_house",1:'verysmall_House', 2:'small_House', 3:'good_House', 4:'big_House', 5:'villa',6:'villawithgarden'}) 
#df.to_csv(r'/home/omarcc/git_environment/apriori-algrothem/data/upgraded_data.csv', index = False)

print(df)
"""Block 1 finish """


def getMinSupport(groupset, workingList, minS, noOfReapeated):
        newItemSet = set()
        currentSet = defaultdict(int)  #get the element that repeated and there index
        for i in groupset:
                for w in workingList:
                        if i.issubset(w):
                                noOfReapeated[i] += 1
                                currentSet[i] += 1
        for i, count in currentSet.items():
                support = float(count)/len(workingList)
                if support >= minS:
                        newItemSet.add(i)
        return newItemSet




def getItemSet(dataBase):
    myGroupOfData = list()
    itemSet = set()
    for r in dataBase:
        data = frozenset(r)#frozenset take only the keys foe tuple (dictionary)
        myGroupOfData.append(data)
        for item in data:
            itemSet.add(frozenset([item]))              # Generate 1-itemSets
    return itemSet, myGroupOfData

def enterSets(groupSet, length):
        return set([i.union(j) for i in groupSet for j in groupSet if len(i.union(j)) == length])


def grouping(arr):
    return chain(*[combinations(arr, i + 1) for i, a in enumerate(arr)])


def apriori(dataBase, minSupport, minConfidence):
    noOfReapeated = defaultdict(int)
    allData = dict()
    assocationRules = dict()
    itemSet, groupOfData = getItemSet(dataBase)
    
    mySet = getMinSupport(itemSet, groupOfData, minSupport,noOfReapeated)

    workingSet = mySet
    k = 2
    while(workingSet != set([])):
        allData[k-1] = workingSet
        workingSet = enterSets(workingSet, k)
        currentCSet = getMinSupport(workingSet, groupOfData,minSupport, noOfReapeated)
        workingSet = currentCSet
        k = k + 1

    def getSupport(item):
            return float(noOfReapeated[item])/len(groupOfData)

    finalItems = []
    for key, value in allData.items():
        finalItems.extend([(tuple(item), getSupport(item))
                           for item in value])

    Finalrules = []
    for key, value in islice(allData.items(), 1, None):     
        for item in value:
            _subsets = map(frozenset, [x for x in grouping(item)])
            for element in _subsets:
                #print(element)
                remain = item.difference(element)
                if len(remain) > 0:
                    confidence = getSupport(item)/getSupport(element)
                    left=confidence/getSupport(remain)
                    leaverage=getSupport(item)-getSupport(element)*getSupport(remain)
                    if confidence >= minConfidence:
                        Finalrules.append(((tuple(element), tuple(remain)),
                                           confidence,left,leaverage))

            # i=0
            # for rule, confidence in sorted(toRetRules, key=operator.itemgetter(1)):
                # pre, post = rule
                # remain = item.difference(element)
                # if len(remain) > 0:
                #     confidence = getSupport(item)/getSupport(pre)
                #     left=confidence/getSupport(post)
                #     leaverage=getSupport(item)-getSupport(post)*getSupport(pre)                                      
                    # toRetRules[i][3]=left
                    # toRetRules[i][3]=leaverage
                    # print(toRetRules[i])               
    return finalItems, Finalrules


def printResults(items, rules):

    print (" RULES:")
    for rule in rules:
        (f, sec),confidence,left,leaverage = rule
        
        # print(pre)
        # print(post)
        # print(confidence)
        # print(left)
        # print(leaverage)
        print ("Rule: %s ==> %s ,confidence= %.f,left=  %.4f,leaverage= %.4f" % (str(f), str(sec), confidence,left,leaverage  ))
        #print ("  ,left=",rule[3])


def dataFromFile(fname):
        """Function which reads from the file and yields a generator"""
        file_iter = open(fname, 'rU')
        for line in file_iter:
                line = line.strip().rstrip(',')                         # Remove trailing comma
                record = frozenset(line.split(','))
                yield record


if __name__ == "__main__":

    optparser = OptionParser()
    optparser.add_option('-f', '--inputFile',
                         dest='input',
                         help='filename containing csv',
                         default=None)
    optparser.add_option('-s', '--minSupport',
                         dest='minS',
                         help='minimum support value',
                         default=0.15,
                         type='float')
    optparser.add_option('-c', '--minConfidence',
                         dest='minC',
                         help='minimum confidence value',
                         default=0.6,
                         type='float')

    (options, args) = optparser.parse_args()

    inFile = None
    if options.input is None:
            inFile = sys.stdin
    elif options.input is not None:
            inFile = dataFromFile(options.input)
    else:
            print ('No dataset filename specified, system with exit\n')
            sys.exit('System will exit')

    minSupport = options.minS
    minConfidence = options.minC

    items, rules = apriori(inFile, minSupport, minConfidence)

    printResults(items, rules)
