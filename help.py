def dictone(temp):
    dList=[]
    tdict ={}
    for key,value in zip(temp.iterkeys(),temp.itervalues()):   
        if type(value) not in [list,dict]:
            tdict[key]=value
        if type(value) == dict:
            for key2, value2 in (value.items()):
                #value['brokers']
                #value['tagId']
                #value['imageList'
                if  type(value2) == list:
                    if len(value2)>0:
                        if type(value2[0]) ==dict:
                            map(lambda x:dList.append(x),value2)
                else:
                     tdict[key2]=value2
    dList.append(tdict) 
    return dList