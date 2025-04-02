li = [1,3,5,2,1,4,6]
di={}
def num_freq(lis,dict):
    for i in range(0,len(li)):
        c=0
        for j in range(0,len(li)):
            if(li[i]==li[j]):
                c+=1
        di[li[i]]=c
    print(di)

num_freq(li,di)