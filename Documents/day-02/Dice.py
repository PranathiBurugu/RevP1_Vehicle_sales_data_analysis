import random

d1=[1,2,3,4,5,6]
d2=[1,2,3,4,5,6]
lis=[]
di={}
for i in d1:
    for j in d2:
        lis.append((i,j))

for i in range(2,13):
    c=0
    for j in lis:
        if i==j[0]+j[1]:
            c+=1
    di[i]=c/len(lis)

n = int(input("enter no of rounds:"))
li=[]
def dice_prob(n):
    for i in range(0,n):
        list=[]
        for j in range(0,4):
            list.append(random.randint(1,6))
        li.append(list)

    for i in range(0,n):
        p1=0
        p2=0
        s1 = li[i][0]+li[i][1]
        x1 = di[s1]
        s2=li[i][2]+li[i][3]
        x2=di[s2]
        if(x1<x2):
            p1+=1
        elif(x1>x2):
            p2+=1
    if p1>p2:
        print("player 1")
    elif p2>p1:
        print("player 2")
    else:
        print("draw")


dice_prob(n)