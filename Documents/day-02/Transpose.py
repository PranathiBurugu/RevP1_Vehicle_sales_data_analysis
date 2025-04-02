li = [[1,2,3],[4,5,6],[7,8,9]]
li1=[[0,0,0],[0,0,0],[0,0,0]]
def trans(lis,lis1):
    for i in range(0,len(li)):
        for j in range(0,len(li[i])):
            li1[i][j]=li[j][i]
    print(li1)

trans(li,li1)