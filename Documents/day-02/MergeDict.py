d1 = {"name":"xxx","age":"20","location":"hyd"}
d2 = {"name":"yyy","age":"22","gender":"female"}
li = []
d3={}
for i in d1:
    d3[i]=d1[i]
for i in d2:
    d3[i]=d2[i]
for i in d1.keys():
    list=[]
    for j in d2.keys():
        if i == j:
           list.append(d1[i])
           list.append(d2[j])
           d3[i]=list
    li.append(list)
# print(li)
print(d3)