employees = []
di={}
n=int(input("enter no of employees"))
for i in range(0,n):
    di["name"]=input("enter name")
    di["salary"]=input("enter sal")
    di["rating"]=input("enter rating")
    employees.append(di)

def rate(i):
        if i["rating"]==4 or i["rating"]==5:
            return 0.1*i["salary"]
        elif i["rating"]==3:
            return 0.05*i["salary"]
        elif i["rating"]==1 or i["rating"]==2:
            return -0.03*i["salary"]
        
# x = list(filter(lambda e : rate(e) , employees))
s1 = list(map(lambda e : e["salary"]+rate(e) , employees))
print(s1)

f=0
for emp in employees:
    emp["salary"]=s1[f]
    f+=1
print(employees)