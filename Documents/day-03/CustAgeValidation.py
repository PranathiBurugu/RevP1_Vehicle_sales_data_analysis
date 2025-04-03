customers=[]
n = int(input("enter no of customers:"))
di={}
for i in range(0,n):
    di["name"]=input("enter name:")
    di["age"]=input("enter age:")
    di["total_purchase"]=input("enter total_prchase:")
    customers.append(di)
print(di)

def is_eligible(i):
        if i["age"]>=18 and i["age"]<=25:
            is_eligible=True
            return 0.1*i["total_purchase"]
        elif i["age"]>=26 and i["age"]<=40:
            is_eligible=True
            return 0.05*i["total_purchase"]
        if i["age"]>40:
            is_eligible=False
            return 0

eligible = list(filter(lambda c : is_eligible(c),customers))
print(eligible)
after_discount = list(map(lambda c : c["total_purchase"]-is_eligible(c),eligible))

f=0
for i in eligible:
     i["total_purchase"]=after_discount[f]
     f+=1
print(eligible)