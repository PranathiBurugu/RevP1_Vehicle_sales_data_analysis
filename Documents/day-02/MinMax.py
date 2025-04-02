li = [1,2,456,-5,3,4,5,7,3]
min=li[0]
max =li[0]
def min_max(min,max,lis):
    for i in li:
        if i<min:
            min=i
        if i>max:
            max=i
    print(min,max)

min_max(min,max,li)