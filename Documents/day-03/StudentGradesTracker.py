students=[]
n = int(input("enter no of students:"))
for i in range(0,n):
    ml=[]
    name=input("enter name:")
    for j in range(0,4):
        marks=int(input("enter marks:"))
        ml.append(marks)
    tu=(name,ml)
    students.append(tu)

#converting list to dictionary
di={}
for i in students:
    di[i[0]]=i[1]

#calculating average grade for a given student
def avg_grade_of_student(di):
    nam=input("enter student name to calculate avg:")
    for i in di.keys():
            if nam==i:
                a = calc_avg(nam)
    return a

#finding the student with the highest average grade
def high_avg_grade(di):
    avg_List={}
    c=0
    max=0
    for i in di.keys():
        a = calc_avg(i)
        if max<a:
            max=a
            max_avg_stud=i
        if a>50:
            c+=1
    return max_avg_stud,c

#finding the number of students who have passed
def calc_avg(name):
    s=0
    for i in di[name]:
        s+=i
    avg=s/4
    return avg
        
print("Average grade for Bob: ",avg_grade_of_student(di))
print("Student with the highest average grade and no of students who passed: ",high_avg_grade(di))



