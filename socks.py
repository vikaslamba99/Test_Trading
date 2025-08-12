#!/bin/python3

import math
import os
import random
import re
import sys

import bisect

"""
HackerLand National Bank has a simple policy for warning clients about possible fraudulent 
account activity. If the amount spent by a client on a particular day is greater than or 
equal to  the client's median spending for a trailing number of days, they send the client 
a notification about potential fraud. The bank doesn't send the client any notifications 
until they have at least that trailing number of prior days' transaction data.
Given the number of trailing days  and a client's total daily expenditures for a period of 
days, find and print the number of times the client will receive a notification over all  days.

"""

# Complete the activityNotifications function below.
def activityNotifications(expenditure, d):
    k = len(expenditure)
    action = 0
    d_array = []
    
    for index in range(d):
        bisect.insort(d_array,expenditure[index])
    
    for index in range(d,k):
        if(d%2 == 0):
            first = d_array[int(len(d_array)/2)]
            second = d_array[int(len(d_array)/2 - 1)]
            med = (first+second)/2.0
        else:
            med = d_array[len(d_array)/2]
        print('Median is : ', med)    
        if(expenditure[index] >= int(2*med)):
            action +=1
        
        to_remove = bisect.bisect(d_array, expenditure[index - d]) - 1
        d_array.pop(to_remove)
        bisect.insort(d_array, expenditure[index])
        
    print(action)
    
#def activityNotifications(expenditure, d):
#
#    notif = 0 ; MAX = 200 ; c = [0] * (MAX+1)
#    for e in expenditure[:d] : c[e] += 1
#
#    def median2(): # returns twice the median
#        s = 0
#        for x in range(MAX+1):
#            s += c[x]
#            if 2*s >= d : break
#        if d%2 == 1 or 2*s > d : return 2*x
#        return x + next( y for y in range(x+1,MAX+1) if c[y] )
#
#    for i in range(d,len(expenditure)):
#        if expenditure[i] >= median2() : notif += 1
#        c[expenditure[i-d]] -= 1 ; c[expenditure[i]] += 1        
#    return notif

if __name__ == '__main__':
#    fptr = open(os.environ['OUTPUT_PATH'], 'w')
#
#    nd = input().split()
#
#    n = int(nd[0])

    d = 4

    expenditure = [1, 2, 3, 4, 4]

    result = activityNotifications(expenditure, d)

#    fptr.write(str(result) + '\n')
#
#    fptr.close()
    
"""
Lily's Homework problem - minimum number of swaps

import math
import os
import random
import re
import sys

# Complete the lilysHomework function below.
def lilysHomework(arr):
    k = len(arr)
    def rearrange(newarr):
        d_array = []
        swaps = 0
        for j in range(k):
            d_array = newarr[j:k]
            l_val = min(d_array)
            first = newarr.index(l_val)
            second = newarr.index(d_array[0])
            if(newarr[first] < newarr[second]):
                newarr[first], newarr[second] = newarr[second], newarr[first]
                swaps += 1
        return swaps
    return min(rearrange(arr), rearrange (list(reversed(arr))))
    
if __name__ == '__main__':
    fptr = open(os.environ['OUTPUT_PATH'], 'w')

    n = int(input())

    arr = list(map(int, input().rstrip().split()))

    result = lilysHomework(arr)

    fptr.write(str(result) + '\n')

    fptr.close()


"""

"""
# Complete the cuttree function below.
#
def cuttree(n, k, edges):
    myval = []
    o_dict = {}
    for i in range(len(edges)):
        myval.append(edges[i][0])
        myval.append(edges[i][1])
        b = edges
    max_occur_num = max(myval,key=myval.count)  
    myset = set(myval)
    newmyval = list(myset)
    for j in range(len(myval)):
        occurances = myval.count(myval[j])
        if (occurances > k) and myval[j] not in o_dict:
            o_dict[myval[j]] = occurances
    print(o_dict)
    nodes = (n)*(n - len(o_dict))
    return nodes

"""

"""
# Calculate Standard Deviation

import sys
n = int(input().strip())
X = [int(x) for x in input().strip().split()]

mean = sum(X)/n
variance = sum([((x - mean) ** 2) for x in X]) / n
stddev = variance ** 0.5

print("{0:0.1f}".format(stddev))

"""

"""
Calculate 3 quartiles - medians
def median(nums):
    if len(nums)%2 == 0:
        return int(sum(nums[len(nums)//2-1:len(nums)//2+1])/2)
    else:
        return nums[len(nums)//2]

def quartiles(N,nums):
    Q1 = median(nums[:len(nums)//2])
    Q2 = median(nums)
    if N%2 == 0:
        Q3 = median(nums[len(nums)//2:])
    else:
        Q3 = median(nums[len(nums)//2+1:])
    return Q1,Q2,Q3

N = int(input())
nums = sorted([int(num) for num in input().split()])

Q1,Q2,Q3 = quartiles(N,nums)
print(Q1)
print(Q2)
print(Q3)

"""


