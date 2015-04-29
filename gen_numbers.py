import random
'''
Script generates a series of random numbers between 0 and 100 to be stored
in the file 'numbers.txt'
'''

f = open('numbers.txt','w')

for i in range(10000):
	f.write(str(random.randint(0,100)))
	f.write('\n')

f.close()
