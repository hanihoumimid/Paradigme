#
# Performance of MongoDB with indexes
#
import csv
from json import dumps
import time
 
def perf_mongo(csv_file, n):

    from pymongo import MongoClient

    myclient = MongoClient()
    mydb = myclient["mydatabase"]
    mycol = mydb["mycollection"]
    # Empty the collection
    mycol.drop()
    # Create an index for the collection
    mycol.create_index([ ('M', 1) ])

    with open(csv_file, encoding = 'utf-8') as csvfile:
        my_reader = csv.DictReader(csvfile,delimiter='\t')
        my_data = [my_row for my_row in my_reader]
        #print(my_data)
        pres = dup = 0
        print('Start performance eval over',n,'inputs')
        # get the start time
        st = time.process_time()
        for my_row in my_data[0:n]:
            # print(my_row['M'],type(my_row['M']))
            #
            # find and replace <=> test if key exists
            #
            mycol.replace_one({my_row['M']: 1},{my_row['M']:1},upsert=True,hint=[ ('M', 1) ])
        # get the end time
        et = time.process_time()
        # get execution time
        res = et - st
        print('CPU Execution time:', res, 'seconds')
        print('We found',n - mycol.count_documents({}),'duplicates in the input')
        print()
        print('Wall time (also known as clock time or wall-clock time) is simply the total time')
        print('elapsed during the measurement. It’s the time you can measure with a stopwatch.')
        print('It is the difference between the time at which a program finished its execution and')
        print('the time at which the program started. It also includes waiting time for resources.')
        print()
        print('CPU Time, on the other hand, refers to the time the CPU was busy processing')
        print('the program’s instructions. The time spent waiting for other task to complete')
        print('(like I/O operations) is not included in the CPU time. It does not include')
        print('the waiting time for resources.')
#Step 1
 
perf_mongo("DEMO.csv", 14000)
