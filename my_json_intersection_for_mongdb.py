# -*- coding: utf-8 -*-

#
# Implement an intersection operation over two JSON representations sharing
# a common attribute, here the implicit name_surname. When we detect identical
# names (keys are identical), we intersect the remainder attributes.
# Store the intersection in MongoDB.
#

import csv
import json

def csv_to_json_first_method(csv_file):

    from json import dumps
    #create a dictionary
    data_dict = {}
    my_dict = {}
    with open(csv_file, encoding = 'latin1') as csvfile:
        my_reader = csv.DictReader(csvfile)
        #print(my_reader.fieldnames)
        my_data = [my_row for my_row in my_reader]
        for my_row in my_data:
            #print("==",my_row,"==",len(my_row),type(my_row))
            my_dict = {}
            i = 0
            for my_key,my_val in my_row.items():
                if my_key == 'Prénom et nom':
                    data_dict[my_row[my_reader.fieldnames[i]]] = my_dict
                else:
                    my_dict[my_reader.fieldnames[i]] = my_row[my_reader.fieldnames[i]]
                i = i+1

            #my_dict[my_reader.fieldnames[0]] = my_row[my_reader.fieldnames[0]]
            #my_dict[my_reader.fieldnames[1]] = my_row[my_reader.fieldnames[1]]
            #data_dict[my_row[my_reader.fieldnames[2]]] = my_dict
    #print("====================")
    my_my_dict = {}
    my_my_dict['test'] = data_dict
    #
    # convert both intermediary results to JSON object
    #
    y = dumps(my_my_dict)

    return y

class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

#
# Dictionnaries intersection
#
def dict_intersect(*dicts):
    comm_keys = dicts[0].keys()
    for d in dicts[1:]:
        # intersect keys first
        comm_keys &= d.keys()
    # then build a result dict with nested comprehension
    result = {key:[d[key] for d in dicts] for key in comm_keys}
    # if you choose the following representation, we keep less values. Why?
    #result = {key:{d[key] for d in dicts} for key in comm_keys}
    res = {}
    for key,val in result.items():
        #print(key,'==',val)
        data_str = json.dumps(val, cls=SetEncoder)
        #print(data_str)
        res[key] = data_str
    
    return res

#
# Compute the intersection of json1 and json2 according to the data representation
# of json objects given by csv_to_json_first_method()
# Note that json1 and json2 are str, meaning serialized object
# Note also that, the name&surname key is the common an implicit attribute
# for the join.
#
def intersection(mc,id1,id2):

    #print(type(mc),id1,id2)
    doc1 = mc.find({'_id':id1})
    doc2 = mc.find({'_id':id2})
    
    # Second, iterate through dictionaries
    d_res = {}
    for d1 in doc1:
        d11 = list(d1.keys())
        res1 = d1
        #print('==',d11,'==')
    for d2 in doc2:
        d22 = list(d2.keys())
        res2 = d2
        #print('==',d22,'==')
    d_res = {}
    for d_111 in d11:
       for d_222 in d22:
           if d_111 != '_id' and d_222 != '_id': 
               if d_111 == d_222:
                   #d.update(res1[d_111])
                   #d.update(res2[d_222])
                   d_res[d_111] = dict_intersect(res1[d_111],res2[d_222])
                   #print("**",d_111,d_222,"**")
    my_my_dict = {}
    my_my_dict['test'] = d_res

    z = dumps(my_my_dict)
    
    # Save the intersect in the collection
    mc.insert_one(my_my_dict)

    return z

# Main program

if __name__ == "__main__":

    from pymongo import MongoClient
    myclient = MongoClient()
    mydb = myclient["mydatabase"]
    mycol = mydb["mycollection"]
    
    json_one = csv_to_json_first_method("test.csv")
    json_two = csv_to_json_first_method("test1.csv")

    from json import loads
    from json import dumps

    # First, transform json objects to dictionaries

    d1_name = list(loads(json_one))[0]
    #print(d1_name)
    d2_name = list(loads(json_two))[0]
    #print(d2_name)

    d1 = loads(json_one)[d1_name]
    d2 = loads(json_two)[d2_name]
    #print(d1)
    #print(d2)
    
    # store them into MongoDB

    #client.test_database.drop()
    post_id_one = mycol.insert_one(d1).inserted_id
    post_id_two = mycol.insert_one(d2).inserted_id

    # compute the intersect
    d = intersection (mycol,post_id_one,post_id_two)

    # print the 3 documents in the collection
    from pprint import pprint

    cursor = mycol.find({})
    for document in cursor: 
        pprint(document)

    # On fait du ménage
    mycol.drop()
    myclient.drop_database('mydatabase')
