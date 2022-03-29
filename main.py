from mongo_utils import MongoDbTask
import csv


mongo_obj = MongoDbTask('mongodb','mongodb')

#print(mongo_obj.isDatabasePresent('mongoDbTask'))

#print(mongo_obj.createDatabase('mongodb_nanotube'))

#print(mongo_obj.listDatabases())

#print(mongo_obj.createCollection('mongoDbTask','carbon_nanotube'))
#print(mongo_obj.isCollectionPresent('mongoDbTask','carbon_nanotube'))

dict1 = {
    "Chiral indice n" : "2",
    "Chiral indice m" : "1",
    "Initial atomic coordinate u" : "0,679005",
    "Initial atomic coordinate v" : "0,701318",
    "Initial atomic coordinate w" : "0,017033",
    "Calculated atomic coordinates u" : "0,721039",
    "Calculated atomic coordinates v" : "0,730232",
    "Calculated atomic coordinates w" : "0,017014"
}

#print(mongo_obj.insertOneDocument('mongoDbTask','carbon_nanotube', dict1))

list = [
{
    "Chiral indice n" : "2",
    "Chiral indice m" : "1",
    "Initial atomic coordinate u" : "0",
    "Initial atomic coordinate v" : "717298;0",
    "Initial atomic coordinate w" : "642129;0",
    "Calculated atomic coordinates u" : "231319;0",
    "Calculated atomic coordinates v" : "0,65675",
    "Calculated atomic coordinates w" : "0,232369"
},
{
    "Chiral indice n" : "2",
    "Chiral indice m" : "1",
    "Initial atomic coordinate u" : "0,679005",
    "Initial atomic coordinate v" : "0,701318",
    "Initial atomic coordinate w" : "0,017033",
    "Calculated atomic coordinates u" : "0,721039",
    "Calculated atomic coordinates v" : "0,730232",
    "Calculated atomic coordinates w" : "0,017014"
}
]
#print(mongo_obj.insertManyDocument('mongoDbTask','carbon_nanotube', list))

#print(mongo_obj.bulkUploadData('mongoDbTask','carbon_nanotube','carbon_nanotubes.csv'))

#data = mongo_obj.getRecords('mongoDbTask','carbon_nanotube',3)
#for i in data:
#    print(i)

#condition={'Chiral indice n':{'$gt':'2'}}
#data = mongo_obj.filterRecords('mongodb_nanotube','carbon_nanotube',3,condition)



present_data={'Chiral indice n': '2'}
new_data={'Chiral indice m': '5'}
#print(mongo_obj.updateOneRecord('mongoDbTask','carbon_nanotube', present_data, new_data))
#print(mongo_obj.updateAllRecords('mongoDbTask','carbon_nanotube', present_data, new_data))

#condition = {"Chiral indice n" : "2"}
#print(mongo_obj.deleteOneRecord('mongoDbTask','carbon_nanotube',condition))

#print(mongo_obj.deleteAllRecords('mongoDbTask','carbon_nanotube',condition))

#print(mongo_obj.deleteCollection('mongoDbTask','carbon_nanotube'))



