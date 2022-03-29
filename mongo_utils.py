import csv
import pymongo
import logging as log

log.basicConfig(filename="mongo_log.log", level=log.INFO, format="%(asctime)s %(levelname)s %(message)s")


class MongoDbTask:
    def __init__(self, username, password):
        '''Method responsible to establish connection and sets client url'''
        try:
            self.username = username
            self.password = password
            self.link = f"mongodb+srv://{username}:{password}@cluster0.jnq0q.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
            log.info("Initialized database connection")
        except Exception as e:
            print("Connection Problem", str(e))
            log.exception("Connection Problem", str(e))

    def getMongoDbClient(self):
        '''Method return mongo client'''
        try:
            client = pymongo.MongoClient(self.link)
            log.info("Client created")
            return client
        except Exception as e:
            log.exception("Connection Problem", str(e))
            raise Exception(f"something went wrong to create MongoClient: {str(e)}")

    def listDatabases(self):
        '''List downs all databases present in MonogDB Atlas'''
        try:
            client = self.getMongoDbClient()
            list = client.list_database_names()
            log.info("Listing all the databases =",list)
            return list
        except Exception as e:
            log.exception("Something went wrong", str(e))

    def isDatabasePresent(self, database_name):
        '''Methods checks if Database already present or not'''
        try:
            client = self.getMongoDbClient()
            if database_name in client.list_database_names():
                return True
            else:
                return False
        except Exception as e:
            raise Exception(f"{database_name} already exists! Please chose different one", str(e))

    def getDatabaseName(self, database_name):
        '''Method returns database name'''
        try:
            client = self.getMongoDbClient()
            database = client[database_name]
            return database
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception(f"Problem with databse creation", str(e))

    def isCollectionPresent(self, database_name, collection_name):
        '''Methods checks if Collection already present or not'''
        try:
            if self.isDatabasePresent(database_name):
                database = self.getDatabaseName(database_name)
                if collection_name in database.list_collection_names():
                    return True
                else:
                    return False
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception(f"{collection_name} already exists! Please chose different one", str(e))

    def getCollectionName(self, database_name, collection_name):
        '''Method returns collection name'''
        try:
            database = self.getDatabaseName(database_name)
            collection = database[collection_name]
            return collection
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Something went wrong in fetching collection",str(e))

    def createDatabase(self, database_name):
        '''Method creates database'''
        try:
            if self.isDatabasePresent(database_name):
                return f"{database_name} already present"
            else:
                client = self.getMongoDbClient()
                database = client[database_name]
                return f"{database_name} created successfully!"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Something went wrong in database creation", str(e))

    def createCollection(self, database_name, collection_name):
        '''Method creates collection'''
        try:
            if self.isCollectionPresent(database_name, collection_name):
                return f"{collection_name} already exists"
            else:
                database = self.getDatabaseName(database_name)
                collection = database[collection_name]
                log.info("Collection created =", collection)
                return f"{collection_name} created successfully!"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Something went wrong in collection creation", str(e))


    def insertOneDocument(self, database_name, collection_name, dictionary):
        '''Insert one document at a time'''
        try:
            collection = self.getCollectionName(database_name, collection_name)
            collection.insert_one(dictionary)
            log.info("Inserted one document")
            return f"Data inserted successfully in {collection_name}"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Something went wrong in inserting one document", str(e))


    def insertManyDocument(self, database_name, collection_name, list):
        '''Insert Many document at a time'''
        try:
            collection = self.getCollectionName(database_name, collection_name)
            collection.insert_many(list)
            log.info("Inserted Many document")
            return f"Data inserted successfully in {collection_name}"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Something went wrong in inserting Many document", str(e))

    def bulkUploadData(self, database, collection_name, filename):
        '''Upload bulk dataset on mongodb Atlas'''
        try:
            with open(filename,"r") as f:
                col = f.readline().rstrip().split(";")
                file = csv.reader(f, delimiter = ";")
                records = []
                for data in file:
                    row = { col[i] : data[i] for i in range(len(col))}
                    records.append(row)
                collection = self.getCollectionName(database, collection_name)
                collection.insert_many(records)
                log.info("Bulk upload of data successfully")
                return f"Uploaded successfully in collection={collection_name} & database={database}"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Error occured while uploading dataset")

    def getRecords(self, database, collection_name, limit):
        '''Method will fetch recods as per limit'''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self. getCollectionName(database, collection_name)
                data = collection.find().limit(limit)
                return data
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occured while fetching data")

    def filterRecords(self, database, collection_name, limit, condition):
        '''MEthod will fetch records based on certain condition'''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                data = collection.find(condition).limit(limit)
                for i in data:
                    print(i)
                return data
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occured while fetching data")

    def updateOneRecord(self, database, collection_name, current,update):
        '''Method will update old record via new '''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                new_data = {"$set" : update}
                collection.update_one(current, new_data)
                log.info("One record updated")
                return "Data updated successfully"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occured while fetching data")

    def updateAllRecords(self, database, collection_name, current, update):
        '''Method will update old record via new '''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                new_data = {"$set": update}
                collection.update_many(current, new_data)
                log.info("Many records updated")
                return "Data updated successfully"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occurred while fetching data")


    def deleteOneRecord(self, database, collection_name, condition):
        '''Method deletes one record based on condition'''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                collection.delete_one(condition)
                log.info("One record deleted")
                return f"One record based on condition {condition} deleted"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occures while deleting one record")

    def deleteAllRecords(self, database, collection_name, condition):
        '''Method deletes all records based on condition'''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                collection.delete_many(condition)
                log.info("Many records deleted")
                return f"All records based on condition {condition} deleted"
        except Exception as e:
            log.exception("Something went wrong", str(e))
            raise Exception("Problem occures while deleting all records")

    def deleteCollection(self, database, collection_name):
        '''Method drop entire collection'''
        try:
            if self.isCollectionPresent(database, collection_name):
                collection = self.getCollectionName(database, collection_name)
                collection.drop()
                log.info("Collection:{collection_name} of Database:{database} dropped")
                return f"Collection:{collection_name} of Database:{database} dropped successfully"
        except Exception as e:
                log.exception("Something went wrong", str(e))
                raise Exception("Something went wrong while deleting collection")