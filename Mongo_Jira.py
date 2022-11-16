# Load Dependencies/Libraries
from flask import Flask, request, json, Response
from pymongo import MongoClient

# Create Connect with MongoDB
class MongoAPI:
    def __init__(self, data):
        self.client = MongoClient("mongodb+srv://daft:punk@mergedev.iiiixxn.mongodb.net/?retryWrites=true&w=majority")  
        database = data["database"]
        collection = data["collection"]
        cursor = self.client[database]
        self.collection = cursor[collection]
        self.data = data

# This method will allow us to read all of the documents present in our collection. Line number 3 is used to reformat the data. The output of the collection object is of datatype dictionary
    def read(self):
        documents = self.collection.find()
        output = [{item: data[item] for item in data if item != '_id'} for data in documents] #removing the MongoDB generated ID
        return output

# Create a Flask Server
app = Flask(__name__)

# HTTP Method via Flask Routes
@app.route('/tickets', methods=['GET'])
def mongo_read():
    data = request.json
    if data is None or data == {}:
        #not really working 
        return Response(response=json.dumps({"Error": "Please provide connection information"}),
                        status=400,
                        mimetype='application/json')
    obj1 = MongoAPI(data)
    response = obj1.read()
    return Response(response=json.dumps(response),
                    status=200,
                    mimetype='application/json')

# Running the Flask Server/App
if __name__ == '__main__':
    app.run(debug=True, port=5555, host='0.0.0.0')

# if __name__ == '__main__':
#     data = {
#         "database": "Ticket_Common_Model",
#         "collection": "Github",
#     }
#     mongo_obj = MongoAPI(data)
#     print(json.dumps(mongo_obj.read(), indent=4))