#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Load dependencies 
import pandas as pd
import requests
import json
from pymongo import MongoClient

# Get data from External API
username = "raghav23mehta@gmail.com"
password = "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"

subdomain = input("Enter Sub-Domain:\n")

response = requests.get(
    url = "https://api.github.com/orgs/"+subdomain+"/issues?state=all&filter=all",
    auth = (username, password))

# Convert JSON repsonse of External API into Dict object 
res_1 = json.loads(response.text)
df = pd.json_normalize(res_1,errors='ignore',  max_level=None)

# Select required fields
FIELDS_OF_INTEREST = ["id", "title", "assignee.login","state", "url", "body"]

# New dataframe with filtered fields
data = df[FIELDS_OF_INTEREST]

# Opioniated unified model for Status
data["state"] =  ["DONE" if x =="closed" else "NOT DONE" for x in df["state"]]

# Hard - coding issue for all ticket types 
data ["ticket_type"] = "Issue"


# Connect to MongoDB
client =  MongoClient("mongodb+srv://daft:punk@mergedev.iiiixxn.mongodb.net/?retryWrites=true&w=majority")
db = client['Ticket_Common_Model']
collection = db['Github']

# Delete existing content in MongoDB
x = collection.delete_many({})
print(x.deleted_count," documents deleted")

data.reset_index(inplace=True)
data_dict = data.to_dict("records")

# Insert collection
collection.insert_many(data_dict)

