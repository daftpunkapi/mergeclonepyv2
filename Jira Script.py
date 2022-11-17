# load dependencies 
import pandas as pd
import requests
import json
from pymongo import MongoClient
# import numpy as np

# Get data from External API
username = "daftpunkapi@gmail.com"
password = "38nMKRsJFK2CGA57Dr3F7539"

subdomain = input("Enter Sub-Domain:\n")

response = requests.get(
    url = "https://"+subdomain+".atlassian.net/rest/api/3/search",
    auth = (username, password))

# Convert JSON repsonse of External API into Dict object 
res_1 = json.loads(response.text)
res_1 = res_1["issues"]


# Load Dict object into DataFrame 
df = pd.json_normalize(res_1,errors='ignore',  max_level=None)

# Add another Column "CommonStatus" i.e. Opioniated Unified Model on Jira's 'fields.status.name' column 
df["CommonStatus"] = [ "DONE" if x =="Done" else "NOT DONE" for x in df["fields.status.name"]]


# Run loop on attachment URL to produce Attachment filename 
link = pd.DataFrame(df[["id","self"]])
link["attachments"] = ""
for i in range(len(link)):
    temp = requests.get(
    url = link["self"][i],
    auth = (username, password))
    temp = json.loads(temp.text)
    n = len(temp["fields"]["attachment"])
    attach = [1]*n
    for j in range(n):
        attach[j] = temp["fields"]["attachment"][j]["filename"]
    link["attachments"][i] = attach

del (i,j,n,attach)    
    
# Define which fields we care about using dot notation for nested fields.
FIELDS_OF_INTEREST = ["id", "key", "fields.summary", "fields.assignee.displayName", "fields.issuetype.name", "fields.parent.id" , "fields.duedate", "CommonStatus", "fields.priority.name"]

# # Filter to only display the fields we care about. To actually filter them out use df = df[FIELDS_OF_INTEREST].
data = df[FIELDS_OF_INTEREST]

# concate custom ticket URL 
data["ticket_url"] = "https://"+subdomain+".atlassian.net/browse/"+data["key"]

# Appending Attachment from loop output to main DF
data = pd.merge(data,link, on ="id", how = "inner")

# dropping column containing issueAPI URL and Index
data = data.drop(["self"],axis=1)

# Run loop on description and attachment  
# desc = df[["id","fields.description.content"]]
# desc_json = pd.json_normalize(desc["fields.description.content"], errors='ignore',  max_level=None)

# desc_text = pd.DataFrame(desc[["id"]])
# desc_text["description"] = ""

# for i in range(len(desc_json)):
#     text = []
#     for j in range(len(desc_json.columns)):
#         if desc_json[i][j] == None:
#             break 
#         else: 
#             desc_json2 = pd.json_normalize(desc_json[i][j], errors='ignore',  max_level=None)
#             desc_json2 = pd.json_normalize(desc_json2["content"],errors='ignore', max_level=None)
#             desc_json2 = pd.json_normalize(desc_json2[0][0],errors='ignore', max_level=None)
#             text.append(desc_json2["text"][0])
#     desc_text["description"][i] = text

        




# # rename column names
data = data.rename(columns = {
    'id':'remote_id',
      'fields.summary':'name',
      'fields.assignee.displayName':'assignee',
      'CommonStatus':'status',
      'fields.issuetype.name':'ticket_type',
      'fields.parent.id':'parent_ticket',
      'fields.duedate': 'due_date',
      'fields.priority.name':'priority'
    })


# # change order of columns
data = data[[
  'remote_id',
  'name',
  'assignee',
  'due_date',
  'status',
  'description',
  'ticket_type',
  'parent_ticket',
  'attachments',
  'ticket_url',
  'priority']]


# Connect to MongoDB
client =  MongoClient("mongodb+srv://daft:punk@mergedev.iiiixxn.mongodb.net/?retryWrites=true&w=majority")
db = client['Ticket_Common_Model']
collection = db['Jira']

# Delete existing content in MongoDB
x = collection.delete_many({})
print(x.deleted_count," documents deleted")


data.reset_index(drop= True, inplace=True)
data_dict = data.to_dict("records")

# Insert collection
collection.insert_many(data_dict)
