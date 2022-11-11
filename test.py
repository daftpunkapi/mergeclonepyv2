import pandas as pd
# import requests
# import json
# from pymongo import MongoClient
from atlassian import Jira


jira_instance = Jira(
    url = "https://dpapi55.atlassian.net/",
    username = "daftpunkapi@gmail.com",
    password = "38nMKRsJFK2CGA57Dr3F7539",
)

# Get results of jql query
results = jira_instance.jql("project = BXB", limit = 10)

# Load the results into a DataFrame. The list of issues is under the "issues" key of the results object.
df = pd.json_normalize(results["issues"], errors='ignore',  max_level=None)

# Add another Column "CommonStatus" i.e. Opioniated Unified Model on Jira's 'fields.status.name' column 
df["CommonStatus"] = [ "DONE" if x =="Done" else "NOT DONE" for x in df["fields.status.name"]]

# Add another Column "Attachment" i.e. to add data from nested API GET request on issues/attachment->filename 
df["Attachment"] = "Test"

# Define which fields we care about using dot notation for nested fields.
FIELDS_OF_INTEREST = ["id", "Attachment", "fields.parent.id" , "fields.assignee.displayName", "fields.duedate", "fields.status.name", "CommonStatus", "fields.priority.name", "fields.issuetype.name", "fields.description"]

# Filter to only display the fields we care about. To actually filter them out use df = df[FIELDS_OF_INTEREST].
data = df[FIELDS_OF_INTEREST]
print(data)


# # # # Connect to MongoDB
# client =  MongoClient("mongodb+srv://daft:punk@mergedev.iiiixxn.mongodb.net/?retryWrites=true&w=majority")
# db = client['Ticket_Common_Model']
# collection = db['Jira']

# # # data.reset_index(inplace=True)
# data_dict = data.to_dict("records")

# # # # Insert collection
# collection.insert_many(data_dict)
