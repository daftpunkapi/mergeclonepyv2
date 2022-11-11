import pandas as pd
import requests
import json


## OPTION B
username = "daftpunkapi@gmail.com"
password = "38nMKRsJFK2CGA57Dr3F7539"

response = requests.get(
    url = "https://dpapi55.atlassian.net/rest/api/3/search",
    auth = (username, password),
    # params = {"fields": ["attachment"]}
)


# Convert JSON repsonse into Dict object for further manipulation
res_1 = json.loads(response.text)
res_1 = res_1["issues"]

df = pd.json_normalize(res_1,errors='ignore',  max_level=None)

# Load the results into a DataFrame. The list of issues is under the "issues" key of the results object.
# df = pd.json_normalize(response["issues"], errors='ignore',  max_level=None)

# Add another Column "CommonStatus" i.e. Opioniated Unified Model on Jira's 'fields.status.name' column 
df["CommonStatus"] = [ "DONE" if x =="Done" else "NOT DONE" for x in df["fields.status.name"]]

link = pd.DataFrame(df["self"])
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
        

# Define which fields we care about using dot notation for nested fields.
FIELDS_OF_INTEREST = ["id", "fields.summary", "fields.assignee.displayName", "fields.issuetype.name", "fields.parent.id" , "fields.duedate", "fields.status.name", "CommonStatus", "fields.priority.name", "Attachment"]

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