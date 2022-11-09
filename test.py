from atlassian import Jira
import pandas as pd
import requests
import json

jira_instance = Jira(
    url = "https://dpapi55.atlassian.net/",
    username = "daftpunkapi@gmail.com",
    password = "38nMKRsJFK2CGA57Dr3F7539",
)

# Get results of jql query
results = jira_instance.jql("project = BXB", limit = 10)


# Load the results into a DataFrame. The list of issues is under the "issues" key of the results object.
df = pd.json_normalize(results["issues"], errors='ignore',  max_level=None)

# Define which fields we care about using dot notation for nested fields.
FIELDS_OF_INTEREST = ["id", "fields.summary", "fields.assignee.displayName", "fields.duedate", "fields.status.name", "fields.priority.name", "fields.issuetype.name", "fields.description"]

# Filter to only display the fields we care about. To actually filter them out use df = df[FIELDS_OF_INTEREST].
print(df[FIELDS_OF_INTEREST])