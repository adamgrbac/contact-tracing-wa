import requests
import pandas as pd
import sqlite3
import yagmail
import utils
import yaml
from bs4 import BeautifulSoup
import re
import urllib

# Load email config
with open("email_config.yml", "r") as f:
    email_config = yaml.safe_load(f)

# Setup Email details
yag = yagmail.SMTP(email_config["sender"], oauth2_file="oauth2_file.json")

# Open DB Connection
con = sqlite3.connect("contact_tracing_wa.db")

# Prep database tables
utils.prep_database(con)

# GET NSW Data
res = requests.get("https://www.wa.gov.au/organisation/covid-communications/covid-19-coronavirus-locations-visited-confirmed-cases")

# Parse page with bs4
page = BeautifulSoup(res.text, 'html.parser')

tables = page.find_all("table", {"class": "js-no-table"})

# Create empty list of dfs to merge later
dfs = []

# Extract data from each table
for table in tables:

    headers = [head.text for head in table.thead.tr.find_all("th")]

    # Skip non-wa locations & flights
    if headers[0] != "Exposure date":
        continue

    # Convert <tr> attributes to list of dicts
    data = []
    for row in table.tbody.find_all("tr"):
        cells = [cell.text for cell in row.find_all("td")]
        data.append(dict(zip(headers, cells)))
    
    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Append df to list, to be merged later
    dfs.append(df)

# Merge dfs into one df
if len(dfs) == 0:
    print("No Exposure Sites!")
    quit()

df = pd.concat(dfs)

# Merge dfs into one df and clean
df = utils.clean_dataframe(df)

# Load latest snapshot into tmp table
df.to_sql(name="contact_tracing_staging", con=con, schema="temp", if_exists="append", index=False)

# Break the staging table into INSERTs & UPDATEs and load into DataFrames
utils.load_staging_tables(con)
updated_records = pd.read_sql("select * from temp.contact_tracing_updates", con=con)
new_records = pd.read_sql("select * from temp.contact_tracing_inserts", con=con)

# If there are any new / updated rows, process and email to dist list
if len(new_records) > 0 or len(updated_records) > 0:

    # Email body
    contents = []

    # Create upto two sections depending on presences of new/updated records
    if len(new_records) > 0:
        contents.append("New Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(new_records))
    if len(updated_records) > 0:
        contents.append("Updated Contact Tracing Locations added to the website:")
        contents.append(utils.htmlify(updated_records))

    contents.append('<br><br><br>If you wish to unsubscribe from this service, click <a href="https://covidmailer.au.ngrok.io/unsubscribe">here</a> and fill out the form.')
    
    # Send email to dist list
    yag.send(bcc=email_config["dist_list"], subject="New WA Contact Tracing Locations!", contents=contents)

    # Update Existing Records & Insert new records into database to mark them as processed
    utils.update_historical_records(con)
    new_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
    updated_records.to_sql("contact_tracing_hist", con, if_exists="append", index=False)
else:
    # For logging purposes
    print("No updates!")

# Close DB connection
con.close()
