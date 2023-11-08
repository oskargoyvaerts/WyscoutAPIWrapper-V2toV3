# WyscoutAPIWrapper-V2toV3

# Wyscout API Wrappers for V2 and V3

This repository contains Python wrappers for the Wyscout API, catering to both Version 2 (V2) and Version 3 (V3) endpoints. The purpose of these wrappers is to simplify the process of extracting football data and saving it in various formats for further analysis or use within other applications.

## Configuration

Before you start extracting data using these wrappers, you need to configure the database connection and set your Wyscout credentials.

### Database Connection

In the `importfunctions.py` files for both V2 and V3, set up the database engine:

```python
engine = create_engine('postgresql://postgres:YourPassword@localhost:5432/wyscout')
Replace YourPassword with the actual password for your PostgreSQL database.

# Wyscout Credentials
You will also need to provide your Wyscout credentials:

client_id = "YourClientId"
client_secret = "YourClientSecret"

Replace YourClientId and YourClientSecret with your actual Wyscout credentials.

Note: Make sure to modify the database connection string depending on the SQL database type you are using.

# Usage
V2 Wrapper
The V2 Wrapper is designed to save data by default to JSON format, considering the compatibility with a variety of open-source repositories that utilize JSON.

V3 Wrapper
In contrast, the V3 Wrapper saves data directly to an SQL database, as there are no existing repositories for processing V3 data.

# Setting the Area Code

To use the wrappers:

#Set up your database.

Obtain the correct area_code for the competition you are interested in by visiting Wyscout [Competitions Support.](https://support.wyscout.com/competitions)

# Retrieving Data

After setting up, you can run the cells in the provided Jupyter notebooks to retrieve data. The wrappers allow you to:

Retrieve data as pandas DataFrames.
Save the data to JSON files (V2).
Insert the data into an SQL database (V3).


# Support
If you encounter any issues or have questions, please file an issue in the repository.
