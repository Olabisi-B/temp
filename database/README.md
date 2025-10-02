This folder hosts the AQUERY database

Initialization:
1. git clone
2. pip install requirements.txt

Running frontend/streamlit:
1. run 'streamlit run app.py' in your terminal 
2. Follow the link that appears in the terminal and test out the functionality

Running api:
1. run 'uvicorn aquery_api:app --reload' in your terminal
2. Follow the link
3. Once at the link on your web browser and /docs to view the documentation of the api

Extra information:
- The files: AQUERY_long.parquet, aquery.duckdb, AQUERY.parquet, Hold the database information and do not have to be changed
- Species_list.py was used to get all the species in the AQUERY database. To view this output click on species_list.txt
- All_merged.csv has the full AQUERY database in csv form.
- Long_csv_to_sql.py was used to transform the all_merged.csv file to sql format 
- Query_example.py provides a starting point of ways to query the database directly using SQL for advanced users
- Example_api_use.py provides example api runs for programmatic access 
- all_merged_column_mapping.json is a json file that contains all the AQUERY columns in json format