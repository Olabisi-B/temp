import pandas as pd

csv_file = "all_merged.csv"
exclude = {"accession", "latitude", "longitude", "environmental_condition", "season",
    "depth", "temperature", "salinity", "ph", "carbon", "phosphorus",
    "carbon dioxide", "organic carbon", "inorganic carbon", "nitrate", "nitrite",
    "nitrogen", " oxygen concentration", "phosphate", "chlorophyll", "chloride",
    "methane", "date"}

# Grab only header
columns = pd.read_csv(csv_file, nrows=0).columns.tolist()

# Exclude unwanted ones
filtered_columns = [col for col in columns if col not in exclude]

# Save to file
with open("species_list.txt", "w", encoding="utf-8") as f:
    for col in filtered_columns:
        f.write(col + "\n")

print("Saved filtered columns to species_list.txt")

