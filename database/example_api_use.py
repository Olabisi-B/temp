import requests

# Top 5 species
response = requests.get("http://localhost:8000/species?limit=5")
data = response.json()
print(data)

# Species in a sample
response = requests.get("http://localhost:8000/sample/SRR123456")
data = response.json()
print(data)
