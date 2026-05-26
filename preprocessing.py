import pandas as pd
data = pd.read_csv("anime_dataset_pre.csv")
#data = data[["title", "popularity", "favorites", "studios"]]
#data.to_csv("anime_dataset_pre.csv", index=False)

#print(data.isnull().sum())
#data = data.dropna()
#data.to_csv("anime_dataset_pre.csv", index=False)

#print(data.duplicated().sum())
data.drop_duplicates(inplace=True) 
data.to_csv("anime_dataset_pre.csv", index=False) 