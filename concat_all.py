import pandas as pd

import os

files = os.listdir("./categories_tables")

file_paths = ["./categories_tables/" + file for file in files]

concat_tables = [pd.read_excel(file) for file in file_paths]

res_table = pd.concat(concat_tables)
res_table = res_table.loc[:, ~res_table.columns.str.contains('^Unnamed')]
res_table.drop_duplicates()
res_table.to_excel("full_table.xlsx", index=False)

