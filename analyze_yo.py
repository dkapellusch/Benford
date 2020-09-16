import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from psycopg2 import connect
from sqlalchemy import create_engine

con = connect("dbname=benford user=postgres password=53P0RMVlCi host=192.168.1.17 port=30321")
cursor = con.cursor()

data = pd.read_sql('SELECT * FROM numbers',
                   create_engine('postgresql+psycopg2://postgres:53P0RMVlCi@192.168.1.17:30321/benford'))

columns = {'ones':1, 'twos':2, 'threes':3,
           'fours':4, 'fives':5, 'sixes':6,
           'sevens':7, 'eights':8, 'nines':9 }

totals = {c:sum(data[c]) for c in columns}
absolute_total = sum(totals.values())

reshaped_data = []
percentages = {}

for c, d in totals.items():
    reshaped_data.extend([columns[c]] *  d)
    percentages[c] = round((d/absolute_total)*100, 2)

plt.figure(figsize=(9, 6))
sns.histplot(reshaped_data)

for column in columns:
    plt.annotate(f'{percentages[column]}%', xy=(columns[column]-0.1, totals[column]+absolute_total*0.005),
                 xytext=(columns[column]-0.1, totals[column]+absolute_total*0.005))

plt.xlim(0, 10)
plt.xticks(list(columns.values()))
plt.title(f'First-digit of Numbers in {len(data)} Wikipedia Articles')
plt.tight_layout()
plt.savefig('/Users/danielkapellusch/Desktop/benford.png', pad_inches=0.5)
plt.show()

print()
