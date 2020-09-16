import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from collections import Counter
from psycopg2 import connect
from sqlalchemy import create_engine

from scrape import get_first_digit

con = connect("dbname=benford user=postgres password=53P0RMVlCi host=192.168.1.17 port=30321")
cursor = con.cursor()

data = pd.read_sql('SELECT * FROM numbers LIMIT 1000 ',
                   create_engine('postgresql+psycopg2://postgres:53P0RMVlCi@192.168.1.17:30321/benford'))

word_count_first_digits = [get_first_digit(r.word_count) for r in data.itertuples()]
absolute_total = len(word_count_first_digits)
counts = Counter(word_count_first_digits)

percentages = {c:round((d/absolute_total) * 100) for c,d in counts.items()}


plt.figure(figsize=(9, 6))
sns.distplot(word_count_first_digits)

for column in percentages:
    plt.annotate(f'{percentages[column]}%', xy=(column, counts[column]+absolute_total*0.005),
                 xytext=(column, counts[column]+absolute_total*0.005))

plt.xlim(0, 10)
plt.xticks(sorted(counts.keys()))
plt.title(f'First-digit of Numbers in {len(data)} Wikipedia Articles')
plt.tight_layout()
plt.savefig('/Users/danielkapellusch/Desktop/benford_word_counts.png', pad_inches=0.5)
plt.show()


