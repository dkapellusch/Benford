from typing import List
from psycopg2 import connect
from goose3 import Goose, Article
from re import compile
from collections import Counter
from sqlalchemy import create_engine
from multiprocessing import Pool
from os import cpu_count

import math
import pandas as pd

con = connect("dbname=benford user=postgres password=53P0RMVlCi host=192.168.1.17 port=30321")
cursor = con.cursor()

wikipedia_random_url = "https://en.wikipedia.org/wiki/Special:Random"
number_regex = compile("\d+\.?,?\d+")
remove_citations = compile("([(\[])\d+?([)\]])")


def extract_numbers(text) -> List[float]:
    cleaned = remove_citations.sub("", text)
    return list(set([float(num.strip().replace(",", "")) for num in number_regex.findall(cleaned)]))


def count_words(text) -> int:
    return len(text.split(" "))


def count_letters(text) -> int:
    return len(text)


def get_first_digit(number: float) -> int:
    return int(number // 10 ** (int(math.log(number, 10)) - 1 + 1)) if number > 0 else 0


def bucket_numbers(numbers: List[float]) -> Counter:
    first_digits = map(get_first_digit, numbers)
    return Counter(first_digits)


def fetch_article() -> Article:
    with Goose() as goose:
        return goose.extract(url=wikipedia_random_url)


def insert_data(data: pd.DataFrame):
    engine = create_engine('postgresql+psycopg2://postgres:53P0RMVlCi@192.168.1.17:30321/benford')
    data.to_sql('numbers', engine, if_exists='append', index=False)


def benford(request_size=50):
    columns = ['word_count', 'letter_count', 'number_count',
               'ones', 'twos', 'threes',
               'fours', 'fives', 'sixes',
               'sevens', 'eights', 'nines',
               'url', 'title', 'numbers']
    df = pd.DataFrame(columns=columns)
    for i in range(request_size):
        try:
            random_article = fetch_article()
            title = random_article.title
            link = random_article.canonical_link
            text = random_article.cleaned_text
            numbers = extract_numbers(text)
            if len(numbers) == 0: continue

            word_count = count_words(text)
            letter_count = count_letters(text)
            number_counts = bucket_numbers(numbers)
            row = [word_count, letter_count, len(numbers),
                   number_counts[1], number_counts[2], number_counts[3],
                   number_counts[4], number_counts[5], number_counts[6],
                   number_counts[7], number_counts[8], number_counts[9],
                   link, title, numbers]
        except Exception as ex:
            print(ex)
            continue
        df.loc[i] = row
    insert_data(df)
    print(f"finished batch {i}")


if __name__ == '__main__':
    with Pool(cpu_count() - 1) as pool:
        pool.map(benford, range(10_000))
