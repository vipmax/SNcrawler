import operator

import unicodecsv as csv
import os
import re
import pymorphy2
import pymongo
from dateutil.relativedelta import relativedelta
import datetime
from stop_words import get_stop_words
import functional

high_quality_groups = [-41240468, -74058720, -50305445, -81526971, -47214165]
mid_quality_groups = [ -465, -36286006, -55821382]
low_quality_groups = [-30525261, -23611958, -38119975, -41538339, -86218441]
all_groups = high_quality_groups + mid_quality_groups + low_quality_groups

group_names = {
    -41240468: "Дайджест психологических исследований",
    -74058720: "Когнитивистика",
    -50305445: "Психология — русский журнал",
    -81526971: "Факультет психологии",
    -47214165: "Praxis",

    -465: "Факультет психологии СПбГУ",
    -36286006: "Департамент психологии НИУ ВШЭ",
    -55821382: "Факультет психологии МГУ имени М.В. Ломоносова",

    -30525261: "Psychology|Психология",
    -23611958: "Психология",
    -38119975: "Психология",
    -41538339: "Практическая психология",
    -86218441: "Психология",

    "high_quality_groups": "high_quality_groups",
    "mid_quality_groups": "mid_quality_groups",
    "low_quality_groups": "low_quality_groups",
    "all_groups": "all_groups"
}

morph = pymorphy2.MorphAnalyzer()

stop_words = get_stop_words('en')
stop_words.extend(get_stop_words('ru'))
stop_words.extend(['И','b'])

def filter_stop_words(words):
    for word in words:
        if word not in stop_words and word != '':
            yield word


year_ago = datetime.datetime.now() - relativedelta(years=1)
filter = {
    'date': {'$gte': int(year_ago.timestamp())},
    'copy_history': {'$exists': False},
    'text': { '$ne': '' }
}

print(filter)

cursor = pymongo.MongoClient("192.168.13.133")['Prichislenko']['posts'].find(filter)
remaining = cursor.count()
print(remaining)

thesaurus = {}


def update_thesaurus(normed, group):
    for norm in normed:
        th_freqs = thesaurus.get(group, {})
        freq = th_freqs.get(norm, 0) + 1
        th_freqs.update({norm: freq})
        thesaurus.update({group: th_freqs})


data = list(cursor)
# data = cursor


for c in data:
    text = str(c['text'])
    f_text = text.replace("\n",' ').lower()
    cleaned_text = re.sub(r'[^a-zA-ZА-яёЁ ]', '', f_text).split(" ")
    cleaned_text_2 = list(filter_stop_words(cleaned_text))

    normed = []
    for cleaned_t in cleaned_text_2:
        t__normalized = morph.parse(cleaned_t)[0].normalized.normal_form
        normed.append(t__normalized)

    words = list(filter_stop_words(normed))



    # print("text=",text)
    # print("cleaned_text=",cleaned_text)
    # print("normalized=", normed)

    group = c['from_id']

    update_thesaurus(words, group)

    print(group)
    if group in high_quality_groups:
        print("high_quality_groups")
        update_thesaurus(words, "high_quality_groups")


    if group in mid_quality_groups:
        print("mid_quality_groups")
        update_thesaurus(words, "mid_quality_groups")

    if group in low_quality_groups:
        print("low_quality_groups")
        update_thesaurus(words, "low_quality_groups")

    if group in all_groups:
        print("all_groups")
        update_thesaurus(words, "all_groups")

    remaining -= 1

    print("remaining {} posts".format(remaining))



for g,th in thesaurus.items():
    print(g,th)

    filename = "prichislenko2/{}_{}_thesaurus.csv".format(group_names[g],g)

    with open(os.path.join("", filename), "wb") as f:
        wrt = csv.writer(f, dialect='excel', delimiter='\t', encoding='utf-16')

        header = "word, frequency"
        wrt.writerow([header])

        for w in sorted(th, key=th.get, reverse=True):
            wrt.writerow([w,th[w]])
