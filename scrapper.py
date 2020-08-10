import re
import requests
import bs4
import time
import dateparser
from collections import defaultdict
import atexit
import signal
import requests
from bs4 import BeautifulSoup

from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint


def get_page(url, page_number):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    page = requests.get(url + str(page_number), headers=headers)
    page.raise_for_status()
    return BeautifulSoup(page.content, 'html.parser')


def get_page_text(page, content_class='read__internal_content', is_ignore=True, ignore_class='masha-ignore'):
    """
    Returns all text inside <p> ... </p> tags in <div class=content_class>
    Ignore class is optional, added to ignore date at the end of the page
    This function must be changed when kremlin.ru HTML changes.
    """
    paragraphs = []
    date = None
    for s in page.find('div', {'class': content_class}).find_all('p'):
        parent = s.parent
        if is_ignore and ignore_class in parent.find_parent('div').attrs['class']:
            if s.text.strip().startswith('Дата публикации'):
                # Parse "Дата публикации:   <date>"
                date = dateparser.parse(s.text.strip()[16:].strip().split(',')[0])
            continue
        paragraphs.append(s.text)
    return date, ' '.join(paragraphs)


def extract_persons_from_text(text, person_regexp=None):
    """
    Split text into list [('Person_name', 'Speech of that person')]
    Example:
        В.Путин:
        Этим вопросом я займусь лично.
        Д.Медведев:
        Хорошо, Владимир Владимирович.
    Transforms into [('В.Путин:','Этим вопросом я займусь лично.'), ('Д.Медведев','Хорошо, Владимир Владимирович.')]
    """
    persons_and_texts = []
    copy_text = text
    exp = re.compile('[А-ЯЁ].[А-ЯЁ][а-яё]+:') if not person_regexp else person_regexp
    current_person = ''
    while True:
        person_match = exp.search(copy_text)
        if not person_match:
            persons_and_texts.append((current_person, copy_text))
            break
        span = person_match.span()
        persons_and_texts.append((current_person, copy_text[:span[0]]))
        current_person = copy_text[span[0]:span[1]]
        copy_text = copy_text[span[1]:]
    if persons_and_texts[0] == ('', ''):
        persons_and_texts = persons_and_texts[1:]
    return persons_and_texts


def get_all_page_numbers(page_numbers_file='numbers.txt'):
    page_numbers_list = []
    with open(page_numbers_file, 'r') as file:
        for line in file:
            if line.strip('\n').isdigit():
                page_numbers_list.append(int(line))
    return page_numbers_list


def smart_page_request(url, page_num, default_delay):
    current_delay = default_delay
    current_page = None
    while not current_page:
        try:
            current_page = get_page(url, page_num)
        except requests.HTTPError:
            current_page = None
            current_delay = max(2, current_delay) ** 2
        time.sleep(current_delay)
    return current_page


def save_index():
    global _index
    with open('index.txt', 'w') as index_file:
        print(f'Program is terminated, {_index} pages processed')
        index_file.write(str(_index))


if __name__ == '__main__':
    # All pages with transcripts have url like base_url + {some_number}
    # Valid page numbers scrapped with web_notebook and stored in numbers.txt
    base_url = 'http://www.kremlin.ru/events/president/transcripts/'
    upper_bound = 10

    # Get last index processed at the last program launch

    with open('index.txt', 'r') as index_file:
        last_index = int(index_file.read())

    page_numbers = get_all_page_numbers()[last_index:]

    # Use MongoDB for saving speeches
    dataset = defaultdict(list)
    client = MongoClient('mongodb://localhost:27017')
    db = client.nlp

    _index = 0
    atexit.register(save_index)
    signal.signal(signal.SIGTERM, save_index)
    signal.signal(signal.SIGINT, save_index)
    cnt = 0
    print(len(page_numbers))
    for index, page_number in enumerate(page_numbers):
        _index = index
        page = smart_page_request(base_url, page_number, default_delay=0.5)
        date, text = get_page_text(page)
        extracted_list = extract_persons_from_text(text)
        for (person, speech) in extracted_list:
            p = person[:-1]  # Remove ':'
            dataset[p].append(speech)
            item = {
                'name': p,
                'text': speech,
                'date': str(date)
            }
            # if db.speechs.find_one({'name': p, 'date': str(date)}):
            #    continue
            db.speechs.insert_one(item)

        if upper_bound and cnt == upper_bound:
            break
        if cnt % 100 == 0:
            print(f"{cnt // 100}00 pages processed")
        cnt += 1
    print(_index)