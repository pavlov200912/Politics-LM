import re
import requests
import bs4
import time
from collections import defaultdict

import requests
from bs4 import BeautifulSoup


def get_page(url, page_number):
    page = requests.get(url + str(page_number))
    page.raise_for_status()
    return BeautifulSoup(page.content, 'html.parser')


def get_page_text(page, content_class='read__internal_content', is_ignore=True, ignore_class='masha-ignore'):
    """
    Returns all text inside <p> ... </p> tags in <div class=content_class>
    Ignore class is optional, added to ignore date at the end of the page
    This function must be changed when kremlin.ru HTML changes.
    """
    paragraphs = []
    for s in page.find('div', {'class': content_class}).find_all('p'):
        parent = s.parent
        if is_ignore and ignore_class in parent.find_parent('div').attrs['class']:
            continue
        paragraphs.append(s.text)
    return ' '.join(paragraphs)


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


def get_all_pages_text(url, page_numbers, delay=0.3, upper_bound=None):
    all_texts = []
    cnt = 0
    for i in page_numbers:
        page = get_page(url, i)
        time.sleep(delay)
        if upper_bound and cnt == upper_bound:
            return all_texts
        if cnt % 100 == 0:
            print(f"{cnt // 100}00 pages has been gotten")
        all_texts.append(get_page_text(page))
        cnt += 1
    return all_texts


if __name__ == '__main__':
    # All pages with transcripts have url like base_url + {some_number}
    # Valid page numbers scrapped with web_notebook and stored in numbers.txt
    base_url = 'http://www.kremlin.ru/events/president/transcripts/'
    page_numbers = get_all_page_numbers()
    texts = get_all_pages_text(base_url, page_numbers, upper_bound=10)
    dataset = defaultdict(list)
    for text in texts:
        # TODO: Add date extracting
        extracted_list = extract_persons_from_text(text)
        for (person, speech) in extracted_list:
            p = person[:-1]  # Remove ':'
            dataset[p].append(speech)
    for k, v in dataset.items():
        print(f"PERSON: {k}")
        for text in v:
            print("___" * 15 + '\n')
            print(text)
