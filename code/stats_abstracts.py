import csv
import time
import sys

from bs4 import BeautifulSoup
from web_scrape import scraper
from urllib.error import URLError

import nltk.tokenize as tok
from nltk.corpus import stopwords

URL_BASE = 'https://arxiv.org'
SUBJECTS = {'AP', 'CO', 'ME', 'ML', 'OT', 'ST'}
NUMB_ABS = [409, 432, 423, 419, 499, 536, 428, 372, 438]  # number of stat subject links to fetch per year
# Use link to move to each month of articles
# from each month, go to view all and get abstracts and
# label with main subject


###############################################################################

def failed_connection(link, err):
    """Output for a failed connection"""
    print(f'Tried to connect to this link: {link}')
    print("and pull it's html contents.")
    print("DIDN'T WORK")
    print(err)


def get_page(link):
    scrape = scraper.UrlScraper(link)
    soup = BeautifulSoup(scrape.data, 'html.parser')
    return scrape, soup


def get_month_links(soup):
    """Return list of links to month recent pages."""
    month_link_ls = []
    tags = soup.find_all(lambda tag:
                         tag.name == 'a' and
                         tag.get_text() and
                         tag.get_text()[:2] == '19')
    for tag in tags:
        month_link = tag.get('href')
        month_link_ls.append(URL_BASE + month_link)
    return month_link_ls


def get_month_all_link(soup):
    """Return the link to the month page that contains links to the abstracts for all the papers posted that month."""
    tag = soup.find_all(lambda tag:
                        tag.name == 'a' and
                        tag.get_text() == 'all')[0]
    link = URL_BASE + tag.get('href')
    return link


def get_abstract_links(soup):
    """Return list of the links to the abstracts contained in the soup object (monthly page)."""
    abstract_link_ls = []
    for tag in soup.find_all(lambda tag: tag.name == 'dt'):
        abstract_ref = tag.find_all('a')[1].get_text()  # arXiv:1910.00006
        abtract_numb = abstract_ref[6:]  # 1910.00006
        abstract_link = URL_BASE + '/abs/' + abtract_numb  # https://arxiv.org/abs/1910.00006
        abstract_link_ls.append(abstract_link)
    return abstract_link_ls


def get_abstract_info(abstract_soup):
    """Return the abstract and its category as strings."""
    text = abstract_soup.find_all('blockquote')[0].get_text()
    # Extract category
    category = abstract_soup.find_all(lambda tag:
                                     tag.name == 'td' and
                                     tag.get_text() and
                                     tag.get_text()[:5] == 'arXiv')[0].get_text()[-9:][6:8]
    return text, category


def clean_text(text, category):
    """Extract the 'strong' words in text.

        Arguments:
            text -- str, abstract of paper
            subject -- str, category of corresponding abstract

        Returns:
            words -- list of strong words in the abstract, first word is the label

    - Tokenize: split into list of words with punctuation separated
    - Make words lowercase and remove symbols
    - Get rid of stop words (and letters)
    - Add subject to the beginning (for later classification)
    """
    tokens = tok.word_tokenize(text)[1:]  # get rid of 'abstract'
    words = [word.lower() for word in tokens if word.isalpha()]  # remove punctuation and make lowercase
    stop_words = set(stopwords.words('english') + list('abcdefghijklmnopqrstuvwxyz'))
    words = [category] + [w for w in words if w not in stop_words]  # remove stop words
    return words


def go_sleep(mult, dur=300):
    """Pause computation for dur*mult seconds"""
    dur = mult * dur
    print('=' * 80)
    print(f'Sleeping for {dur / 60} minutes ... ')
    time.sleep(dur)
    print('=' * 80)


###############################################################################

def main():
    t1 = time.time()
    url_block_ct = 0

    # Get the recent statistics 2019 research paper page (contains links to recent papers from each month)
    # If not, terminate
    try:
        arxiv_scraper, soup = get_page(URL_BASE + '/year/stat/19')
    except Exception as err:
        failed_connection(URL_BASE + '/year/stat/19', err)
        sys.exit()
    print('Connected to main page\n')

    # Get all monthly page links for year 2019 (so far)
    month_link_ls = get_month_links(soup)
    month_ct = 1
    for month_link in month_link_ls:
        try:
            month_scraper, month_soup = get_page(month_link)
        except URLError as url_err:
            failed_connection(month_link, url_err)
            url_block_ct += 1
            go_sleep(url_block_ct)
            # Try again
            try:
                month_scraper, month_soup = get_page(month_link)
            except URLError as url_err:
                failed_connection(month_link, url_err)
                print('Tried again and still could not connect.')
                break
        except Exception as err:
            print('Something went wrong')
            print(err)
            continue
        print(f'Connected to month page: {month_link}\n')

        # Go to link with ALL papers from that month
        month_all_link = get_month_all_link(month_soup)
        try:
            month_all_scraper, month_all_soup = get_page(month_all_link)
        except URLError as url_err:
            failed_connection(month_all_link, url_err)
            url_block_ct += 1
            go_sleep(url_block_ct)
            # Try again
            try:
                month_all_scraper, month_all_soup = get_page(month_all_link)
            except URLError as url_err:
                failed_connection(month_all_link, url_err)
                print('Tried again and still could not connect.')
                break
        except Exception as err:
            print('Something went wrong')
            print(err)
            continue
        print(f'Connected to month ALL page: {month_all_link}\n')

        # Extract all the abstracts (and their subject) for that month
        abstract_link_ls = get_abstract_links(month_all_soup)
        abs_ct = 1
        for abstract_link in abstract_link_ls:
            if abs_ct >= NUMB_ABS[month_ct-1]:
                break
            abs_ct += 1
            try:
                abstract_scraper, abstract_soup = get_page(abstract_link)
            except URLError as url_err:
                failed_connection(abstract_link, url_err)
                url_block_ct += 1
                go_sleep(url_block_ct)
                # Try again
                try:
                    abstract_scraper, abstract_soup = get_page(abstract_link)
                except URLError as url_err:
                    failed_connection(abstract_link, url_err)
                    print('Tried again and still could not connect.')
                    break
            except Exception as err:
                print('Something went wrong')
                print(err)
                continue
            print(f'Connected to month {month_ct} abstract page {abs_ct}: {abstract_link}\n')
            text, subject = get_abstract_info(abstract_soup)
            if subject not in SUBJECTS:
                continue  # Ensure valid label
            words = clean_text(text, subject)  # clean up abstract

            # Write to file
            with open(f'abstracts_all.txt', 'a+') as file:
                csv_writer = csv.writer(file)
                csv_writer.writerow(words)

        month_ct += 1

    print(time.time() - t1)


if __name__ == '__main__':
    main()
