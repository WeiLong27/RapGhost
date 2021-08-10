#!/usr/bin/env python
# coding: utf-8


# import libraries
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd

total_links = ['https://www.ohhla.com/all.html', 'https://www.ohhla.com/all_two.html',
               'https://www.ohhla.com/all_two.html',
               'https://www.ohhla.com/all_three.html', 'https://www.ohhla.com/all_four.html',
               'https://www.ohhla.com/all_five.html']


def get_parent_links(parent_link):
    soup = BeautifulSoup(requests.get(parent_link).text, 'html.parser')
    gross_links = [link['href'] for link in soup.find_all('a', href=True)]
    return gross_links[:-2]


# reference to see how long this takes
start = time.time()

# retrieve all links and store them in a list of lists
# each set is links per link in total links ( in total links, we have all the artists links)
# so here is stored all links(albums) per artist
links_per_total_links = []

processes = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for link in total_links:
        processes.append(executor.submit(get_parent_links, link))

for task in as_completed(processes):
    links_per_total_links.append(task.result())

end = time.time()

# store all of the links in all_links
all_links = [item for sublist in links_per_total_links for item in sublist]
unique_links = []
# ignore duplicates
unique_links = [link for link in all_links if link not in unique_links]
link_df = pd.DataFrame(unique_links)
# link_df.head(50)


# remove links that either didnt lead to lyrics, or to another page with diff formats
# very little of these, so decided not to include these in dataset
ohhla = link_df[link_df[0].apply(lambda x: x[:len("http://ohhla.com/")] == "http://ohhla.com/")].index.tolist()

amazon = link_df[link_df[0].apply(lambda x: "www.amazon.com/" in x)].index.tolist()

itunes = link_df[
    link_df[0].apply(lambda x: x[:len("http://itunes.apple.com/")] == "http://itunes.apple.com/")].index.tolist()

apk = link_df[
    link_df[0].apply(lambda x: x[:len("https://www.apkfollow.com/")] == "https://www.apkfollow.com/")].index.tolist()

all_text = link_df[link_df[0].apply(lambda x: x[:len("all")] == "all")].index.tolist()

all_html = link_df[link_df[0].apply(lambda x: ".html" in x)].index.tolist()

rap_reviews = link_df[
    link_df[0].apply(lambda x: x[:len("http://rapreviews.com/")] == "http://rapreviews.com/")].index.tolist()

angry_marks = link_df[
    link_df[0].apply(lambda x: x[:len("http://angrymarks.com/")] == "http://angrymarks.com/")].index.tolist()

total_remove = ohhla + amazon + itunes + apk + all_text + all_html + rap_reviews + angry_marks

link_df.drop(total_remove, inplace=True)

# create text file
link_df.to_csv('initial_directories.txt', header=False, index=False)

# print(end-start)


# --------------------# Get Sub-Directories of each Artists (album links which leads to song links)---------------

# read text file
dir_list = pd.read_csv(r'initial_directories.txt', header=None)[0].to_list()

sub_dir_list = []


# print(dir_list)

def get_sub_directories(parent_dir):
    url = "https://ohhla.com/" + parent_dir
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    gross_links = [parent_dir + link['href'] for link in soup.find_all('a', href=True) if
                   "/" in link['href'] and "anonymous" not in link['href']]
    return gross_links


start = time.time()
processes = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for link in dir_list:
        processes.append(executor.submit(get_sub_directories, link))

for task in as_completed(processes):
    sub_dir_list.append(task.result())

unpacked_sub_dir_list = [item for each_dir in sub_dir_list for item in each_dir]

end = time.time()
print(end - start)

# print(unpacked_sub_dir_list)


pd.DataFrame(unpacked_sub_dir_list).to_csv('total_sub_directories.txt', header=False, index=False)

# -------------------Get Songs of each Artists (links to .txt files)--------------------

# read text file
all_dir_list = pd.read_csv(r'total_sub_directories.txt', header=None)[0].to_list()

text_links = []


def get_text_links(parent_dir):
    url = "https://ohhla.com/" + parent_dir
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    gross_links = [parent_dir + link['href'] for link in soup.find_all('a', href=True) if ".txt" in link['href']]
    return gross_links


start = time.time()
processes = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for link in all_dir_list:
        processes.append(executor.submit(get_text_links, link))

for task in as_completed(processes):
    text_links.append(task.result())

unpacked_text_links = [item for each_dir in text_links for item in each_dir]

end = time.time()
print(end - start)

pd.DataFrame(unpacked_text_links).to_csv('total_text_links.txt', header=False, index=False)

# --------------------------GET LYRICS FROM EACH TEXT FILE-------------------------

start = time.time()
text_link_df = pd.read_csv('total_text_links.txt', header=None, names=['Text_Link'])

text_link_df.head()


def get_lyrics(text_link):
    url = "https://ohhla.com" + text_link
    html = ''
    while html == '':
        try:
            html = requests.get(url).text
            break
        except:
            print("Connection refused by the server..")
            print("Let me sleep for a bit")
            print("ZZzzzz...")
            time.sleep(100)
            print("Was a nice sleep, now let me continue...")
            continue
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(10)

    # remove unnecessary stuff eg. social media links
    if soup.find('pre'):
        double_loc = soup.find('pre').text.find("\n\n") + 2
        return soup.find('pre').text[double_loc:]
    else:
        double_loc = html.find('\n\n') + 2
        return html[double_loc:]


lyrics_list = []

start = time.time()
processes = []

with ThreadPoolExecutor(max_workers=10) as executor:
    for count, link in enumerate(text_link_df['Text_Link']):
        print(count)
        processes.append(executor.submit(get_lyrics, link))

for task in as_completed(processes):
    lyrics_list.append(task.result())

end = time.time()
print(end - start)

with open('total_lyrics.txt', 'a') as f:
    for lyrics in set(lyrics_list):
        f.write(lyrics)
        f.write('\n' * 2)
    f.close()

# --------------------To Add: clean until remain only english lyrics------------------
