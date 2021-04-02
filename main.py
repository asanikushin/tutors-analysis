import json
from dataclasses import dataclass
import csv
import os
import threading
import argparse
import pathlib

import typing

import requests
from bs4 import BeautifulSoup
import tqdm
import pyodbc

CNT = 0


@dataclass
class Profile:
    prof_id: str
    name: str
    gender: str
    rating: float
    reviews: int
    marks: typing.List[int]
    services: typing.List[str]
    trust: str
    url: str

    def to_list(self, services=True, flatten=False) -> tuple:
        global CNT
        result = [self.prof_id, self.name, self.gender,
                  self.rating, self.reviews]
        if flatten:
            result.extend(self.marks)
        else:
            result.append(self.marks)
        if services:
            result.append(self.services)
        result.extend([self.trust, self.url])
        CNT = max(CNT, len(self.services))
        return tuple(result)

    @staticmethod
    def get_const_services(services, num):
        return services[:num] + ["NULL"] * (num - len(services))

    def to_sql(self):
        return self.prof_id, self.name, self.gender, self.rating, self.reviews, *self.marks, self.url, \
               *Profile.get_const_services(self.services, 3)

    @staticmethod
    def to_sql_column():
        result = ["prof_id", "full_name", "gender",
                  "rating", "reviews"]
        result.extend(["mark_1", "mark_2", "mark_3", "mark_4", "mark_5"])
        result.extend(["url"])
        result.extend(["service_1", "service_2", "service_3"])
        return tuple(result)

    @staticmethod
    def to_sql_values():
        return ",".join("?" * len(Profile.to_sql_column()))

    @staticmethod
    def header(services=True, flatten=False) -> typing.Tuple[str, ...]:
        result = ["prof_id", "name", "gender",
                  "rating", "reviews"]
        if flatten:
            result.extend(["m1", "m2", "m3", "m4", "m5"])
        else:
            result.append("marks")
        if services:
            result.append("services")
        result.extend(["trust", "url"])
        return tuple(result)


RAW_CONFIG = dict(services=True, flatten=False)
FULL_CONFIG = dict(services=True, flatten=True)
AGGREGATE_CONFIG = dict(services=False, flatten=True)


def get_url_data(url: str) -> (str, bool):
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:86.0) Gecko/20100101 Firefox/86.0"
    }
    res = requests.get(url, headers=headers)
    return res.text, res.status_code != 404


def save_data(text: str, name: str, path: str) -> None:
    path = os.path.join(path, name) + ".html"
    with open(path, "w") as file:
        file.write(text)


def read_data(path: str, name: str) -> str:
    with open(os.path.join(path, name), "r") as data:
        return data.read()


def get_app_data(text: str) -> dict:
    start = text.find("window.APP_DATA")
    start = text.find("{", start)
    end = text.find("</script>", start)

    data = text[start:end]
    return json.loads(data)


def dict_get(data: dict, path: str) -> typing.Any:
    keys = path.split("/")
    cur = data
    for key in keys:
        if len(key) == 0:
            continue
        if type(cur) == list:
            key = int(key)
        cur = cur[key]
    return cur


def fill_profile(data: dict) -> Profile:
    prof_id = dict_get(data, "appState/commonData/pxf/filters/models/ids/0/id")
    name = dict_get(data, f"appState/page/profiles/{prof_id}/fullName")
    gender = dict_get(data, f"appState/page/profiles/{prof_id}/gender")
    rating: float = float((dict_get(data, f"/appState/page/profiles/{prof_id}/newRank") or "0").replace(",", "."))
    reviews: int = dict_get(data, "appState/page/reviews/totalCount")

    services_data: list = dict_get(data, f"appState/page/profiles/{prof_id}/topServices")
    services = [service["name"] for service in services_data]

    marks_data: list = dict_get(data, "/appState/page/reviews/reviewsMarksHistogram")
    marks = [0, 0, 0, 0, 0]
    for mark in marks_data:
        marks[int(mark["value"]) - 1] = mark["count"]

    trust_badges = dict_get(data, f"appState/page/profiles/{prof_id}/trustBadges")
    if len(trust_badges) > 0:
        trust = trust_badges[0]["type"]
    else:
        trust = ""

    url = dict_get(data, "appState/commonData/meta/og/url")
    return Profile(prof_id, name, gender,
                   rating, reviews, marks,
                   services, trust, url)


def extract_profile(line: typing.List[str]) -> Profile:
    return Profile(line[0], line[1], line[2],
                   float(line[3]), int(line[4]), json.loads(line[5]),
                   json.loads(line[6].replace("'", '"')), line[7], line[8])


def spider_main(start, end, skip=1, pos: int = 1, progress: tqdm.tqdm = None, counter: tqdm.tqdm = None):
    base_url = "https://profi.ru"
    base_page = "https://profi.ru/rph-profiles/"

    pbar = tqdm.tqdm(total=500, position=pos, postfix=f"thread: {pos:02d}")
    for page in range(start, end, skip):
        page_url = f"{base_page}?p={page}"
        path = os.path.join(DATA_DIR, str(page))
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)
        a, b = process_web_page(page, page_url, base_url, path, pbar, counter)
        if progress is not None:
            progress.update(1)


def process_web_page(page: int, page_url: str, base_url: str,
                     base_dir: str,
                     pbar: tqdm.tqdm, counter: tqdm.tqdm = None) -> (int, int):
    profiles_data, found = get_url_data(page_url)
    if not found:
        print(f"page {page_url} not found")
        return 0, 0
    path = os.path.join(base_dir, PROFILES_DIR)
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    soup = BeautifulSoup(profiles_data, "html.parser")
    links = soup.find_all("h2")

    fails_cnt = 0
    pbar.reset(len(links))
    pbar.set_description(f"{page=}")

    for index, header in enumerate(links):
        header_link = header.find("a")
        link: str = header_link.attrs["href"]
        prof_id = link.split("/")[-2]
        try:
            profile_text, ok = get_url_data(base_url + link)
            save_data(profile_text, prof_id, path)
        except:
            fails_cnt += 1

        pbar.update(1)
        if counter is not None:
            counter.update(1)
    return len(links), fails_cnt


def parse_main(start, end, skip=1, pos: int = 1, progress: tqdm.tqdm = None, counter: tqdm.tqdm = None):
    pbar = tqdm.tqdm(total=500, position=pos, postfix=f"thread: {pos:02d}")
    done, fails_cnt = 0, 0
    for page in range(start, end, skip):
        path = os.path.join(DATA_DIR, str(page))
        with open(os.path.join(path, "fails.txt"), "w") as fails:
            with open(os.path.join(path, "data.csv"), "w") as csvfile:
                writer = csv.writer(csvfile, delimiter=",")
                writer.writerow(Profile.header(**RAW_CONFIG))
                a, b = process_local_page(page, csvfile, fails, writer, path, pbar, counter)
                done += a
                fails_cnt += b
        if progress is not None:
            progress.update(1)
    if done != 0:
        percent = round(fails_cnt / done * 100, 2)
    else:
        percent = 0
    print(f"thread={pos}:\ttotal = {done}\tfails = {fails_cnt}\t{percent= }%")


def process_local_page(page: int,
                       csvfile, fails, writer, base_dir: str,
                       pbar: tqdm.tqdm, counter: tqdm.tqdm = None) -> (int, int):
    path = os.path.join(base_dir, PROFILES_DIR)
    files = os.listdir(path)

    fails_cnt = 0
    pbar.reset(len(files))
    pbar.set_description(f"{page=}")

    for index, file in enumerate(files):
        prof_id = os.path.splitext(file)[0]
        try:
            profile_text = read_data(path, file)
            data = get_app_data(profile_text)
            profile = fill_profile(data)
            if writer is not None:
                writer.writerow(profile.to_list(**RAW_CONFIG))
        except Exception as err:
            if fails is not None:
                print(prof_id, file=fails)
            fails.flush()
            fails_cnt += 1

        pbar.update(1)
        if counter is not None:
            counter.update(1)
        if index % 10 == 0 and csvfile is not None:
            csvfile.flush()
    return len(files), fails_cnt


def aggregate_main(pb_pages: tqdm.tqdm, counter: tqdm.tqdm, conn=None):
    aggregate = os.path.join(DATA_DIR, AGGREGATE)
    pathlib.Path(aggregate).mkdir(parents=True, exist_ok=True)
    pbar = tqdm.tqdm(total=500, position=1)
    if conn is not None:
        cursor = conn.cursor()
        columns = ",".join(Profile.to_sql_column())
    else:
        cursor = None
        columns = ""
    with open(os.path.join(DATA_DIR, "full_data.csv"), "w") as full_data:
        writer = csv.writer(full_data, delimiter=",")
        writer.writerow(Profile.header(**FULL_CONFIG))
        for page in range(START, PAGES):
            pbar.reset(total=500)
            file = os.path.join(DATA_DIR, str(page), "data.csv")
            with open(file, "r") as csv_file:
                reader = csv.reader(csv_file, delimiter=",")
                next(reader)
                pbar.set_description(f"{page=}")
                for line in reader:
                    profile = extract_profile(line)
                    for service in profile.services:
                        service = service.lower().replace(" ", "_").replace("/", "_").replace(".", "_") + ".csv"
                        with open(os.path.join(aggregate, service), "a") as result:
                            res_writer = csv.writer(result, delimiter=",")
                            res_writer.writerow(profile.to_list(**AGGREGATE_CONFIG))
                    writer.writerow(profile.to_list(**FULL_CONFIG))
                    if cursor:
                        # print(profile.to_sql())
                        cursor.execute(f"insert into {config.table}({columns}) values ({Profile.to_sql_values()})",
                                       *profile.to_sql())
                    counter.update(1)
                    pbar.update(1)
            if conn is not None:
                conn.commit()
            pb_pages.update(1)
    return


def add_csv_header(file):
    with open(file, "r") as content:
        data = content.read()
    with open(file, "w") as result:
        writer = csv.writer(result, delimiter=",")
        writer.writerow(Profile.header(**AGGREGATE_CONFIG))
    with open(file, "a") as result:
        result.write(data)
    pass


def add_headers_to_files(files, pbar: tqdm.tqdm = None):
    for file in files:
        add_csv_header(file)
        if pbar:
            pbar.update(1)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--treads", default=1, type=int, help="Number of threads")
    parser.add_argument("mode", default="web", choices=["spider", "parse", "aggregate"],
                        help="Data source: collect data, parse local data or aggregate data",
                        nargs="*")
    parser.add_argument("-s", "--store", default="data", help="Directory to store data")
    parser.add_argument("--pages", default=300, type=int, help="Number of pages to process")
    parser.add_argument("--start", default=1, type=int, help="Number of pages to process")
    parser.add_argument("--upload", action="store_true", help="Upload data during aggregation phase")
    parser.add_argument("--server", help="SQL Server server for uploading data")
    parser.add_argument("--database", help="SQL Server database for uploading data")
    parser.add_argument("--username", help="SQL Server username for uploading data")
    parser.add_argument("--password", help="SQL Server password for uploading data")
    parser.add_argument("--table", help="table for uploading data")
    return parser.parse_args()


def wait_threads(threads):
    if type(threads) == list:
        for thread in threads:
            wait_threads(thread)
    else:
        threads.join()


def start_spider(pages_pb, counter):
    threads = []
    for thread_id in range(1, THREADS + 1):
        pos = thread_id
        threads.append(threading.Thread(target=spider_main, args=(pos, PAGES, THREADS, pos, pages_pb, counter)))
        threads[-1].start()

    wait_threads(threads)


def start_parse(pages_pb, counter):
    threads = []
    for thread_id in range(1, THREADS + 1):
        pos = thread_id
        threads.append(threading.Thread(target=parse_main, args=(pos, PAGES, THREADS, pos, pages_pb, counter)))
        threads[-1].start()

    wait_threads(threads)


def start_aggregate(pages_pb, counter, conn=None):
    aggregate_main(pages_pb, counter, conn)
    storage = os.path.join(DATA_DIR, AGGREGATE)
    print("add headers")
    agg_files = os.listdir(storage)
    add_headers_to_files(map(lambda x: os.path.join(storage, x), agg_files))
    print("done")


def main():
    pages_pb = tqdm.tqdm(total=PAGES, position=0, desc="Pages")
    counter = tqdm.tqdm(position=THREADS + 1, desc="Profiles")

    if "spider" in config.mode:
        start_spider(pages_pb, counter)
        pages_pb.clear()
        counter.clear()

    if "parse" in config.mode:
        start_parse(pages_pb, counter)
        pages_pb.clear()
        counter.clear()

    if "aggregate" in config.mode:
        conn = None
        if config.upload:
            connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server={config.server};" \
                                f"Database={config.database};uid={config.username};pwd={config.password}"
            # print(connection_string)
            conn = pyodbc.connect(connection_string)
        start_aggregate(pages_pb, counter, conn)
        pages_pb.clear()
        counter.clear()


if __name__ == '__main__':
    config = parse_args()
    print(config)

    THREADS = config.treads

    PROFILES_DIR = "profiles"
    AGGREGATE = "0_aggregate"
    DATA_DIR = config.store

    PAGES = config.pages
    START = config.start

    main()
