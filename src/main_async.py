import aioftp
import asyncio
import csv
import os
from ftplib import error_perm
import glob
import time


class Gutenberg():
    def __init__(self):
        self.request_url = ''
        self.csv_file_path = 'ingest_file/Gutenberg_files.csv'
        self.csv_data = []
        self.cwd = os.getcwd()
        self.ftp_uri = 'aleph.gutenberg.org'
        self.ftp_object = None
        self.download_uris = []
        self.file_download_locattion = 'downloads/'

    def load_csv_file(self):
        absolute_path = self.cwd
        file = self.csv_file_path
        with open(f'{absolute_path}/{file}', 'r') as file:
            data = csv.reader(file, delimiter=',')
            next(data, None)
            for row in data:
                self.csv_data.append({"author": row[0],
                                      "FileNumber": row[1],
                                      "Title": row[2]})

    def iterate_csv_file(self):
        for row in self.csv_data:
            yield row

    def ftp_login(self):
        self.ftp_object = aioftp.Client()
        self.ftp_object.connect(self.ftp_uri)
        self.ftp_object.login()
        print('logged into gutenberg ftp mirror')

    @staticmethod
    def obtain_directory_location(file_number: str):
        """Files are structured into directories by splitting each number, up UNTIL the last number. Then a folder
        named with the file number. So if a file number is 418, it is located at 4/1/418.
        Below 10 is just 0/filenumber."""
        file_location = ''
        for char in file_number[:-1]:
            file_location = file_location+char+'/'
        return file_location+file_number

    @staticmethod
    def find_text_file(file: object, row: dict) -> object:
        if row["FileNumber"]+'.txt' in file:
            return file
        elif row["FileNumber"]+'-0.txt' in file:
            return file
        elif '.txt' in file:
            return file

    @staticmethod
    def iter_lines(open_file: object, write_file: object) -> None:
        lines = open_file.readlines()
        start = False
        end = False
        for line in lines:
            if 'START OF THIS PROJECT' in line:
                start = True
            if 'End of Project' in line:
                end = True
            elif end:
                break
            elif start:
                if 'START OF THIS PROJECT' in line:
                    continue
                write_file.write(line + '\n')

    @property
    def iter_files(self):
        for file in glob.glob(self.file_download_locattion+'*.txt'):
            with open(file.replace('.txt','-mod.txt'), 'w', encoding='utf-8') as write_file:
                with open(file, 'r', encoding='ISO-8859-1') as open_file:
                    self.iter_lines(open_file, write_file)


async def download_file(gutenberg, file: str, filename: str) -> None:
    async with aioftp.ClientSession(gutenberg.ftp_uri) as client:
        try:
            await client.download(file, 'downloads/'+filename+'.txt', write_into=True)
        except error_perm as e:
            print(f'failed to download {filename} located at {file} with error {e}')


async def run(gutenberg):
    files = []
    rows = gutenberg.iterate_csv_file()
    for row in rows:
        file_location = gutenberg.obtain_directory_location(row["FileNumber"])
        files.append(file_location)
    async with aioftp.ClientSession(gutenberg.ftp_uri) as client:
        tasks = []
        for file_to_download in files:
            for path, info in (await client.list(file_to_download, raw_command='LIST')):
                text_file = gutenberg.find_text_file(str(path), row)
                if text_file:
                    print(file_to_download)
                    file_name = file_to_download[file_to_download.rfind('/')+1:]
                    task = asyncio.ensure_future(download_file(gutenberg, text_file, file_name))
                    tasks.append(task)
        results = await asyncio.gather(*tasks)
        return results


def main():
    t0 = time.time()
    ingest = Gutenberg()
    ingest.load_csv_file()
    t3 = time.time()
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(run(ingest))
    results = loop.run_until_complete(future)
    t4 = time.time()
    ingest.iter_files
    t1 = time.time()
    total =  t1-t0
    downloadtime = t4-t3
    print(f'total time {total}')
    print(f'download time {downloadtime}')


if __name__ == '__main__':
    main()
