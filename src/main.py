import aiohttp
import asyncio
import csv
import os
from ftplib import FTP
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
        self.ftp_object = FTP(self.ftp_uri)
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

    def iterate_directory(self, file_location: str) -> iter:
        try:
            file_list = self.ftp_object.nlst(file_location)
            yield file_list
        except error_perm as e:
            print(f'Failed to change directory into {file_location} with error ...{e}')
            exit(1)

    @staticmethod
    def find_text_file(files: iter, row: dict) -> object:
        for file in files:
            if row["FileNumber"]+'.txt' in file:
                print('file name is {file}'.format(file=row['FileNumber']))
                return file
            elif row["FileNumber"]+'-0.txt' in file:
                print('file name is {file}'.format(file=row['FileNumber']))
                return file
            elif '.txt' in file:
                print('file name is {file}'.format(file=row['FileNumber']))
                return file
        print(f'No file found for {row}')

    def download_file(self, file: str, filename: str) -> None:
        try:
            with open('downloads/'+filename+'.txt', 'wb') as out_file:
                self.ftp_object.retrbinary('RETR %s' % file, out_file.write)
        except error_perm as e:
            print(f'failed to download {filename} located at {file} with error {e}')

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


def main():
    t0 = time.time()
    ingest = Gutenberg()
    ingest.load_csv_file()
    rows = ingest.iterate_csv_file()
    ingest.ftp_login()
    t3 = time.time()
    for row in rows:
        file_location = ingest.obtain_directory_location(row["FileNumber"])
        file_list = ingest.iterate_directory(file_location)
        for file in file_list:
            ftp_download_path = ingest.find_text_file(file, row)
            ingest.download_file(ftp_download_path, row["FileNumber"])
    t4 = time.time()
    ingest.iter_files
    t1 = time.time()
    total =  t1-t0
    downloadtime = t4-t3
    print(f'total time {total}')
    print(f'download time {downloadtime}')


if __name__ == '__main__':
    main()
