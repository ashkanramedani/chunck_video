import os
import logging
import random
import sqlite3
import unittest

class Tools:
    def __init__(self) -> None:
        pass

    def getAllInDir(self, path: str) -> list:
        return os.listdir(path)

    def getDirList(self, path: str) -> list:
        data = self.getAllInDir(path)
        result = []
        for i in data:
            if os.path.isdir(f"{path}/{i}"):
                result.append(i)
        return result

    def makeDir(self, path: str) -> bool:
        if not self.isExistedDir(path):
            os.mkdir(path)
        return True if self.isExistedDir(path) else False

    def isExistedDir(self, path: str) -> bool:
        return True if os.path.exists(path) else False

    def findInList(self, l: list, item: any) -> int:
        for i, ind in enumerate(l):
            if i == item:
                return ind
        return -1

    def WriteFileDir(self, path: str, data: any, format="wb") -> None:
        video_chunked_file = open(path, format)
        video_chunked_file.write(data)
        video_chunked_file.close()

    def __del__(self) -> None:
        logging.info('Tools Object is Deleted')

class Sqlite:
    def __init__(self) -> None:
        self.conn = None
        self.dbPath = 'fileManager.db'

    def Connect(self) -> None:
        self.conn = sqlite3.connect(self.dbPath)

    def Close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def Select(self, query: str) -> list:
        if self.conn is None:
            self.Connect()
        curs = self.conn.cursor()
        curs.execute(query)
        return curs.fetchall()

    def Insert(self, data: dict, table: str) -> None:
        if self.conn is None:
            self.Connect()
        curs = self.conn.cursor()
        keys = ",".join(data["keys"])
        value = str(data["values"])[1:-1]
        curs.execute(f'INSERT INTO {table} ({keys}) values ({value})')
        self.conn.commit()

    def CreateEmptyTable(self, tableExistedScript: str, createTableScript: str) -> None:
        if self.conn is None:
            self.Connect()
        curs = self.conn.cursor()
        curs.execute(tableExistedScript)
        curs.execute(createTableScript)
        self.conn.commit()

    def __del__(self):
        logging.info('Slite Object is Deleted')

class Chunk:
    def __init__(self, config: dict) -> None:
        self._objTools = Tools()
        self._objSlite = Sqlite()
        self.video_path_file = config['video_path_file']
        self.storage_path = config['storage_path']
        self.result_path = config['result_path']
        self.chunk_size = config['chunk_size']
        self.listDirStorage = self._objTools.getDirList(self.storage_path)
        self.testConfigFlag = self.TestConfig()

    def CreateTable(self) -> None:
        self._objSlite.CreateEmptyTable(

            """DROP TABLE IF EXISTS "files";""",
            """
                CREATE TABLE "files" (
                    "file_id" INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    "file_name" TEXT NOT NULL,
                    "file_path" TEXT,
                    "file_size" INTEGER,
                    "file_format" TEXT,
                    "part_number" INTEGER,
                    "total_part" INTEGER,
                    "parent_id" INTEGER
                );
            """
        )

    def TestConfig(self) -> bool:
        flags = []
        flags_error = [
            "storage path is not existed."
            "result path is not existed."
            "video in this path file is not existed."
        ]
        flags.append(self._objTools.isExistedDir(self.storage_path))
        flags.append(self._objTools.isExistedDir(self.result_path))
        flags.append(self._objTools.isExistedDir(self.video_path_file))
        if False in flags:
            logging.error('Config is NOK')
            logging.error(f'+ {flags_error[self._objTools.findInList(flags, False)]}')
            return False

        else:
            logging.debug('Config is OK')
            return True

    def MergeChunckVideo(self, fileName: str) -> None:
        parent_id = self._objSlite.Select(f"""SELECT file_id from files WHERE file_name='{fileName}' and parent_id is NULL limit 1;""")
        if len(parent_id) > 0:
            parent_id = parent_id[0][0]

        chunked_list = self._objSlite.Select(f"""SELECT file_id, file_name, file_path, file_format, part_number, total_part from files WHERE parent_id={parent_id};""")

        masterFile = f"{self.result_path}/{fileName}.mp4"
        flg = None
        for i in chunked_list:
            if flg is None:
                flg = [False]*(i[5]+2)
            if not flg[i[4]]:
                if self._objTools.isExistedDir(f"{i[2]}/") == False or self._objTools.isExistedDir(f"{i[2]}/{i[1]}.{i[3]}") == False:
                    continue
                flg[i[4]] = True

                path = f"{i[2]}/{i[1]}.{i[3]}"
                with open(path, 'rb') as file:
                    content = file.read()
                self._objTools.WriteFileDir(masterFile, content, "ab")

    def ChunkerVideo(self) -> None:
        if not self.testConfigFlag:
            return

        start_byte = 0
        part = 1

        # get bytes size file
        fileSize = os.path.getsize(self.video_path_file)
        filePath = self.video_path_file[:self.video_path_file.rindex("/")]
        fileName = self.video_path_file[self.video_path_file.rindex("/")+1:self.video_path_file.rindex(".")]
        fileFormat = self.video_path_file[self.video_path_file.rindex(".")+1:]
        totalPartNumber = int(fileSize/self.chunk_size) + (0 if fileSize%self.chunk_size == 0 else 1)

        data = {
            'keys': ['file_name', 'file_path', 'file_size', 'file_format'],
            'values': [f'{fileName}', f'{filePath}', fileSize, f'{fileFormat}']
        }
        self._objSlite.Insert(data, 'files')

        parent_id = self._objSlite.Select(f"""SELECT file_id from files WHERE file_name='{fileName}' and parent_id is NULL limit 1;""")
        if len(parent_id) > 0:
            parent_id = parent_id[0][0]

        # read video file
        with open(self.video_path_file, 'rb') as file:
            content = file.read()

        while True:
            remaining_size = fileSize - start_byte

            if start_byte == fileSize:
                break

            if remaining_size >= self.chunk_size:
                video_chunked = content[start_byte:start_byte+self.chunk_size]
                start_byte += self.chunk_size
            else:
                video_chunked = content[start_byte:start_byte+remaining_size]
                start_byte += remaining_size

            cnt = 0
            flg = [False]*len(self.listDirStorage)
            while True:
                rand = random.randint(0, len(self.listDirStorage)-1)
                if not flg[rand]:
                    flg[rand] = True
                    self._objTools.WriteFileDir(self.storage_path + f"/{self.listDirStorage[rand]}" + f"/{fileName}-copy {cnt}-{part} out of {totalPartNumber}.{fileFormat}", video_chunked)
                    data = {
                        'keys': ['file_name', 'file_path', 'file_size', 'file_format', 'part_number', 'total_part', 'parent_id'],
                        'values': [f'{fileName}-copy {cnt}-{part} out of {totalPartNumber}', f'{self.storage_path+ f"/{self.listDirStorage[rand]}"}', fileSize, f'{fileFormat}', part, totalPartNumber, parent_id]
                    }
                    self._objSlite.Insert(data, 'files')
                    cnt += 1
                if cnt == 3:
                    break

            part += 1

    def __del__(self) -> None:
        logging.info('Chunk Object is Deleted')

class Test(unittest.TestCase):
    def TestConfig(self):
        pass

if __name__ == '__main__':
    # set video path
    _objTools = Tools()
    fileName = 'saba.mp4'
    config = {
        "chunk_size": 2097152,
        "video_path_file": f"./project/videos/{fileName}",
        "storage_path": "./project/storage",
        "result_path": "./project/result"
    }

    if _objTools.isExistedDir(config['video_path_file']):
        _objChunk = Chunk(config)
        # _objChunk.CreateTable()
        _objChunk.ChunkerVideo()
        _objChunk.MergeChunckVideo(fileName[:fileName.rindex('.')])
