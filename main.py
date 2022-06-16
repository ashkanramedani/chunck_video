import os
import logging
import sqlite3


class Tools:
    def __init__(self) -> None:
        pass

    def getDirList(self, path: str) -> list:
        return os.listdir(path)

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

    def __del__(self) -> None:
        logging.info('Tools Object is Deleted')


class Slite:
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

    def Insert(self, query: str) -> None:
        if self.conn is None:
            self.Connect()
        curs = self.conn.cursor()
        curs.execute(query)
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
        self._objSlite = Slite()
        self.temporary_path = './project/temporary'
        self.video_path_file = config['video_path_file']
        self.storage_path = config['storage_path']
        self.result_path = config['result_path']
        self.chunk_size = config['chunk_size']
        self.testConfigFlag = self.testConfig()

    def CreateTable(self):
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
    def testConfig(self) -> bool:
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

    def chunker_video(self) -> None:
        if not self.testConfigFlag:
            return
        if not self._objTools.makeDir(self.temporary_path):
            return

        start_byte = 0
        part = 1

        # get bytes size file
        fileSize = os.path.getsize(self.video_path_file)
        filePath = self.video_path_file[:self.video_path_file.rindex("/")]
        fileName = self.video_path_file[self.video_path_file.rindex("/")+1:self.video_path_file.rindex(".")]
        fileFormat = self.video_path_file[self.video_path_file.rindex(".")+1:]
        totalPartNumber = int(fileSize/self.chunk_size) + (0 if fileSize%self.chunk_size == 0 else 1)

        self._objSlite.Insert(f"""
            INSERT INTO files 
            (file_name, file_path, file_size, file_format, part_number, total_part, parent_id) 
            VALUES
            ('{fileName}', '{filePath}', {fileSize},'{fileFormat}', NULL, NULL, NULL);
        """)

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

            self._objSlite.Insert(f"""
                INSERT INTO files
                (file_name, file_path, file_size, file_format, part_number, total_part, parent_id)
                VALUES
                ('{fileName}', '{filePath}', {fileSize},'{fileFormat}', {part}, {totalPartNumber}, {parent_id});
            """)

            video_chunked_file = open(self.temporary_path + f"/{fileName}-{part} out of {totalPartNumber}.{fileFormat}", "ab")
            video_chunked_file.write(video_chunked)
            video_chunked_file.close()
            part += 1

    def __del__(self) -> None:
        logging.info('Chunk Object is Deleted')


config = {
    "chunk_size": 2097152,
    "video_path_file": "./project/videos/filim.mp4",
    "storage_path": "./project/storage",
    "result_path": "./project/result"
}
_objChunk = Chunk(config)
# _objChunk.CreateTable()
_objChunk.chunker_video()


if __name__ == '__main__':
    # set video path
    vpath = 'project/videos/filim.mp4'
