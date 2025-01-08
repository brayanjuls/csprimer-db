import csv
import os
import struct
from typing import List

PAGE_SIZE = 4096
DB_HEADER_SIZE = 396

class DataBaseIO:
    
    def __init__(self,input_path,db_path,db_name,table_name,schema):
        self.header = DBHeader(db_name,table_name,schema)
        self.page = DBPage()
        self.db_path = db_path
        self.db = self.db_init()
        self.csv_f = open(input_path)
        self.reader = csv.reader(self.csv_f)
        #read header
        next(self.reader)

    def get_next_tuple(self) -> tuple:
        try:
            return tuple(next(self.reader))
        except:
            return ()

    def persist(self) -> bool:
        #should encode records and header and persist them to disk
        try:
            db_page = self.page.encode()
            db_header = self.header.encode()
            db_layout = bytearray()
            db_layout.extend(db_header)
            db_layout.extend(db_page)
            print("db_header size: {}".format(len(db_header)))
            print("db_page size: {}".format(len(db_page)))
            print("db_layout size: {}".format(len(db_layout)))

            print("header encoded")
            print(db_header)            
            self.db.seek(0)
            self.db.write(db_layout)
            self.flush()
        except:
            return False
        return True

    def read(self) -> tuple:
        #should load the entire db from the database and decode the records
        db_header_bytes = self.db.read(DB_HEADER_SIZE)
        #print(db_header_bytes)
        self.header.decode(db_header_bytes)
        page_bytes = self.db.read(PAGE_SIZE)
        #print(page_bytes)
        #print("\n")
        self.page.decode(page_bytes)
        #print(self.db.read())
        ()

    def add_record(self):
        #should add records and correctly update the db header
        pass


    def flush(self):
        self.db.flush()

    def db_init(self):
        if os.path.isfile(self.db_path):
           db = open(self.db_path,mode='r+b')
        else:
           db = open(self.db_path,mode='w+b')
           db_layout = bytearray()
           db_header = self.header.encode()
           db_page = self.page.encode()
           db_layout.extend(db_header)
           db_layout.extend(db_page)
           db.write(db_layout)
        return db

        
    
    def __del__(self):
        if self.csv_f is not None:
            self.csv_f.close()
        if self.db is not None:
            self.db.close()

class DBHeader:

    def __init__(self,db_name:str,table_name:str,schema:tuple,table_size=0,start_offset=0,end_offset=0):
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.table_size = table_size
        self.start_offset = start_offset # starting offset of the first page, this should help to read records
        self.end_offset = end_offset # ending offset of the last page, this should help to append new pages when the existing ones are full.
        self.byte_format = struct.Struct("<64s64s256siii")
        #should we include total number of pages?
    
    def encode(self) -> bytes:
        db_name = self.__get_db_name()
        table_name = self.__get_table_name()
        schema = self.__get_schema()
        start_offset = self.__get_start_offset()
        end_offset = self.__get_end_offset()
        table_size = self.__get_table_size()
        result = self.byte_format.pack(db_name,table_name,schema,table_size,start_offset,end_offset)
        return result
    
    def decode(self,header:bytes):
        #byte 129 starting point of schema
        #byte 385 starting point of table size
        #byte 389 starting point of start offset
        #byte 392 starting point of end offset
        #this function should decode the header and set the value on the current instance of the object
        pass

    def __get_db_name(self):
        return self.db_name.encode('utf-8')
    
    def __get_table_name(self):
        return self.table_name.encode('utf-8')
    
    def __get_schema(self):
        return ','.join(self.schema).encode('utf-8')
    
    def __get_start_offset(self):
        return self.start_offset
    
    def __get_end_offset(self):
        return self.end_offset
    
    def __get_table_size(self):
        return self.table_size
    
class PageHeader:
    def __init__(self,min_id:int,max_id:int,start_offset:int,end_offset:int,
                 record_pointers:List[tuple[int,int]],static_bytes_format:struct.Struct):
        self.min_id = min_id
        self.max_id = max_id
        self.start_offset = start_offset # end of records pointers should make it easy to add new records pointers
        self.end_offset = end_offset # end of last appended record, should make it easy to add new records
        self.record_pointers = record_pointers
        self.static_bytes_format = static_bytes_format
        self.size = len(record_pointers)# number of rows stored
        # should we include remaining free space variable?
    
    def encode(self) -> bytes:
        min_id = self.min_id
        max_id = self.max_id
        start_offset = self.start_offset
        end_offset = self.end_offset

        record_pointers = self.encode_record_pointers()

        size = self.size + len(record_pointers)
        static_header = self.static_bytes_format.pack(min_id,max_id,size,start_offset,end_offset)

        result = bytearray()
        result.extend(static_header)
        
       
        result.extend(bytes(record_pointers))
        return result

    def decode(self, header:bytearray) -> "PageHeader":
        #byte 0 min id
        #byte 4 max id
        #byte 8 size
        #byte 12 start offset
        #byte 16 end offset
        #byte 20 of page starting point of record pointers, use the start offset or size to limit how much you decode
        print(int.from_bytes(header[20:24],'little')) #first value of the tuple (record pointers)
        print(int.from_bytes(header[24:28],'little')) #second value of the tuple (record pointers)

        pass

    
    def encode_record_pointers(self) -> bytearray:
        record_pointers = bytearray()
        for o,s in self.record_pointers:    
            record_pointers.extend(struct.pack("<ii",o,s))
        return record_pointers

    def update(self,max_id:int,start_offset:int,end_offset:int):
        self.max_id = max_id
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.size = len(self.record_pointers)
    
class PageRecord:
    def __init__(self,record:tuple,schema:tuple):
        self.record = record
        self.schema = schema

    def encode(self) -> bytearray:
        result_record = bytearray()
        n = len(self.record)
        i = 0
        while i < n:
            dtype = self.schema[i]
            if dtype == 'int':
                cur_col = self.record[i]
                result_record.extend(struct.pack('i',4))
                result_record.extend(struct.pack('i',int(cur_col)))
            elif dtype == 'float':
                cur_col = self.record[i]
                result_record.extend(struct.pack('i'),len(struct.pack("<f",float(cur_col))))
                result_record.extend(struct.pack('f',float(cur_col)))
            elif dtype == 'str':
                cur_col = self.record[i]
                result_record.extend(struct.pack('i',len(cur_col))) #does the string size equal to the bytes size?
                result_record.extend(struct.pack('{}s'.format(len(cur_col)),cur_col.encode('utf-8')))
            else:
                raise('dtype {} is not supported by the enconding algorithm',dtype)

            i+=1
        print("record encoded")
        print(result_record)
        return result_record
    
    def set_maxid(self,id:int):
        self.record = (id,*self.record)


    def decode(self, record:bytes) -> tuple:
        # Need to work on correclty identifying how much of the bytes that i am reading represent a row. 
        # I should make sure I read the entire page. 
        # identify end of records in the page. 
        pass        

class DBPage:
    def __init__(self,header:PageHeader = None,records:List[PageRecord] = None):
        self.static_header_format = struct.Struct("<iiiii")
        if header is None:
            start_offset = self.static_header_format.size
            end_offset = PAGE_SIZE
            self.header = PageHeader(0,0,start_offset,end_offset,[],self.static_header_format)
            self.records = []
        else:
            self.header = header
            self.records = records
        


    def encode(self) -> bytes:
        page = bytearray(PAGE_SIZE)
        if self.records is None:
            encoded_header = self.header.encode()
            header_size = len(encoded_header)
            page[0:header_size] = encoded_header
        else:
            encoded_header = self.header.encode()
            header_size = len(encoded_header)
            print("page header encoded")
            print(encoded_header)
            page[0:header_size]=encoded_header
            
            for record,pointer in zip(self.records,self.header.record_pointers):
                record_start_offset = pointer[0] - pointer[1]
                record_end_offset = pointer[0]
                print("pointer[0]: {}, pointer[1]: {}".format(pointer[0],pointer[1]))
                print("record_end_offset: {}".format(record_end_offset))
                page[record_start_offset:record_end_offset] = record.encode()
    
        return page
    
    def decode(self,page_bytes:bytes):
        #use the record pointers to start reading each row
        print(page_bytes)
        start_offset = int.from_bytes(page_bytes[12:16],"little")
        print("start_offset: {}".format(start_offset))
        page_header = page_bytes[0:start_offset]
        print("page_header : {}".format(page_header))
        self.header.decode(page_header)
        

    def add_empty_page(self,offset) -> int:
        page = bytearray(PAGE_SIZE)
        page_header_size = self.fmt.page_header.size
        page_header = self.fmt.page_header.pack(0,0,page_header_size,page_header_size,len(page))
        page.extend(page_header)
        self.db.seek(offset)
        last_offset = self.db.write(page)
        return last_offset

    def add_record(self,record:PageRecord):
        id = self.header.max_id+1
        record.set_maxid(id)
        
        self.records.append(record)
        record_bytes = record.encode()
        record_size = len(record_bytes)
        self.header.record_pointers.append((self.header.end_offset,record_size))
        pointer_size = 8
        start_offset = self.header.start_offset + pointer_size
        end_offset = self.header.end_offset - record_size
        self.header.update(max_id=id,start_offset=start_offset,end_offset=end_offset)
        print(record.record)
    
class PageLayout:
    record:bytearray



if __name__ == '__main__':
    db_io = DataBaseIO("/home/ubuntu/Home/Downloads/ml-20m/movies.csv",
                     "/home/ubuntu/Home/Downloads/ml-20m/movies.db",
                     'mydb','movies',('int','str','str')
                     )
    # i = 15
    # while i > 0:
    #     i=i-1
    #     record = db_io.get_next_tuple()
    #     db_io.page.add_record(PageRecord(record,('int','int','str','str')))
    
    # db_io.persist()
    db_io.read()