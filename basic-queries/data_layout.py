import csv
import os
import struct
from typing import List

class DBHeader:

    def __init__(self,db_name:str,table_name:str,schema:tuple,table_size=0,start_offset=0,end_offset=0):
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.table_size = table_size
        self.start_offset = start_offset
        self.end_offset = end_offset
        self.byte_format = struct.Struct("<64s64s330siii")
        #maybe export the db_header to here.
    
    def encode(self) -> bytes:
        db_name = self.__get_db_name()
        table_name = self.__get_table_name()
        schema = self.__get_schema()
        start_offset = self.__get_start_offset()
        end_offset = self.__get_end_offset()
        table_size = self.__get_table_size()
        result = self.byte_format.pack(db_name,table_name,schema,table_size,start_offset,end_offset)
        return result
    
    def decode(self,header:bytearray) -> "DBHeader":
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
        self.size = self.static_bytes_format.size
    
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
        
       
        result.extend(record_pointers)
        return bytes(result)

    def decode(self, header:bytearray) -> "PageHeader":
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
    

class PageRecord:
    def __init__(self,record:tuple):
        self.record = record

    def encode(self,schema:tuple) -> bytes:
        result_record = bytearray()
        n = len(self.record)
        i = 0
        while i < n:
            dtype = schema[i]
            if dtype == 'int':
                cur_col = record[i]
                result_record.extend(struct.pack('i'),len(int(cur_col).to_bytes(4)))
                result_record.extend(struct.pack('i',cur_col))
            elif dtype == 'float':
                cur_col = record[i]
                result_record.extend(struct.pack('i'),len(struct.pack("<f",cur_col)))
                result_record.extend(struct.pack('f',record[i]))
            elif dtype == 'str':
                cur_col = record[i]
                result_record.extend(struct.pack('i',len(cur_col))) #does the string size equal to the bytes size?
                result_record.extend(struct.pack('{}s'.format(len(cur_col)),cur_col.encode('utf-8')))
            else:
                raise('dtype {} is not supported by the enconding algorithm',dtype)

            i+=1

        return result_record
    
    def set_maxid(self,id:int):
        self.record = (id) + record


    def decode_record(self, record:bytes) -> tuple:
        # Need to work on correclty identifying how much of the bytes that i am reading represent a row. 
        # I should make sure I read the entire page. 
        # identify end of records in the page. 
        pass        

class DBPage:
    PAGE_SIZE = 4096
    def __init__(self,header:PageHeader = None,records:List[PageRecord] = None):
        self.static_header_format = struct.Struct("<iiiii")
        self.records = records
        if header is None:
            self.header = PageHeader(0,0,self.static_header_format.size,self.PAGE_SIZE,())
        else:
            self.header = header
        


    def encode(self) -> bytes:
        page = bytearray(self.PAGE_SIZE)
        if self.records is None:
            page.extend(self.header.encode())
        else:
            encoded_header = self.header.encode()
            page.extend(encoded_header)
            records_bytes = bytearray()
            for record in self.records:
                records_bytes.extend(record)
            page.extend(record.encode())
        return self.content
    
    def add_empty_page(self,offset) -> int:
        page = bytearray(self.PAGE_SIZE)
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
        record_size = len(record.encode())
        self.header.record_pointers.append((self.header.end_offset,record_size))
        start_offset = self.header.start_offset + len(self.header.encode_record_pointers())
        end_offset = self.header.end_offset - len(record.encode())
        self.header.update(max_id=id,start_offset=start_offset,end_offset=end_offset)
    

class DataBaseIO:
    
    def __init__(self,input_path,db_path,db_name,table_name,schema):
        self.header = DBHeader(db_name,table_name,schema)
        self.page = DBPage()
        self.db_path = db_path
        self.db = self.db_init()
        self.csv_f = open(input_path)
        self.reader = csv.reader(self.f)
        #read header
        next(self.reader)

    def get_next_tuple(self) -> tuple:
        try:
            return tuple(next(self.reader))
        except:
            return ()

    def persist(self) -> bool:
        db_page = self.page.encode()
        #should encode the records and persist them to disk
        #should update the header according with the new records persisted.
        pass

        
    def update_db_header(self,header:DBHeader):
        pass


    def flush(self):
        self.db.flush()

    def db_init(self):
        if os.path.isfile(self.db_path):
           db = open(self.db_path,mode='w+b')
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
        self.f.close()
        self.db.close()



class PageLayout:
    record:bytearray



if __name__ == '__main__':
    db_io = DataBaseIO("/home/ubuntu/Home/Downloads/ml-20m/movies.csv",
                     "/home/ubuntu/Home/Downloads/ml-20m/movies.db",
                     'mydb','movies',('int','str','str')
                     )
    i = 1
    while i > 0:
        i=i-1
        record = db_io.get_next_tuple()
        #print(record)
        db_io.page.add_record(PageRecord(record))