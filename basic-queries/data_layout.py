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
        """
            This function should encode records and header and persist them to disk,
            currently this function is used as a utility to create the custom database file format on disk.
        """
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

    def read(self):
        """
            Read one page from the db and decodes it. Every time this function is called within the same execution instance
            it would read the next page into memory.
        """
        page_bytes = self.db.read(PAGE_SIZE)
        self.page.decode(page_bytes,self.header.schema)

    def add_record(self,record:"PageRecord"):
        self.page.add_record(record)
        self.header.table_size = self.header.byte_format.size + PAGE_SIZE # this should be the sum of all existing pages
        self.header.end_offset = self.header.end_offset + PAGE_SIZE # this should be the sum of all existing pages


    def encode(self) -> (bytearray, bytearray):
         db_page = self.page.encode()
         db_header = self.header.encode()
         return db_header,db_page

    def decode(self,header_bytes,page_bytes) -> tuple:
        self.header.decode(header_bytes)
        self.page.decode(page_bytes,self.header.schema)
        return (self.header,self.page.records)

       
    def flush(self):
        self.db.flush()

    def db_init(self):
        """
            Initiallize the database by reading the file if it already exists and only decoding the header
            or creating a new database structure.
        """
        if os.path.isfile(self.db_path):
           db = open(self.db_path,mode='r+b')
           db_header_bytes = db.read(DB_HEADER_SIZE)
           self.header.decode(db_header_bytes)
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

    def __init__(self,db_name:str,table_name:str,schema:tuple,table_size=0,end_offset=0):
        self.db_name = db_name
        self.table_name = table_name
        self.schema = ('int',*schema) #this adds an internal id of type int to the schema
        self.table_size = table_size
        self.byte_format = struct.Struct("<64s64s256siii")
        self.start_offset = self.byte_format.size # start offset of the first page created, this should help to read records
        self.end_offset = end_offset # end offset of the last page created, this should help to append new pages when the existing ones are full.
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
        """
            This function should decode the header and set the value on the current instance of the object.
            Attributes starting bytes position:
            - byte 0 database name
            - byte 64 table name
            - byte 128 schema
            - byte 384 table size
            - byte 388 start offset
            - byte 392 end offset
        """
        self.db_name = header[0:64].decode('utf-8')
        self.table_name = header[64:128].decode('utf-8')
        #self.schema = tuple(header[128:384].decode('utf-8').split(','))
        self.table_size = int.from_bytes(header[384:388],'little')
        self.start_offset = int.from_bytes(header[388:392],'little')
        self.end_offset = int.from_bytes(header[392:396],'little')
        print(self.schema)
    

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
        self.record_pointers = record_pointers # tuple representing end offset and row size, this should help transversing the records and in the future probably updating records without strictly "closing the gap" while the operation happens.
        self.static_bytes_format = static_bytes_format
        self.size = len(record_pointers)# number of rows stored
        # should we include remaining free space variable?
    
    def encode(self) -> bytes:
        min_id = self.min_id
        max_id = self.max_id
        start_offset = self.start_offset
        end_offset = self.end_offset

        record_pointers = self.encode_record_pointers()

        size = len(self.record_pointers)
        static_header = self.static_bytes_format.pack(min_id,max_id,size,start_offset,end_offset)

        result = bytearray()
        result.extend(static_header)
        
       
        result.extend(bytes(record_pointers))
        return result

    def decode(self, header:bytearray):
        """
            decodes page header from bytes back to PageHeader object. Following are byte start position for each attribute
            - byte 0 min id
            - byte 4 max id
            - byte 8 size
            - byte 12 start offset
            - byte 16 end offset
            - byte 20 of page starting point of record pointers
        """
        
        min_id = int.from_bytes(header[0:4],'little')
        max_id = int.from_bytes(header[4:8],'little')
        size = int.from_bytes(header[8:12],'little')
        start_offset = int.from_bytes(header[12:16],'little')
        end_offset = int.from_bytes(header[16:20],'little')
        self.min_id = min_id
        self.max_id = max_id
        self.size = size
        self.start_offset = start_offset
        self.end_offset = end_offset
        record_pointers_size = len(header[20:])
        #print("record_pointers_size: {}".format(record_pointers_size))
        for i in range(20,record_pointers_size+20,8):
            first_value = int.from_bytes(header[i:i+4],'little')
            second_value = int.from_bytes(header[i+4:i+8],'little')
            #print("record_pointers decoded: {} ".format((first_value,second_value)))
            self.record_pointers.append((first_value,second_value))

    
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
        """
            Encodes the record attribute into bytes. 
            For each column we match the type and encode the content lenght 
            and the content itself one next to the other, if the column is size 0 we assume it is null, 
            this implies that an empty str may be considered null.

            In order to remove complexity from parsing we assume the recors columns are in the same order as the schema.
        """
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
        # print("record encoded : {}".format(result_record))
        return result_record
    
    def set_maxid(self,id:int):
        self.record = (id,*self.record)


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
        """
            This function uses the DBPage object content(header and records) and convert them into byte format,
            so it can eventually be used to persist on disk. It returns the page in bytes.
        """
        page = bytearray(PAGE_SIZE)
        if self.records is None:
            encoded_header = self.header.encode()
            header_size = len(encoded_header)
            page[0:header_size] = encoded_header
        else:
            encoded_header = self.header.encode()
            header_size = len(encoded_header)
            # print("page header encoded")
            # print(encoded_header)
            page[0:header_size]=encoded_header
            
            for record,pointer in zip(self.records,self.header.record_pointers):
                record_start_offset = pointer[0] - pointer[1]
                record_end_offset = pointer[0]
                # print("pointer[0]: {}, pointer[1]: {}".format(pointer[0],pointer[1]))
                # print("record_end_offset: {}".format(record_end_offset))
                page[record_start_offset:record_end_offset] = record.encode()
    
        return page
    
    def decode(self,page_bytes:bytes,schema:tuple):
        """
            This function takes a page in byte format as input and parse it to human redeable format.
            It uses the record pointers in the header of the page to reading each row.
        """
        start_offset = int.from_bytes(page_bytes[12:16],"little")
        page_header = page_bytes[0:start_offset]
       # print("page_header : {}".format(page_header))
        self.header.decode(page_header)
        for pointer in self.header.record_pointers:
            start_offset = pointer[0] - pointer[1]
            record_size = pointer[1]
            record_bytes = page_bytes[start_offset:start_offset+record_size]
            record = self.decode_record(record_bytes,schema)
            self.records.append(PageRecord(record,schema))
        #print([record.record for record in self.records])
    
    
    def decode_record(self, record:bytes,schema:tuple) -> tuple:
        """
            First four bytes represent the column size.
            The next bytes from the end of the column size to the column size represent the content of the current column.
            We assume record columns has the same order as the schema so we use it to parse the types.
        """
        total_columns = len(schema)
        i = 0
        decode_record = []
        #print("record bytes: {}".format(record))
        #print("schema : {}".format(schema))
        start_index =  i * 4
        while i < total_columns:
            dtype =  schema[i]
            end_index = start_index+4
            col_size = int.from_bytes(record[start_index:end_index],'little')
            col_content = record[end_index:end_index+col_size]
            
            if dtype == 'int':
                value = int.from_bytes(col_content,'little')
                decode_record.append(value)
                #print("col_size:col_content {}".format((col_size,value)))
            elif dtype == 'float':
                value = float.fromhex(col_content.hex)
                decode_record.append(value)
            elif dtype == 'str':
                value = col_content.decode('utf8')
                decode_record.append(value)
            else:
                raise('dtype {} is not supported by the enconding algorithm',dtype)
            i+=1
            start_index = end_index+col_size
        return tuple(decode_record)

    def add_empty_page(self,offset) -> int:
        """
            Creaste a new empty page with only the header
        """
        pass

    def add_record(self,record:PageRecord):
        """
            This function adds the row into the list of existing rows in the page. 
            It generates an internal id that may be used to filter pages later and also calculate the record pointer that enables
            transversing all the records of the page.
            
        """
        id = self.header.max_id+1
        record.set_maxid(id)
        
        self.records.append(record)
        record_bytes = record.encode()
        record_size = len(record_bytes)
        pointer_size = 8
        start_offset = self.header.start_offset + pointer_size
        end_offset = self.header.end_offset - record_size
        self.header.record_pointers.append((self.header.end_offset,record_size))
        self.header.update(max_id=id,start_offset=start_offset,end_offset=end_offset)
        #print(record.record)


if __name__ == '__main__':
    db_io = DataBaseIO("/home/ubuntu/Home/Downloads/ml-20m/movies.csv",
                     "/home/ubuntu/Home/Downloads/ml-20m/movies.db",
                     'mydb','movies',('int','str','str'))
    i = 15
    while i > 0:
        i=i-1
        record = db_io.get_next_tuple()
        db_io.add_record(PageRecord(record,('int','int','str','str')))
    
    header,records = db_io.decode(*db_io.encode())
    print([record.record for record in records])
    #db_io.persist()
    #db_io.read()