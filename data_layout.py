import csv
from functools import reduce
from io import BytesIO
import os
import struct
from typing import List

PAGE_SIZE = 4096
DB_HEADER_SIZE = 400

class DataBase:
    def __init__(self,db_path,db_name,table_name,schema):
        self.header = DBHeader(db_name,table_name,schema)
        self.pages:list[DBPage] = list()
        self.db_path = db_path
        self.db = self.db_init()

    def persist(self) -> bool:
        """
            This function should encode records and header and persist them to disk,
            currently this function is used as a utility to create the custom database file format on disk.
        """
        try:
            db_header, db_pages = self.encode()
            db_layout = bytearray()
            db_layout.extend(db_header)
            db_layout.extend(db_pages)
            self.db.seek(0)
            self.db.write(db_layout)
            self.flush(0)
        except:
            return False
        return True

    def read(self):
        """
            Read one page from the db and decodes it. Every time this function is called within the same execution instance
            it would read the next page into memory.
        """
        try:
            #print("current memory position {}".format(self.db.tell()))
            page_bytes = self.db.read(PAGE_SIZE)
            if len(page_bytes) == 0:
                return False
            self.pages.append(DBPage())
            self.last_page().decode(page_bytes,self.header.schema)
        except:
            return False
        return True
    
    def reset_page_read(self):
        self.db.seek(self.header.start_offset)
        
    
    def write(self):
        """
            Write all the new pages to disk, it set the starting offset based on the current end offser which
            is updated every time we add a new page in the add_record function, the page size and the 
            number of pages that will be writen.
        """
        start_page_offset = self.header.end_offset - PAGE_SIZE * len(self.pages)
        _, db_pages = self.encode()
        print("page to write: {}".format(db_pages))
        self.db.seek(start_page_offset)
        self.db.write(db_pages)

    def add_record(self,record:tuple):
        page = self.last_page()
        record = PageRecord(record)
        if not self.has_free_space(page,record=record):
            self.write()
            self.pages.pop()
            self.pages.append(DBPage())
            page = self.last_page()
            self.header.end_offset = self.header.end_offset + PAGE_SIZE
            
        page.add_record(record,self.header.schema)
        self.header.table_size = self.header.byte_format.size + PAGE_SIZE * len(self.pages)
        

    def combine_pages(self,x,y,schema) -> bytearray:
   
        if isinstance(x,DBPage) and isinstance(y,DBPage):
            x.encode(schema).extend(y.encode(schema)) 
            return x
        elif isinstance(x,DBPage) and isinstance(y,bytearray):
            x.encode(schema).extend(y)
            return x
        elif isinstance(x,bytearray) and isinstance(y,DBPage):
            x.extend(y.encode(schema))
            return x
        else:
            x.extend(y)
            return x

    def encode(self) -> (bytearray, bytearray):
         print("number of pages: {}".format(len(self.pages)))
         db_pages = reduce(lambda x,y:self.combine_pages(x,y,self.header.schema),self.pages,bytearray())
         db_header = self.header.encode()
        #  db = bytearray()
        #  db.extend(db_header)
        #  db.extend(db_pages)
         return db_header,db_pages

    def decode(self,header_bytes,page_bytes) -> tuple:
        self.header.decode(header_bytes)
        iter_pager = iter(self.pages)
        n_pages = len(self.pages)
        i = 0
        while i < n_pages:
            page = next(iter_pager)
            page.decode(page_bytes[PAGE_SIZE*i:PAGE_SIZE*(i+1)],self.header.schema)
            i+=1
            
        return (self.header,self.pages)

       
    def flush(self,offset):
        ## todo: change this logic, maybe we may want to send the bytes to write to disk as input parameters instead of reference from the class
        self.db.seek(offset)
        with open(self.db_path,"w+b") as f:
            f.write(self.db.read())

    def has_free_space(self,page:"DBPage",record:"PageRecord") -> bool:
        return page.header.end_offset - len(record.encode(self.header.schema)) > page.header.start_offset + 8        

    def last_page(self) -> "DBPage":
        """
        Get last inserted page in the pages list, if it does not exists load the page from file
        """
        n = len(self.pages)
       # print(n)
        if n > 0:
           last = n-1
        else:
            self.db.seek( self.header.end_offset - PAGE_SIZE if self.header.end_offset > 0 else 0)
            page_bytes = self.db.read(PAGE_SIZE)
            page = DBPage()
            page.decode(page_bytes,self.header.schema)
            self.pages.append(page)
            last = 0
        
        return self.pages[last]


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
           self.pages.append(DBPage())
           self.header.end_offset = self.header.end_offset + PAGE_SIZE
        return db

        
    
    def __del__(self):
        if self.db is not None:
            self.db.close()    

class DBHeader:

    def __init__(self,db_name:str,table_name:str,schema:tuple,table_size=0,end_offset=0):
        self.db_name = db_name
        self.table_name = table_name
        self.schema =  schema #this adds an internal id of type int to the schema
        self.table_size = table_size
        self.byte_format = struct.Struct("<64s64s256siiq")
        self.start_offset = self.byte_format.size # start offset of the first page created, this should help to read records
        self.end_offset = self.byte_format.size # end offset of the last page created, this should help to append new pages when the existing ones are full.
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
        self.end_offset = int.from_bytes(header[392:DB_HEADER_SIZE],'little')
        print("db end offset 8 bytes {}".format(int.from_bytes(header[392:400],'little')))
        print("db end offset 4 bytes {}".format(int.from_bytes(header[392:396],'little')))
    

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
        for i in range(20,record_pointers_size+20,8):
            first_value = int.from_bytes(header[i:i+4],'little')
            second_value = int.from_bytes(header[i+4:i+8],'little')
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
    #TODO: do i really need a page record class? maybe this can be contained in the page class as a list of tuples
    def __init__(self,record:tuple):
        self.record = record

    def encode(self,schema:tuple) -> bytearray:
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
            dtype = schema[i]
            if dtype == 'int':
                cur_col = self.record[i]
                result_record.extend(struct.pack('i',int(cur_col)))
            elif dtype == 'float':
                cur_col = self.record[i]
                result_record.extend(struct.pack('f',float(cur_col)))
            elif dtype == 'str':
                cur_col = self.record[i]
                col_size = len(cur_col.encode('utf-8'))
                #print("encode col_size: {}".format(col_size))
                #print("encode col_size bytes 1: {}".format(struct.pack('<i',col_size)))
                result_record.extend(col_size.to_bytes(1,byteorder='little'))
                result_record.extend(struct.pack('{}s'.format(col_size),cur_col.encode('utf-8')))
            else:
                raise('dtype {} is not supported by the enconding algorithm',dtype)

            i+=1
        return result_record
    
    def set_internal_id(self,id:int):
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

    def encode(self,schema:tuple) -> bytearray:
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
            page[0:header_size]=encoded_header
            
            for record,pointer in zip(self.records,self.header.record_pointers):
                record_start_offset = pointer[0] - pointer[1]
                record_end_offset = pointer[0]
                page[record_start_offset:record_end_offset] = record.encode(schema)
    
        return page
    
    def decode(self,page_bytes:bytes,schema:tuple):
        """
            This function takes a page in byte format as input and parse it to human redeable format.
            It uses the record pointers in the header of the page to reading each row.
        """
        start_offset = int.from_bytes(page_bytes[12:16],"little")
        page_header = page_bytes[0:start_offset]
        self.header.decode(page_header)
        for pointer in self.header.record_pointers:
            start_offset = pointer[0] - pointer[1]
            record_size = pointer[1]
            #print("record_size: {}, start_offset: {}".format(record_size,start_offset))
            record_bytes = page_bytes[start_offset:start_offset+record_size]
            #print("record: {}".format(record_bytes))
            record = self.decode_record(record_bytes,schema)
            self.records.append(PageRecord(record))
    
    
    def decode_record(self, record:bytes,schema:tuple) -> tuple:
        """
            First four bytes represent the column size.
            The next bytes from the end of the column size to the column size represent the content of the current column.
            We assume record columns has the same order as the schema so we use it to parse the types.
        """
        total_columns = len(schema)
        i = 0
        decode_record = []
        start_index =  i
        while i < total_columns:
            dtype =  schema[i]            
            
            if dtype == 'int':
                end_index = start_index+4
                col_content = record[start_index:end_index]
                value = int.from_bytes(col_content,'little')
                decode_record.append(value)
                start_index = end_index
            elif dtype == 'float':
                end_index = start_index+4
                col_content = record[start_index:end_index]
                value = struct.unpack('f',col_content)[0]
                decode_record.append(value)
                start_index = end_index
            elif dtype == 'str':
                end_index = start_index+1
                col_size = int.from_bytes(record[start_index:end_index],'little')
                col_content = record[end_index:end_index+col_size]
                value = col_content.decode('utf8')
                decode_record.append(value)
                start_index = end_index+col_size
            else:
                raise('dtype {} is not supported by the enconding algorithm',dtype)
            i+=1
            
        return tuple(decode_record)

    def add_record(self,record:PageRecord,schema:tuple):
        """
            This function adds the row into the list of existing rows in the page. 
            It generates an internal id that may be used to filter pages later and also calculate the record pointer that enables
            transversing all the records of the page.
            
        """
        id = self.header.max_id+1
        #record.set_internal_id(id)
        
        self.records.append(record)
        record_bytes = record.encode(schema)
        record_size = len(record_bytes)
        pointer_size = 8
        start_offset = self.header.start_offset + pointer_size
        end_offset = self.header.end_offset - record_size
        self.header.record_pointers.append((self.header.end_offset,record_size))
        self.header.update(max_id=id,start_offset=start_offset,end_offset=end_offset)


def get_next_tuple(reader) -> tuple:
        try:
            return tuple(next(reader))
        except:
            return ()
        
if __name__ == '__main__':
    csv_f = open("/home/ubuntu/Home/Downloads/ml-20m/movies.csv")
    reader = csv.reader(csv_f)
    #read header
    next(reader)
    db_io = DataBase(
                     "/home/ubuntu/Home/Downloads/ml-20m/movies_slotted_2.db",
                     'mydb','movies',('int','str','str'))


    # csv_f = open("/home/ubuntu/Home/Downloads/ml-20m/ratings.csv")
    # reader = csv.reader(csv_f)
    # #read header
    # next(reader)
    # db_io = DataBase(
    #                  "/home/ubuntu/Home/Downloads/ml-20m/ratings_slotted.db",
    #                  'mydb','ratings',('int','int','float','int'))

    i = 120
    while True:
        #i=i-1
        record = get_next_tuple(reader)
        if record == ():
            break
        db_io.add_record(record)


    # pages = db_io.decode(*db_io.encode())[1]
    # records_list = [p.records  for p in pages]
    # records = [ record 
    #            for record_list in records_list
    #            for record in record_list]
    # print([record.record for record in records])

    #header,records = db_io.decode(*db_io.encode())

    
    # while db_io.read():
    #     records = db_io.last_page().records
    #     print([record.record for record in records])
