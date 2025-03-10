from data_layout import DataBase
from collections import defaultdict

class MergeJoin(object):
    def __init__(self,left_node,right_node,left_key,right_key):
        self.left_node = left_node
        self.right_node = right_node
        self.left_key = left_key
        self.right_key = right_key
        self.rows_buffer = []
        self.leading_value = None
        self.non_leading_value = None
        self.previous_leading_v = None
        self.buff_idx = 0 

    def next(self) -> tuple:
        """
            To implement this join strategy we assume both datasets come sorted then we chose one to be the leading
            dataset(left node) and the other one would be the non leading dataset(right node), we iterate over each 
            element using a technique similar to two pointers to find equal keys and return the joined dataset.
        """
        if self.leading_value is None and self.left_node.has_next():
            self.leading_value = self.left_node.next()


        while True:
            if self.non_leading_value is None and self.right_node.has_next():
                self.non_leading_value = self.right_node.next()

            if self.leading_value is None and self.left_node.has_next():
                self.leading_value = self.left_node.next()
                # Validate if the current and previous leading values are different then we should clean the rows buffer
                if self.previous_leading_v is not None and self.left_key(self.previous_leading_v) != self.left_key(self.leading_value):
                    self.rows_buffer.clear()
                    self.buff_idx = 0
            
        
            if self.leading_value is None or self.non_leading_value is None:
                return None
            # If previous and leading values are equal then we should join the leading value with the rows_buffer 
            if self.previous_leading_v is not None and self.left_key(self.previous_leading_v) == self.left_key(self.leading_value):
                #print(f"{self.previous_leading_v} - {self.leading_value}")
                if len(self.rows_buffer) > self.buff_idx:
                        result = (*self.leading_value,*self.rows_buffer[self.buff_idx])
                        self.buff_idx += 1
                        return result
                else:
                    self.buff_idx = 0
                    self.previous_leading_v = self.leading_value
                    self.leading_value = None
                    continue
            
            if self.left_key(self.leading_value) == self.right_key(self.non_leading_value):
                #If values are matching we should append new non-leading records to the buffer
                self.rows_buffer.append(self.non_leading_value)
                result = (*self.leading_value,*self.non_leading_value)
                self.non_leading_value = None
            
                return result
            else:
                if self.left_key(self.leading_value) > self.right_key(self.non_leading_value):
                    self.non_leading_value = None
                else:
                    self.previous_leading_v = self.leading_value
                    self.leading_value  = None
                    


    def has_next(self) -> bool:
        return self.left_node.has_next() and self.right_node.has_next()
    
    def reset(self):
        self.left_node.child.reset()
        self.right_node.child.reset()
        self.leading_value = None
        self.non_leading_value = None

class HashJoin(object):
    def __init__(self,left_node,right_node,left_key,right_key):
        self.left_node = left_node
        self.right_node = right_node
        self.left_key = left_key
        self.right_key = right_key
        self.hash_table = defaultdict(list)
        self.left_list = []
        self.current_right_v = None

    def next(self) -> tuple:
        """
            This function create a hash table from the left node and use it to find the relation on the right node. 
            Apart from the left_node and right_node representing datasets, we also have left_key and right_key which 
            are lambda functions to get the keys that will be used to join the datasets. 
        """
        if len(self.hash_table) == 0:
            while self.left_node.has_next():
                left_v = self.left_node.next()
                left_k = self.left_key(left_v)
                self.hash_table[left_k].append(left_v)
        else:
            if self.left_list is not None and len(self.left_list) > 0:
                left_v = self.left_list.pop(0)
                result = (*left_v,*self.current_right_v)
                return result
            elif self.right_node.has_next():
                right_v = self.right_node.next()
                right_k = self.right_key(right_v)
                self.left_list = self.hash_table.get(right_k,None)
                if self.left_list == None:
                    return None
                self.current_right_v = right_v
                left_v = self.left_list.pop(0)
                print(self.left_list)
                return (*left_v,*right_v)
        
        return None
            
        

    def has_next(self) -> bool:
        return self.left_node.has_next() or (self.right_node.has_next() and len(self.hash_table) > 0) or len(self.left_list) > 0   


    def reset(self):
        self.left_node.child.reset()
        self.right_node.child.reset()
        self.hash_table = defaultdict(list)


class NestedLoopJoin(object):
    def __init__(self,left_node,right_node):
        self.left_node = left_node
        self.right_node = right_node
        self.buffer_join = []

    def next(self) -> tuple:
        if len(self.buffer_join) > 0:
            return self.buffer_join.pop(0)
        elif self.left_node.has_next():
            left_v =  self.left_node.next()
            while self.right_node.has_next():
                right_v = self.right_node.next()
                self.buffer_join.append((*left_v,*right_v))
            self.right_node.reset()
            if len(self.buffer_join) > 0: 
                return self.buffer_join.pop(0)
        return None

    def has_next(self) -> bool:
        return self.left_node.has_next() or len(self.buffer_join) > 0
    
    def reset(self):
        self.left_node.reset()
        self.right_node.reset()
        self.buffer_join = []



class FileScan(object):
    def __init__(self,path,db_name,table_name,schema):
        self.db = DataBase(path,db_name,table_name,schema)
        
    
    def next(self) -> tuple:
        if self.has_next():
            record = self.db.last_page().records.pop(0)
            return record.record
        return None

    def has_next(self) -> bool:

        if len(self.db.pages) == 0:
            self.load_next_page()

        if len(self.db.pages) > 0  and len(self.db.last_page().records) == 0:
            self.db.pages.pop()
            self.load_next_page()

        if len(self.db.pages) > 0 and len(self.db.last_page().records) > 0:
            #print("We have records to read")
            return True
        return False
            
    
    def load_next_page(self):
         #load page
        is_page_loaded = self.db.read()
        # if not is_page_loaded:
        #     print("not more pages available to load")
    
    def reset(self):
        self.db.reset_page_read()




class CSVFileStream(object):

    def __init__(self,path,chunk_size,separetor = ",",contain_header=True):
        self.file = open(path,buffering=chunk_size,mode = 'rt',encoding='utf-8',)
        self.contain_header = contain_header
        if contain_header:
            self.file.readline() # read the header
        self.chunk_size = chunk_size
        self.separetor = separetor

    
    def stream_file(self):
        i = 0
        lines = []
        for line in self.file:
            current_row = line.split(self.separetor)
            lines.append(tuple(current_row))
            i+=1
            if i > self.chunk_size:
                break        
        return lines
    
    def __del__(self):
        print('Closing file resources')
        self.file.close()

    def reset(self):
        self.file.seek(0)
        if self.contain_header:
            self.file.readline() 
        

class CSVFileScan(object):

    def __init__(self,path,chunk_size):
        self.file = CSVFileStream(path,chunk_size)
        self.data = []

    def next(self):
        if len(self.data) == 0:
            self.data = self.file.stream_file()
            if len(self.data) == 0:
                return None
        
        record = self.data.pop(0)
        return record

    def has_next(self):
        if len(self.data) == 0:
            self.data = self.file.stream_file()
        return len(self.data) > 0

class MemoryScan(object):
    """
    Yield all records from the given "table" in memory.

    This is really just for testing... in the future our scan nodes
    will read from disk.
    """
    def __init__(self, table):
        self.table = table
        self.idx = 0

    def next(self):
        if self.idx >= len(self.table):
            return None

        x = self.table[self.idx]
        self.idx += 1
        return x

    def has_next(self):
        if self.idx < len(self.table):
            return True
        else:
            False
    
    def reset(self):
        self.idx = 0


class Projection(object):
    """
    Map the child records using the given map function, e.g. to return a subset
    of the fields.
    """
    def __init__(self, proj):
        self.proj = proj

    def next(self):
        if self.has_next():
            current_tuple = self.child.next()
            if current_tuple is not None:
                return self.proj(current_tuple)
        else:
            return None
    
    def has_next(self):
        return self.child.has_next()
    
    def reset(self):
        self.child.reset()

class Selection(object):
    """
    Filter the child records using the given predicate function.

    Yes it's confusing to call this "selection" as it's unrelated to SELECT in
    SQL, and is more like the WHERE clause. We keep the naming to be consistent
    with the literature.
    """
    def __init__(self, predicate):
        self.predicate = predicate

    def next(self):
        if self.has_next():
            current_tuple = self.child.next()
            if self.predicate(current_tuple):
                return current_tuple
        else:        
            return None
    
    def has_next(self):
        return self.child.has_next()


class Limit(object):
    """
    Return only as many as the limit, then stop. If offset parameter is provided the function will 
    skip the number of rows provided as its value and start the limiting counting from the offset number.
    """
    def __init__(self, n, offset = 0):
        self.n = n
        self.offset = offset
        self.fetched = 0 - offset

    def next(self):
        if self.has_next():
            cur_element = self.child.next()
            if self.n > self.fetched:
                self.fetched += 1 
                if self.fetched > 0:
                    return cur_element
        return None
            


    def has_next(self):
        return self.child.has_next() and self.fetched < self.n
    
    def reset(self):
        self.fetched = 0 - self.offset

class Sort(object):
    """
    Sort based on the given key function
    """
    def __init__(self, key, desc=False):
        self.key = key
        self.desc = desc
        self.sorted_elements = []
        self.idx = 0

    def next(self):
        if len(self.sorted_elements) == 0:
            while True:
                element = self.child.next()
                if element is None:
                    break
                self.sorted_elements.append(element)
            #print(f"unsorted_elements: {self.sorted_elements}")
            
            self.sorted_elements = list(sorted(self.sorted_elements,key=self.key,reverse=self.desc))
            #self.buble_sort()
            #print(f"sorted_elements: {self.sorted_elements}")
            current_element = self.sorted_elements[self.idx]
            self.idx+=1
            return current_element
        else:
            current_element = self.sorted_elements[self.idx]
            self.idx+=1
            return current_element


    def buble_sort(self):
        i = 0
        n = len(self.sorted_elements)
        while i < n:
            j = 0
            while j < n:
                if self.desc:
                    if self.key(self.sorted_elements[i]) > self.key(self.sorted_elements[j]):
                        self.swap_places(i,j)
                else:    
                    if self.sorted_elements[i] < self.sorted_elements[j]:
                        self.swap_places(i,j)
                j+=1
            i+=1


    def swap_places(self,i,j):
        temp = self.sorted_elements[i]
        self.sorted_elements[i] = self.sorted_elements[j]
        self.sorted_elements[j] = temp


    def has_next(self):
        return self.child.has_next() or self.idx < len(self.sorted_elements)
    
    def reset(self):
        self.idx = 0




class Aggregation(object):
    def __init__(self,group_col,col,func_name):
        self.group_col = group_col
        self.col = col
        self.func_name = func_name.lower()
        self.acc = dict()
        self.result_keys = list()
        self.idx = 0 

    def sum_func(self,current_group_col,current_acc_val,current_tuple):
            current_val_col = self.col(current_tuple)
            self.acc.update({current_group_col:current_acc_val+current_val_col})

    def count_func(self,current_group_col,current_acc_val,current_tuple):
        current_val_col = 0 if self.col(current_tuple) is None else 1
        self.acc.update({current_group_col:current_acc_val+current_val_col})

    def avg_func(self,current_group_col, current_acc_val, current_tuple):
        current_val_col = self.col(current_tuple)
        count_current_val_col = 0 if self.col(current_tuple) is None else 1
        count_key = f"count_{current_group_col}"
        sum_key = f"sum_{current_group_col}"
        self.acc.update({count_key:self.acc.get(count_key,0)+count_current_val_col})
        self.acc.update({sum_key:self.acc.get(sum_key,0)+current_val_col})
        self.acc.update({current_group_col: round(self.acc.get(sum_key,0)/self.acc.get(count_key,1),2)})



    def next(self):
        if len(self.result_keys) == 0:
            while True:
                current_tuple = self.child.next()
                if current_tuple is None:
                    break
                
                #print(current_tuple)
                current_group_col  = self.group_col(current_tuple)
                current_acc_val = self.acc.get(current_group_col,0)
                if current_group_col not in self.acc:
                    self.result_keys.append(current_group_col)
                if self.func_name == 'sum':
                    self.sum_func(current_group_col,current_acc_val,current_tuple)
                elif self.func_name == 'count':
                    self.count_func(current_group_col,current_acc_val,current_tuple)
                elif self.func_name == 'avg':
                    self.avg_func(current_group_col,current_acc_val,current_tuple)
                else:
                    raise NotImplementedError(f"the function {self.func_name} has not been implemented yet or does not exsits")
            if len(self.result_keys) == 0:
                return None
            key = self.result_keys.pop(0)
            return (key,self.acc.get(key))
        else:
            key = self.result_keys.pop(0)
            return (key,self.acc.get(key))

    def has_next(self):
        return self.child.has_next() or len(self.result_keys) > 0

    def reset(self):
        return self.child.reset()    

class Insert(object):

    def __init__(self,db:DataBase,records:list[tuple]):
        self.records = records
        self.db = db
        self.n = len(records)
    
    def next(self):
        self.db.add_record(self.records.pop(0))
    
    def has_next(self):
        if len(self.records) > 0:
            return True
        else:
            self.db.write()
            print("{} records inserted".format(self.n))
        return False

            

def Q(*nodes):
    """
    Construct a linked list of executor nodes from the given arguments,
    starting with a root node, and adding references to each child
    """
    ns = iter(nodes)
    parent = root = next(ns)
    for n in ns:
        parent.child = n
        parent = n
    return root


def run(q):
    """
    Run the given query to completion by calling `next` on the (presumed) root
    """
    while True:
        x = q.next()
        if x is None and q.has_next():
            continue
        elif x is None:
            break
        yield x


import os
import psutil
from datetime import datetime
import threading
import time
class ResourceMonitoring:
    def __init__(self):
        self.pid = os.getpid()
        self.monitoring = False
        self.measurements = []

    def memory_info(self):
        process = psutil.Process(self.pid)
        mem_info = process.memory_info()
        return {
            'timestamp': datetime.now(),
            'rss': self.format_bytes(mem_info.rss),
            'vms': self.format_bytes(mem_info.vms),
            'cpu': process.cpu_percent(interval=0.1)
        }

    def start_monitoring(self):
        self.monitoring = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop)
        self.monitoring_thread.daemon = True
        self.monitoring_thread.start()

    def _monitoring_loop(self):
        while self.monitoring:
            self.measurements.append(self.memory_info())
            time.sleep(0.5)
    
    def stop_monitoring(self):
        self.monitoring = False
        if hasattr(self, 'monitoring_thread'):
            self.monitoring_thread.join()
    
    def format_bytes(self, bytes_value):
        """Format bytes into human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes_value < 1024:
                return f"{bytes_value:.2f} {unit}"
            bytes_value /= 1024
        return f"{bytes_value:.2f} TB"    

class TestInMemoryDB:
    # Test it by running pytest
    birds = (
        ('amerob', 'American Robin', 0.077, True),
        ('baleag', 'Bald Eagle', 4.74, True),
        ('eursta', 'European Starling', 0.082, True),
        ('barswa', 'Barn Swallow', 0.019, True),
        ('ostric1', 'Ostrich', 104.0, False),
        ('emppen1', 'Emperor Penguin', 23.0, False),
        ('rufhum', 'Rufous Hummingbird', 0.0034, True),
        ('comrav', 'Common Raven', 1.2, True),
        ('wanalb', 'Wandering Albatross', 8.5, False),
        ('norcar', 'Northern Cardinal', 0.045, True)
    )
    schema = (
        ('id', str),
        ('name', str),
        ('weight', float),
        ('in_us', bool),
    )

    def test_projection_and_selection(self):
         # ids of non US birds
        result1 = tuple(run(Q(
            Projection(lambda x: (x[0],)),
            Selection(lambda x: not x[3]),
            MemoryScan(self.birds)
        )))
        print(result1)
        assert result1 == (
            ('ostric1',),
            ('emppen1',),
            ('wanalb',),
        )

    def test_limit_and_sorting(self):
        # id and weight of 3 heaviest birds
        result2 = tuple(run(Q(
            Projection(lambda x: (x[0], x[2])),
            Limit(3),
            Sort(lambda x: x[2], desc=True),
            MemoryScan(self.birds),
        )))
        assert result2 == (
            ('ostric1', 104.0),
            ('emppen1', 23.0),
            ('wanalb', 8.5),
        )

    def test_aggregation(self):
        # weight sum of birds comparison US and non US birds
        result3 = tuple(run(Q(
            Projection(lambda x: (x[0],x[1])),
            Aggregation(lambda x: x[3],lambda x: x[2],"sum"),
            MemoryScan(self.birds),
            )))
        print(result3)
        assert result3 == ((False, 135.5), (True, 6.1664))

        # count of US and non US birds
        result4 = tuple(run(Q(
            Projection(lambda x: (x[0],x[1])),
            Aggregation(lambda x: x[3],lambda x: x[2],"count"),
            MemoryScan(self.birds),
            )))
        print(result4)
        assert result4 == ((False, 3), (True, 7))


        # avg us birth weight
        result5 = tuple(run(Q(
            Projection(lambda x: (x[0],x[1])),
            Aggregation(lambda x: x[3],lambda x: x[2],"AVG"),
            MemoryScan(self.birds),
            )))
        print(result5)
        assert  result5 == ((False, 45.17), (True, 0.88))

    
    def test_selection_binary_operators(self):
        # ids of US low weight birds
        result6 = tuple(run(Q(
            Projection(lambda x: (x[0],)),
            Selection(lambda x: x[3] and x[2] <= 0.01),
            MemoryScan(self.birds)
        )))
        print(result6)
        assert result6 == (('rufhum',),)

    

    def test_limit_offset(self):
        # ids of birds from fifth offset
        result7 =  tuple(run(Q(
            Projection(lambda x: (x[0],)),
            Limit(3,5),
            MemoryScan(self.birds)
        )))
        print(result7)
        assert result7 == (('emppen1',), ('rufhum',), ('comrav',))





class TestCSVScanDB:
    #csv_table_path = "/Users/brayanjules/Downloads/ml-20m/movies.csv"
    csv_table_path = "/home/ubuntu/Home/Downloads/ml-20m/movies.csv"
    ""
    rm = ResourceMonitoring()
    def test_projection_limit(self):
        self.rm.start_monitoring()
        result = tuple(run(Q(
            Projection(lambda x: (x[0],x[1])),
            Limit(5),
            CSVFileScan(self.csv_table_path,10)
            )))
        self.rm.stop_monitoring()
        print("Resource Usage Results:")
        print([(ms['rss'],ms['vms'],ms['cpu']) for ms in self.rm.measurements])
        print("Data Results:")
        print(result)
        assert result == (('1', 'Toy Story (1995)'), ('2', 'Jumanji (1995)'), ('3', 'Grumpier Old Men (1995)'), ('4', 'Waiting to Exhale (1995)'), ('5', 'Father of the Bride Part II (1995)'))


    def test_count_performance_on_the_whole_dataset(self):
        
        self.rm.start_monitoring()
        result = tuple(run(Q(
            Projection(lambda x: (x[0], x[1])),
            Limit(5),
            Sort(lambda x: x[1],True),
            Aggregation(lambda x: x[2],lambda x: x[2],"count"),
            CSVFileScan(self.csv_table_path,1000)

        )))
        self.rm.stop_monitoring()
        print("Resource Usage Results:")
        print([(ms['rss'],ms['vms'],ms['cpu']) for ms in self.rm.measurements])
        print("Data Results:")
        print(result)
        assert result == (('Drama\n', 3294), ('Comedy\n', 1791), ('Documentary\n', 1550), ('Comedy|Drama\n', 964), ('Drama|Romance\n', 817))


class TestFileScanDB:
    db_movie_path = "/home/ubuntu/Home/Downloads/ml-20m/movies_slotted_2.db"
    db_rating_path = "/home/ubuntu/Home/Downloads/ml-20m/ratings_slotted.db"

    def test_full_scan(self):
        result = tuple(run(Q(
            Projection(lambda x: (x[0],x[1])),
            Limit(3),
            Sort(lambda x: x[0]),
            FileScan(self.db_movie_path,'mydb','movies',('int','str','str'))
            )))
        print("Data Results:")
        print(result)
        assert result == ((1, 'Toy Story (1995)'), (2, 'Jumanji (1995)'), (3, 'Grumpier Old Men (1995)'))

    def test_count_performance_on_the_whole_dataset(self):
            
            result = tuple(run(Q(
                Projection(lambda x: (x[0], x[1])),
                Limit(10),
                Sort(lambda x: x[1],True),
                Aggregation(lambda x: x[2], lambda x: x[2],"count"),
                FileScan(self.db_movie_path,'mydb','movies',('int','str','str'))
            )))
            expected = (('Drama', 4516), ('Comedy', 2278), ('Documentary', 1942), ('Comedy|Drama', 1263), ('Drama|Romance', 1074), ('Comedy|Romance', 754), ('Comedy|Drama|Romance', 605), ('Horror', 565), ('Crime|Drama', 448), ('Drama|Thriller', 426))
            
            print("Data Results:")
            print(result)
            assert result == expected

    def test_avg_movies_rating(self):
        result = tuple(run(Q(Limit(10),Aggregation(lambda x: x[1], lambda x: x[4],"avg"),MergeJoin(
            Q(Sort((lambda x: x[0])),Projection(lambda x: (x[0],x[1])),FileScan(self.db_movie_path,'mydb','movies',('int','str','str'))),
            Q(Sort((lambda x: x[1])),Projection(lambda x: (x[0],x[1],x[2])),FileScan(self.db_rating_path,'mydb','ratings',('int','int','float','int'))),
            lambda x: x[0],lambda x: x[1]))))
        expected = (('Toy Story (1995)', 3.92), ('Jumanji (1995)', 3.21), ('Grumpier Old Men (1995)', 3.15), ('Waiting to Exhale (1995)', 2.86), ('Father of the Bride Part II (1995)', 3.06), ('Heat (1995)', 3.83), ('Sabrina (1995)', 3.37), ('Tom and Huck (1995)', 3.14), ('Sudden Death (1995)', 3.0), ('GoldenEye (1995)', 3.43))
        print("Data Results:")
        print(result)
        assert result == expected
    


class TestInsertRecordDB:
    db_path = "/home/ubuntu/Home/Downloads/ml-20m/movies_slotted_2.db"
    
    def test_insert_one_record(self):
        record = [(10000000,'Openhaimer','documentary')]
        db = DataBase(self.db_path,"mydb","movies",('int','str','str'))
        run(Q(Insert(db,record)))

    def test_bulk_insert(self):
        record = list(((10000002,"Inside Out (2015)","animation:drama"),)*200)
        db = DataBase(self.db_path,"mydb","movies",('int','str','str'))
        run(Q(Insert(db,record)))

class TestNestedLoopJoin:
    left = (
        ('Poor things',1),
        ('Openhaimer',2),
        ('ToyStory',3)
    )
    right = (
        (4.2, 1),
        (5.0, 2),
        (4.9, 3),
        (3.0, 4)
    )
    def test_product(self):
        result = tuple(run(Q(
            NestedLoopJoin(
                Q(MemoryScan(self.left)),
                Q(MemoryScan(self.right))
            )
        )))

        expected = (
            ('Poor things',1, 4.2, 1),
            ('Poor things',1, 5.0, 2),
            ('Poor things',1, 4.9, 3),
            ('Poor things',1, 3.0, 4),
            
            ('Openhaimer',2, 4.2, 1),
            ('Openhaimer',2, 5.0, 2),
            ('Openhaimer',2, 4.9, 3),
            ('Openhaimer',2, 3.0, 4),

            ('ToyStory',3, 4.2, 1),
            ('ToyStory',3, 5.0, 2),
            ('ToyStory',3, 4.9, 3),
            ('ToyStory',3, 3.0, 4),
        )

        print(result)

        assert result == expected

    def test_self_join(self):
        result = tuple(run(Q(
            NestedLoopJoin(Q(
                MemoryScan(self.left)),
                MemoryScan(self.left)
                )))
            )
        expected =  (
            ('Poor things',1,'Poor things',1),
            ('Poor things',1,'Openhaimer',2),
            ('Poor things',1,'ToyStory',3),
            ('Openhaimer',2,'Poor things',1),
            ('Openhaimer',2,'Openhaimer',2),
            ('Openhaimer',2,'ToyStory',3),
            ('ToyStory',3,'Poor things',1),
            ('ToyStory',3,'Openhaimer',2),
            ('ToyStory',3,'ToyStory',3)
        )
        print(result)
        assert result == expected

    def test_project_after_join(self):
        result = tuple(run(Q(
            Projection(lambda x: (x[0],x[2])),
            NestedLoopJoin(Q(MemoryScan(self.left)),
                           Q(MemoryScan(self.right))))
                        ))
        expected = (
            ('Poor things', 4.2),
            ('Poor things', 5.0),
            ('Poor things', 4.9),
            ('Poor things', 3.0),
            
            ('Openhaimer', 4.2),
            ('Openhaimer', 5.0),
            ('Openhaimer', 4.9),
            ('Openhaimer', 3.0),

            ('ToyStory', 4.2),
            ('ToyStory', 5.0),
            ('ToyStory', 4.9),
            ('ToyStory', 3.0),
            )
        print(result)

        assert result == expected

    def test_project_before_join(self):
        result = tuple(run(Q(
            NestedLoopJoin(Q(Projection(lambda x: (x[0],)),MemoryScan(self.left)),
                           Q(Projection(lambda x: (x[0],)),MemoryScan(self.right))))
                        ))
        expected = (
            ('Poor things', 4.2),
            ('Poor things', 5.0),
            ('Poor things', 4.9),
            ('Poor things', 3.0),
            
            ('Openhaimer', 4.2),
            ('Openhaimer', 5.0),
            ('Openhaimer', 4.9),
            ('Openhaimer', 3.0),

            ('ToyStory', 4.2),
            ('ToyStory', 5.0),
            ('ToyStory', 4.9),
            ('ToyStory', 3.0),
            )
        print(result)

        assert result == expected

    def test_three_way_table(self):
        result = tuple(run(Q(Selection(lambda x: x[4].lower() == 'openhaimer'),NestedLoopJoin(
                           Q(NestedLoopJoin(Q(MemoryScan(self.left)),
                           Q(MemoryScan(self.right)))),
                           Q(Projection(lambda x: (x[0],)),MemoryScan(self.left)))))
                           )
        
        expected = (
            ('Poor things',1, 4.2, 1,'Openhaimer',),
            ('Poor things',1, 5.0, 2,'Openhaimer',),
            ('Poor things',1, 4.9, 3,'Openhaimer',),    
            ('Poor things',1, 3.0, 4,'Openhaimer',),
            ('Openhaimer',2, 4.2, 1,'Openhaimer',),
            ('Openhaimer',2, 5.0, 2,'Openhaimer',),
            ('Openhaimer',2, 4.9, 3,'Openhaimer',),
            ('Openhaimer',2, 3.0, 4,'Openhaimer',),

            ('ToyStory',3, 4.2, 1,'Openhaimer',),
            ('ToyStory',3, 5.0, 2,'Openhaimer',), 
            ('ToyStory',3, 4.9, 3,'Openhaimer',), 
            ('ToyStory',3, 3.0, 4,'Openhaimer',),
 
        )
        
        print(result)

        assert result == expected


class TestHashJoin:
    left = (
        ('Poor things',1,2),
        ('Openhaimer',2,4),
        ('ToyStory',3,3),
        ('ToyStory',3,6),
        ('Jockey',5,5),
    )
    right = (
        (4.2, 1,3),
        (3.0, 4,4),
        (5.0, 2,1),
        (4.9, 3,3),
    )
   
    def test_single_join(self):

       result = tuple(run(Q(
           HashJoin(
               Q(MemoryScan(self.left)),Q(MemoryScan(self.right)),lambda x: x[1],lambda x: x[1])
               )))
       expected = (('Poor things', 1, 2, 4.2, 1, 3), ('Openhaimer', 2, 4, 5.0, 2, 1), ('ToyStory', 3, 3, 4.9, 3, 3), ('ToyStory', 3, 6, 4.9, 3, 3), )
       print(result)
       assert result == expected
    
    def test_multiple_conditions(self):
       result = tuple(run(Q(
           HashJoin(
               Q(MemoryScan(self.left)),Q(MemoryScan(self.right)),lambda x: (x[1],x[2]),lambda x: (x[1],x[2]))
               )))
       expected = ((('ToyStory', 3, 3, 4.9, 3, 3),))
       print(result)
       assert result == expected

    def test_projection_before(self):
        result = tuple(run(Q(
           HashJoin(
               Q(Projection(lambda x: (x[0],x[1])),MemoryScan(self.left)),
               Q(Projection(lambda x: (x[0],x[1])),MemoryScan(self.right)),lambda x: x[1],lambda x: x[1])
               )))
        expected = (('Poor things', 1, 4.2, 1), ('Openhaimer', 2, 5.0, 2), ('ToyStory', 3, 4.9, 3), ('ToyStory', 3, 4.9, 3))
        print(result)
        assert result == expected


    def test_projection_after(self):
        result = tuple(run(Q(Projection(lambda x: (x[0],x[3])),
           HashJoin(
               Q(MemoryScan(self.left)),Q(MemoryScan(self.right)),lambda x: x[1],lambda x: x[1])
               )))
        expected = (('Poor things', 4.2), ('Openhaimer',5.0), ('ToyStory', 4.9), ('ToyStory', 4.9))
        print(result)
        assert result == expected

       
class TestMergeJoin:
    left = (("Claudia",1),("Jose",2),("Marco",3))
    right = ((3.3,1),(3.4,1),(10.5,2),(50,3))

    left_many = (("Claudia",1),("Jose",2),("Jose Jr",2),("Marco",3))
    right_many = ((3.3,1),(3.4,1),(10.5,2),(30.5,2),(50,3))


    def test_join_one_to_many(self):
        result = tuple(run(Q(MergeJoin(
            Q(Sort((lambda x: x[1])),MemoryScan(self.left)),
            Q(Sort((lambda x: x[1])),MemoryScan(self.right)),
            lambda x: x[1],lambda x: x[1]))))
        expected = (('Claudia', 1, 3.3, 1), ('Claudia', 1, 3.4, 1), ('Jose', 2, 10.5, 2), ('Marco', 3, 50, 3))
        print(result)
        assert result == expected
        

    def test_join_many_to_many(self):
        result = tuple(run(Q(MergeJoin(
            Q(Sort((lambda x: x[1])),MemoryScan(self.left_many)),
            Q(Sort((lambda x: x[1])),MemoryScan(self.right_many)),
            lambda x: x[1],lambda x: x[1])
        )))
        expected = (('Claudia', 1, 3.3, 1), 
                    ('Claudia', 1, 3.4, 1), 
                    ('Jose', 2, 10.5, 2), 
                    ('Jose', 2, 30.5, 2), 
                    ('Jose Jr', 2, 10.5, 2), 
                    ('Jose Jr', 2, 30.5, 2), 
                    ('Marco', 3, 50, 3))
        
        print(result)
        assert result == expected



if __name__ == '__main__':
    print('ok')