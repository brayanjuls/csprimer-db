from data_layout import DB_HEADER_SIZE, PAGE_SIZE, DBHeader,DBPage

class FileScan(object):
    def __init__(self,path,db_name,table_name,schema):
        self.header = DBHeader(db_name,table_name,schema)
        self.page = DBPage()
        self.db_path = path
        self.db = open(self.db_path,mode='r+b')
        #load header
        db_header_bytes = self.db.read(DB_HEADER_SIZE)
        self.header.decode(db_header_bytes)
        
        

    def next(self) -> tuple:
        if self.has_next():
            record = self.page.records.pop(0)
            return record.record
        return ()

    def has_next(self) -> bool:
        if len(self.page.records) == 0:
            self.load_next_page()
        if len(self.page.records) > 0:
            return True
        return False
            
    
    def load_next_page(self):
         #load page
        page_bytes = self.db.read(PAGE_SIZE)
        self.page.decode(page_bytes,self.header.schema)

    def __del__(self):
        if self.db is not None:
            self.db.close()



class CSVFileStream(object):

    def __init__(self,path,chunk_size,separetor = ",",contain_header=True):
        self.file = open(path,buffering=chunk_size,mode = 'rt',encoding='utf-8',)
        
        if contain_header:
            self.file.readline() # read the header
        self.chunk_size = chunk_size
        self.separetor = separetor
        # self.reader = csv.reader(self.file)
        # next(self.reader)
    
    def stream_file(self):
        i = 0
        lines = []
        # try:
        #     return [tuple(next(self.reader))]
        # except:
        #     return lines
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
    Return only as many as the limit, then stop
    """
    def __init__(self, n, offset = 0):
        self.n = n
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
            self.buble_sort()
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




class Aggregation(object):
    def __init__(self,group_col,col,func_name):
        self.group_col = group_col
        self.col = col
        self.func_name = func_name.lower()
        self.acc = dict()
        self.result_keys = set()
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
                current_group_col  = self.group_col(current_tuple)
                current_acc_val = self.acc.get(current_group_col,0)
                self.result_keys.add(current_group_col)
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
            key = self.result_keys.pop()
            return (key,self.acc.get(key))
        else:
            key = self.result_keys.pop()
            return (key,self.acc.get(key))
    
        return None


    def has_next(self):
        return self.child.has_next() or len(self.result_keys) > 0

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
    db_path = "/home/ubuntu/Home/Downloads/ml-20m/movies.db"
    def test_full_scan(self):
        result = tuple(run(Q(
            Projection(lambda x: (x[1],x[2])),
            Limit(3),
            FileScan(self.db_path,'mydb','movies',('int','str','str'))
            )))
        print("Data Results:")
        print(result)
        assert result == ((1, 'Toy Story (1995)'), (2, 'Jumanji (1995)'), (3, 'Grumpier Old Men (1995)'))



if __name__ == '__main__':
    # Test data generated by Claude and probably not accurate!
    print('ok')