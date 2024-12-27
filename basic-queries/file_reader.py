class CSVFileStream(object):

    def __init__(self,path,chunk_size,separetor = ",",contain_header=True):
        self.file = open(path,mode = 'rb',buffering=18000)
        if contain_header:
            self.file.readline() # read the header
        self.chunk_size = chunk_size
        self.separetor = separetor
    
    def stream_file(self):
        i = 0
        lines = []
        for line in self.file:
            lines.append(tuple(line))
            #print(line)
            i+=1
            if i > self.chunk_size:
                break        
        return lines

if __name__ == '__main__':
    f = CSVFileStream('/home/ubuntu/Home/Downloads/ml-20m/movies.csv',1000000)
    f.stream_file()
    f.stream_file()

