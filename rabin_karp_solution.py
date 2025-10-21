from Pyro4 import expose

class RollingHash:
    def __init__(self, text, size):
        self.text = text
        self.hash = 0
        self.size = size

        for i in range(size):
            self.hash += ord(self.text[i])
        
        self.start = 0
        self.end = size
    
    def update(self):
        if self.end <= len(self.text) - 1:
            self.hash -= ord(self.text[self.start])
            self.hash += ord(self.text[self.end])
            self.start += 1
            self.end += 1
    
    def digest(self):
        return self.hash
    
    def get_text(self):
        return self.text[self.start:self.end]


def rabin_karp_search(pattern, text):
    if not pattern or not text:
        return []
    
    if len(pattern) > len(text):
        return []
    
    positions = []

    text_hash = RollingHash(text, len(pattern))
    pattern_hash = RollingHash(pattern, len(pattern))
    pattern_hash_value = pattern_hash.digest()

    for i in range(len(text) - len(pattern) + 1):
        if text_hash.digest() == pattern_hash_value:
            if text_hash.get_text() == pattern:
                positions.append(i)
        text_hash.update()
    
    return positions


class Solver:

    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers
        print("Solver initialized")
    
    def solve(self):
        print("Job Started")
        print("Workers: %d" % len(self.workers))
        
        pattern, text = self.read_input()
        print("Text length: %d, Pattern length: %d" % (len(text), len(pattern)))

        num_workers = len(self.workers)
        chunk_size = len(text) // num_workers
        overlap = len(pattern) - 1

        mapped = []
        for i in range(num_workers):
            start = i * chunk_size
            
            if i == num_workers - 1:
                end = len(text)
            else:
                end = (i + 1) * chunk_size + overlap
            
            chunk = text[start:end]
            print("Worker %d: chunk [%d:%d]" % (i, start, end))
            mapped.append(self.workers[i].mymap(pattern, chunk, start))
        
        print("Map phase finished")
        reduced = self.myreduce(mapped)
        print("Found %d matches" % len(reduced))
        self.write_output(reduced)
        
        print("Job Finished")
    
    @staticmethod
    @expose
    def mymap(pattern, text_chunk, offset):
        local_positions = rabin_karp_search(pattern, text_chunk)
        global_positions = [pos + offset for pos in local_positions]
        return global_positions
    
    @staticmethod
    @expose
    def myreduce(mapped):
        all_positions = []
        for result in mapped:
            all_positions.extend(result.value)
        
        all_positions = sorted(list(set(all_positions)))
        return all_positions
    
    def read_input(self):
        with open(self.input_file_name, 'r') as f:
            lines = f.readlines()
            pattern = lines[0].strip()
            text = ''.join(lines[1:]).replace('\n', ' ')
        return pattern, text
    
    def write_output(self, positions):
        with open(self.output_file_name, 'w') as f:
            f.write("Found %d matches\n" % len(positions))
            f.write("Positions: %s\n" % str(positions[:100]))
            if len(positions) > 100:
                f.write("... and %d more\n" % (len(positions) - 100))
