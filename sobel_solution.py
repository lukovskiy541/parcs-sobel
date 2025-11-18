

from Pyro4 import expose  

SOBEL_X = ((-1, 0, 1), (-2, 0, 2), (-1, 0, 1))
SOBEL_Y = ((-1, -2, -1), (0, 0, 0), (1, 2, 1))

class Solver(object):
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        # зберігаємо параметри запуску: вхід, вихід, воркери
        self.input = input_file_name
        self.output = output_file_name
        self.workers = workers

    def solve(self):
        # головний метод, який запускає повний пайплайн MapReduce
        print "Start"
        
        
        # читаємо зображення у форматі PGM як "сирий" байтовий масив
        w, h, raw_data = self._read_pgm_raw(self.input)
        print "Read image: %dx%d" % (w, h)
        
        
        print "Starting Map phase..."
        # ділимо зображення по горизонталі на задачі для воркерів
        num_tasks = len(self.workers) * 5
        chunk_height = h / num_tasks
        mapped_futures = []
        for i in range(num_tasks):
            # базові межі блока (без перекриття)
            start_row = i * chunk_height
            end_row = h if i == num_tasks - 1 else (i + 1) * chunk_height
            
            
            # додаємо по 1 рядку зверху/знизу для коректної свертки 3x3
            top_overlap_row = max(0, start_row - 1)
            bottom_overlap_row = min(h, end_row + 1)
            
            
            # переводимо межі рядків у байтові індекси
            start_byte = top_overlap_row * w
            end_byte = bottom_overlap_row * w
            
            
            # вирізаємо шматок картинки для конкретної задачі
            data_chunk_str = raw_data[start_byte:end_byte]
            
            # вибираємо воркера по кругу
            worker_id = i % len(self.workers)
            
            # відправляємо задачу на віддалений mymap і зберігаємо future
            future = self.workers[worker_id].mymap(data_chunk_str, w, start_row, end_row, top_overlap_row)
            mapped_futures.append(future)

        del raw_data 
        print "Map phase jobs sent."
        print "Starting Reduce phase..."
        # збираємо всі результати від воркерів
        reduced_data = self.myreduce(mapped_futures)
        print "Reduce phase complete."
        # записуємо фінальне зображення
        self._write(reduced_data, w, h)
        print "Done"

    @staticmethod
    def _read_pgm_raw(path):
        with open(path, "rb") as f:
            f.readline()
            d = f.readline().strip()
            while d.startswith("#"):
                d = f.readline().strip()
            w, h = map(int, d.split())
            f.readline()
            raw_data = f.read(w*h)
            return w, h, raw_data

    @staticmethod
    @expose
    def mymap(rows_as_string, original_w, start_row, end_row, top_overlap_row):
        # застосовує оператор Собеля до свого піддіапазону рядків
        output_chunk = []
        w = original_w
        
        
        for r_abs in range(start_row, end_row):
            
            # локальний індекс всередині отриманого блока (з перекриттям)
            r_local = r_abs - top_overlap_row
            
            for c in range(w):
                gx = gy = 0
                for kr in range(3):
                    sr = r_local + kr - 1
                    if 0 <= sr < len(rows_as_string) / w:
                        for kc in range(3):
                            sc = c + kc - 1
                            if 0 <= sc < w:
                                # беремо сусідній піксель і множимо на ядро Собеля
                                index = sr * w + sc
                                pixel_value = ord(rows_as_string[index])
                                gx += pixel_value * SOBEL_X[kr][kc]
                                gy += pixel_value * SOBEL_Y[kr][kc]
                                
                # модуль градієнта, обрізаємо до [0,255]
                magnitude = int((gx*gx + gy*gy)**0.5)
                output_chunk.append(min(255, magnitude))
        
        
        return output_chunk

    @staticmethod
    @expose
    def myreduce(mapped_futures):
        # послідовно конкатенуємо результати всіх map-задач
        final_result = bytearray()
        for future in mapped_futures:
            chunk_of_ints = future.value
            final_result.extend(chunk_of_ints)
            
        return final_result

    def _write(self, data, w, h):
        # записуємо PGM-заголовок та байти зображення
        print "Writing output file..."
        with open(self.output, "wb") as f:
            f.write("P5\n%d %d\n255\n" % (w, h))
            f.write(data)
