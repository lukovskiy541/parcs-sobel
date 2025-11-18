



try:
    from Pyro4 import expose  
except Exception:  
    def expose(func):
        return func



SOBEL_X = (
    (-1, 0, 1),
    (-2, 0, 2),
    (-1, 0, 1),
)

SOBEL_Y = (
    (-1, -2, -1),
    (0, 0, 0),
    (1, 2, 1),
)


class Solver:
    
    def __init__(self, workers=None, input_file_name=None, output_file_name=None):
        self.input_file_name = input_file_name
        self.output_file_name = output_file_name
        self.workers = workers

    @staticmethod
    def _resolve_result(res):
        
        steps = 0
        while not isinstance(res, dict) and steps < 5:
            steps += 1
            try:
                if hasattr(res, 'get') and callable(getattr(res, 'get')):
                    res = res.get()
                    continue
            except Exception:
                pass
            try:
                if hasattr(res, 'result') and callable(getattr(res, 'result')):
                    res = res.result()
                    continue
            except Exception:
                pass
            try:
                if hasattr(res, 'getValue') and callable(getattr(res, 'getValue')):
                    res = res.getValue()
                    continue
            except Exception:
                pass
            if hasattr(res, 'value'):
                try:
                    res = res.value
                    continue
                except Exception:
                    pass
            if hasattr(res, 'result_value'):
                try:
                    res = res.result_value
                    continue
                except Exception:
                    pass
            if hasattr(res, 'data'):
                try:
                    res = res.data
                    continue
                except Exception:
                    pass
            break
        return res

    def solve(self):
        width, height, matrix = self._read_pgm(self.input_file_name)
        use_local = not self.workers
        num_workers = len(self.workers) if self.workers else 1
        chunk_height = (height + num_workers - 1) // num_workers
        mapped = []
        next_row = 0
        for i in range(num_workers):
            start_row = next_row
            end_row = height if i == num_workers - 1 else min(height, start_row + chunk_height)
            top_guard = max(0, start_row - 1)
            bottom_guard = min(height, end_row + 1)
            chunk = [row[:] for row in matrix[top_guard:bottom_guard]]
            payload = {
                "pixels": chunk,
                "start_row": start_row,
                "width": width,
                "has_top_guard": top_guard < start_row,
                "has_bottom_guard": bottom_guard > end_row,
            }
            if use_local:
                mapped.append(self.mymap(payload))
            else:
                
                res = self.workers[i].mymap(payload)
                mapped.append(Solver._resolve_result(res))
            next_row = end_row

        reduced = self.myreduce(mapped)
        self.write_output(reduced)

    @staticmethod
    def _read_pgm(path):
        def _read_next_tokenized_line(fh):
            while True:
                line = fh.readline()
                if not line:
                    raise ValueError("Unexpected EOF in PGM header")
                s = line.strip()
                if not s:
                    continue
                if s.startswith(b"#"):
                    continue
                return s

        with open(path, "rb") as f:
            magic = _read_next_tokenized_line(f)
            if magic != b"P5":
                raise ValueError("Only binary PGM (P5) is supported")
            dims = _read_next_tokenized_line(f)
            parts = dims.split()
            if len(parts) != 2:
                raise ValueError("Invalid PGM dimensions line")
            w, h = map(int, parts)
            maxval_line = _read_next_tokenized_line(f)
            maxval = int(maxval_line)
            if maxval != 255:
                raise ValueError("Unsupported maxval in PGM")
            data = f.read(w * h)
            if len(data) < w * h:
                raise ValueError("Truncated PGM data")
            pixels = list(bytearray(data))
            matrix = [pixels[r * w : (r + 1) * w] for r in range(h)]
            return w, h, matrix

    @staticmethod
    @expose
    def mymap(chunk_payload):
        pixels = chunk_payload["pixels"]
        width = chunk_payload["width"]
        start_row = chunk_payload["start_row"]
        has_top_guard = chunk_payload["has_top_guard"]
        has_bottom_guard = chunk_payload["has_bottom_guard"]

        local_start = 1 if has_top_guard else 0
        local_end = len(pixels) - (1 if has_bottom_guard else 0)
        actual_rows = []

        for local_row in range(local_start, local_end):
            row_result = []
            for col in range(width):
                gx = 0.0
                gy = 0.0
                for kr in range(3):
                    src_row = local_row - 1 + kr
                    if src_row < 0 or src_row >= len(pixels):
                        continue
                    for kc in range(3):
                        src_col = col - 1 + kc
                        if src_col < 0 or src_col >= width:
                            continue
                        pixel_value = pixels[src_row][src_col]
                        gx += pixel_value * SOBEL_X[kr][kc]
                        gy += pixel_value * SOBEL_Y[kr][kc]
                magnitude = (gx * gx + gy * gy) ** 0.5
                row_result.append(magnitude)
            actual_rows.append(row_result)

        return {
            "start_row": start_row,
            "rows": actual_rows,
        }

    @staticmethod
    @expose
    def myreduce(mapped):
        ordered = []
        for res in mapped:
            ordered.append(Solver._resolve_result(res))
        ordered.sort(key=lambda x: x["start_row"])
        combined = []
        for chunk in ordered:
            
            if not isinstance(chunk, dict):
                chunk = {"start_row": 0, "rows": []}
            combined.extend(chunk["rows"])

        combined = [[int(v if v < 255 else 255) for v in row] for row in combined]
        return combined

    def write_output(self, pixels):
        height = len(pixels)
        width = len(pixels[0]) if height else 0
        flat = [v for row in pixels for v in row]
        out_path = "/tmp/output_sobel.pgm"
        with open(out_path, "wb") as f:
            header = "P5\n%d %d\n255\n" % (width, height)
            f.write(header.encode("ascii"))
            f.write(bytearray(int(v) & 0xFF for v in flat))
