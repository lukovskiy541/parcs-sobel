import random
import string

SIZE_MB = 15
PATTERN_OCCURRENCES = 1000 
PATTERN = "SPECIALPATTERN"
CHARSET = string.ascii_letters + string.digits + ' \n'  
OUTPUT_FILE = "rabin_karp_input.txt"  

def generate_large_text(size_mb=SIZE_MB, pattern_occurrences=PATTERN_OCCURRENCES):
    
    target_size = size_mb * 1024 * 1024 
    
    text_parts = []
    current_size = 0
    patterns_inserted = 0

    chunk_size = target_size // (pattern_occurrences + 1)
    
    while current_size < target_size:

        chunk = ''.join(random.choices(
            CHARSET,
            k=min(chunk_size, target_size - current_size)
        ))
        text_parts.append(chunk)
        current_size += len(chunk)

        if patterns_inserted < pattern_occurrences and current_size < target_size:
            text_parts.append(PATTERN)
            current_size += len(PATTERN)
            patterns_inserted += 1
        
        if len(text_parts) % 100 == 0:
            progress = (current_size / target_size) * 100
    
    full_text = ''.join(text_parts)

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(PATTERN + '\n') 
        f.write(full_text)
    
    print(f"Файл збережено: {OUTPUT_FILE}")

if __name__ == '__main__':

    import sys
    size_mb = int(sys.argv[1]) if len(sys.argv) > 1 else SIZE_MB
    generate_large_text(size_mb=size_mb, pattern_occurrences=size_mb * 100)

