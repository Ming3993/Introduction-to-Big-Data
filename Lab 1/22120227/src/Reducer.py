import sys

# Khởi tạo từ điển để lưu kết quả
letter_count = {}

# Đọc từng dòng từ input (Mapper output)
for line in sys.stdin:
    # Tách dữ liệu ra (chữ cái và số đếm)
    try:
        letter, count = line.strip().split("\t")
        count = int(count)
    except ValueError:
        continue  # Bỏ qua dòng lỗi
    
    # Cộng dồn số lần xuất hiện của từng chữ cái
    if letter in letter_count:
        letter_count[letter] += count
    else: 
        letter_count[letter] = count
    
# Danh sách các chữ cái yêu cầu
target_letters = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']  

# In kết quả
for letter in target_letters:
    if letter in letter_count:
        print(f"{letter}\t{letter_count[letter]}")