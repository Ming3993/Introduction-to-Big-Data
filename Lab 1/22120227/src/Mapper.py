import sys
import re

# Danh sách các chữ cái cần đếm
target_letters = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']

# Đọc từng dòng từ input (words.txt)
for line in sys.stdin:
    # Tách các từ, bao gồm cả việc tách từ có ký tự đặc biệt
    words = re.findall(r'[a-zA-Z]+', line)  # Chỉ lấy các phần có chữ cái

    # Lặp qua từng từ
    for word in words:
        first_letter = word[0].lower()  # Lấy chữ cái đầu tiên và chuyển về chữ thường

        # Kiểm tra chữ cái đầu tiên có trong danh sách target_letters
        if first_letter in target_letters:
            print(f"{first_letter}\t1")  # Xuất ra dạng "chữ cái đầu \t 1" để truyền sang Reducer