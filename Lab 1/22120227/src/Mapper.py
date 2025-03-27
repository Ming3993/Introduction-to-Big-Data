import sys
import re

# Danh sách các chữ cái cần đếm
target_letters = ['a', 'f', 'j', 'g', 'h', 'c', 'm', 'u', 's']

# Đọc từng dòng từ input (words.txt)
for line in sys.stdin:
    words = line.strip().split()    # Tách từng từ trong dòng

    # Lặp qua từng từ
    for word in words:
        # Kiểm tra xem có ký tự đặc biệt không
        if re.search(r'[^a-zA-Z]', word):
            continue

        first_letter = word[0].lower()  # Lấy chữ cái đầu tiên và chuyển về chữ thường

        # Kiểm tra chữ cái đầu tiên có trong danh sách target_letters
        if first_letter in target_letters:
            print(f"{first_letter}\t1") # Xuất ra dạng "chữ cái đầu \t 1" để truyền sang Reducer