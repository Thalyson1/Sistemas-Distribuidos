import random
import string

class FileGenerator:
    def __init__(self, split, N, alphabet, minSize, maxSize):
        self.split = split
        self.N = N
        self.alphabet = alphabet
        self.minSize = minSize
        self.maxSize = maxSize

    def generate_random_word(self):
        return ''.join(random.choice(self.alphabet) for _ in range(random.randint(self.minSize, self.maxSize)))

    def generate_file(self, filename):
        with open(filename, 'w') as file:
            for _ in range(self.N):
                word = self.generate_random_word()
                file.write(word + '\n')

    def split_file(self, filename):
        with open(filename, 'r') as file:
            lines = file.readlines()
            num_lines = len(lines)
            lines_per_part = num_lines // self.split
            remainder = num_lines % self.split
            
            start_index = 0
            for i in range(self.split):
                part_filename = f"{filename}_part{i+1}.txt"
                with open(part_filename, 'w') as part_file:
                    part_lines = lines[start_index:start_index + lines_per_part]
                    if remainder > 0:
                        part_lines.append(lines[start_index + lines_per_part])
                        start_index += 1
                        remainder -= 1
                    part_file.writelines(part_lines)
                    start_index += lines_per_part

split = 10
N = 1000
alphabet = ['a', 'b', 'c', 'd']
minSize = 3
maxSize = 4

file_generator = FileGenerator(split, N, alphabet, minSize, maxSize)
filename = "random_words.txt"
file_generator.generate_file(filename)
file_generator.split_file(filename)
