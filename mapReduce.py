import threading
from collections import defaultdict, Counter

from threading import Lock
lock = Lock()

import os

class MapReduceController:
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.word_counts = defaultdict(list)
        self.lock = threading.Lock()
        self.dicionarioPalavras = {}

    def map(self, part_filename, intermediario_filename):
        lock.acquire()
        with open(part_filename, 'r') as file:
            for line in file.readlines():
                words = line.strip().split()
                for word in words:
                    with open(intermediario_filename, 'a') as intermediate_file:
                        intermediate_file.write(f"{word} 1\n")
        lock.release()

    def reduce(self, output_filename, chave, valor):
        lock.acquire()

        with open(output_filename, 'a') as output_file:
            output_file.write(f"Reduce({chave}, {len(valor)})\n")

        lock.release()

    def ler_intermediario(self, intermediario):
        with open(intermediario, 'r') as arquivo:
            for line in arquivo.readlines():
                chave, valor = line.strip().split()
                if self.dicionarioPalavras.get(chave) is not None:
                    self.dicionarioPalavras[chave].append(valor)
                else:
                    self.dicionarioPalavras[chave] = [valor]


    def run_map_reduce(self, filenames, intermediario, arquivoRemove):
        for arquivo in arquivoRemove:
            if os.path.exists(arquivo):
                os.remove(arquivo)

        threadsMap = []

        for filename in filenames:
            thread = threading.Thread(target=self.map, args=(filename, intermediario))
            threadsMap.append(thread)
            thread.start()

        for thread in threadsMap:
            thread.join()

        self.ler_intermediario(intermediario)

        threadsReduce = []
        for item in self.dicionarioPalavras.keys():
            thread = threading.Thread(target=self.reduce, args=(final, item, self.dicionarioPalavras[item]))
            threadsReduce.append(thread)
            thread.start()

        for thread in threadsReduce:
            thread.join()

# Exemplo de uso:
split = 10
num_threads = 10
filenames = []

for i in range(split):
    filenames.append(f"random_words.txt_part{i+1}.txt")

intermediario = "intermediario.txt"
final = "final.txt"

controller = MapReduceController(num_threads)
#controller.map(filenames[0], intermediario)

arquivo_a_remover = ["final.txt", "intermediario.txt"]

controller.run_map_reduce(filenames, intermediario, arquivo_a_remover)

