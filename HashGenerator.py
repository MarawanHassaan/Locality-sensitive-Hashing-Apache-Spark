import sympy
import random


class HashGenerator:
    hashes = []
    N_hashes = 0

    def __init__(self, bits = 64, seed = 1647818):
        random.seed(seed)
        
        self.bits = bits

        prime = random.getrandbits(bits)

        while (not sympy.isprime(prime)):
            prime = random.getrandbits(bits)

        self.p = prime

    def addHash(self):
        a = random.getrandbits(self.bits)%self.p
        b = random.getrandbits(self.bits)%self.p
        self.hashes.append((a,b))
        self.N_hashes += 1

    def HashFamily(self, i):
        while (i >= self.N_hashes):
            self.addHash()
        a = self.hashes[i][0]
        b = self.hashes[i][1]
        return lambda x: (a*x+b)%self.p
