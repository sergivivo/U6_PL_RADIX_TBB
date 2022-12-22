from math import ceil, log2
from array import array

def test1(l):
    """Usando listas de Python"""
    output = l.copy()
    digits = ceil(log2(max(l)))
    for d in range(digits):
        mask = 2**d
        left = []
        right = []
        for e in output:
            if e & mask == 0:
                left.append(e)
            else:
                right.append(e)
        output = left + right
    return output


def test2(l):
    """Usando arrays de Python"""
    output = array('i', l)
    left   = array('i', l)
    right  = array('i', l)

    digits = ceil(log2(max(l)))
    for d in range(digits):
        l_head = 0
        r_head = 0
        mask = 2**d
        for i in range(len(output)):
            if output[i] & mask == 0:
                left[l_head] = output[i]
                l_head += 1
            else:
                right[r_head] = output[i]
                r_head += 1

        # Put left side
        for i in range(l_head):
            output[i] = left[i]

        # Put right side
        for i in range(r_head):
            output[l_head + i] = right[i]

    return output

# Funciones paralelizables
def myMap(inp, op, out):
    """ Paralelizable O(1) """
    for i in range(min(len(inp), len(out))):
        out[i] = op(inp[i])

def myScan(inp, op, out, default):
    """ Paralelizable O(n*log2(n)) """
    result = default
    for i in range(min(len(inp), len(out))):
        result = op(result, inp[i])
        out[i] = result

# Funciones aplicadas en map y scan
def checkMask(value, mask):
    return value & mask != 0

def countTrue(acc, curr):
    return acc + (1 if curr else 0)

def countFalse(acc, curr):
    return acc + (1 if not curr else 0)

# Solución paralelizable
def test3(l):
    """
    Método pre-paralelización:
        Check with mask 0x00000001:
            input  = [5, 4, 3, 2, 1]

            map    = [1, 0, 1, 0, 1] # Map function with mask

            scan1  = [1, 1, 2, 2, 3] # Scan count ones
            scan2  = [0, 1, 1, 2, 2] # Scan count zeroes

            output = [5, 3, 1, 4, 2]
    """
    map0 = array('i', l)

    scan1 = array('i', l)
    scan2 = array('i', l)

    # Alterna entre arrays auxiliares cada iteración
    aux1 = array('i', l)
    aux2 = array('i', l)

    # Array copy
    for i in range(len(l)):
        aux1[i] = l[i]

    # Solution
    digits = ceil(log2(max(l)))
    for d in range(digits):
        # Switch input and output arrays every iteration
        inp = aux1 if d % 2 == 0 else aux2
        out = aux2 if d % 2 == 0 else aux1

        mask = 2**d

        # Paralelizable bloque 1 {
        myMap(inp, lambda value: checkMask(value, mask), map0)
        print("Map: ",map0)
        # } O(1)

        # Paralelizable bloque 2 {
        myScan(map0, countFalse, scan1, 0)
        myScan(map0, countTrue,  scan2, 0)
        print("Scan 1: ",scan1)
        print("Scan 2: ",scan2)
        # } O(n*log(n))

        # Paralelizable bloque 3 {
        for i in range(len(inp)):
            if map0[i] == False:
                j = scan1[i] - 1
                out[j] = inp[i]
            else:
                j = scan1[-1] + scan2[i] - 1
                out[j] = inp[i]
        print("Output: ",out)
        # } O(1)

        # Tiempo total tras paralelizar:
        #   O(1) + O(n*log(n)) + O(1) = O(n*log(n))

    return aux1 if digits % 2 == 0 else aux2


if __name__ == '__main__':
    l = [32, 12, 5, 2, 64, 12, 4, 84, 1, 3]

    output = test3(l)

    print(output)


