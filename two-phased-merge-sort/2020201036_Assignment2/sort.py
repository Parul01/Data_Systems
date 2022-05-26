import sys
import math
import os
import itertools
import functools
import copy
import time

# heapq.heapify(li)
# count = sum( [ len(listElem) for listElem in listOfElems2D])
"""
with open(file_to_save, "r") as text_file:
    for line in itertools.islice(text_file, 19, 52):

"""

start_time = time.time()
# INPUT_FILE = "input.txt"
# OUTPUT_FILE = "output.txt"
INPUT_FILE = sys.argv[1]
OUTPUT_FILE = sys.argv[2]
FILE_SIZE = os.path.getsize(INPUT_FILE)  # IN BYTES
memSize = int(sys.argv[3]) * 1024 * 1024
orderBy = sys.argv[4]
# print(memSize)
noOfArguments = len(sys.argv)
metaData = "metadata.txt"
orderByCol = sys.argv[5:]  # col to sort
corrColSize = []
corrColIndex = []  # columns tp sort by
col_start_idx = []
col_end_idx = []

col_indices = []

class heapnode:

    def __init__(self,tuple,fileHandler,):
        self.tuple = tuple
        self.fileHandler = fileHandler


print("FILE_SIZE")
print(FILE_SIZE)
with open(metaData, 'r') as rfile:
    tmp = rfile.readlines()

# print(tmp)

colList = []

for x in tmp:
    colList.append(x.split(",")[0])
    corrColSize.append(int(x.split(",")[1][:len(x) - 2]))

print("collist")
print(colList)




    # print(x)
# print(corrColSize)
for i in orderByCol:
    corrColIndex.append(colList.index(i))

print("corrcolindex")
print(corrColIndex)
start = 0
# end = corrColSize[0]
for i in range(len(colList)):
    end = start + corrColSize[i]
    col_indices.append(copy.deepcopy([start,end]))
    start = end+2

print("col indices")
print(col_indices)

for i in corrColIndex:
    col_start_idx.append(col_indices[i][0])
    col_end_idx.append(col_indices[i][1])

print("starting indices")
print(col_start_idx)

print("ending indices")
print(col_end_idx)

print("columns tp sort by")

print(corrColIndex)

# print(orderByCol)

blockSize = 5
# tupleSize = sum(corrColSize)  # in bytes
tupleSize = sum(corrColSize) + len(colList)*2
if tupleSize > memSize:
    sys.exit("tuple size exceeds!!")
print(tupleSize)
TUPLE_COUNT_IN_INPUT = int(FILE_SIZE / tupleSize)
print("tuples in input.txt")
print(TUPLE_COUNT_IN_INPUT)
print("mem size")
print(memSize)
noOfTuples = math.floor(memSize / tupleSize)  # no of tuples to sort
size_of_single_file = noOfTuples * tupleSize
# print(tupleSize)
# tupleCount = memSize
partition_count = math.ceil(FILE_SIZE / size_of_single_file)
# partition_count = TUPLE_COUNT_IN_INPUT/
print("no of tuples")

print(noOfTuples)
print("no of files")
print(partition_count)

# with open("input.txt",'r') as
memList = []


# for i in range(noOfTuple):
#     memList.append()

# with open("input1.txt", 'r') as file:
#     content = file.read()
#
# memList = []
# for i in content.split('\n'):
#     memList.append(i)
#     print(i)

# ---------------------------------------------------------------------functions
# create comparator
def compare(t1, t2):
    global col_start_idx,col_end_idx

    global orderBy, corrColIndex
    l1 = str(t1)
    l2 = str(t2)
    #
    # l1 = l1.split('  ')
    # l2 = l2.split('  ')

    if orderBy == 'asc':
        for i in range(len(corrColIndex)):

            if l1[col_start_idx[i]:col_end_idx[i]] < l2[col_start_idx[i]:col_end_idx[i]]:
                return -1
            elif l1[col_start_idx[i]:col_end_idx[i]] > l2[col_start_idx[i]:col_end_idx[i]]:
                return 1

        return -1
    else:
        for i in range(len(corrColIndex)):
            if l1[col_start_idx[i]:col_end_idx[i]] > l2[col_start_idx[i]:col_end_idx[i]]:
                return -1
            elif l1[col_start_idx[i]:col_end_idx[i]] < l2[col_start_idx[i]:col_end_idx[i]]:
                return 1

        return -1

def comparep(t1, t2):
    global col_start_idx, col_end_idx

    global orderBy, corrColIndex
    l1 = str(t1)
    l2 = str(t2)

    if orderBy == 'asc':
        for i in range(len(corrColIndex)):

            if l1[col_start_idx[i]:col_end_idx[i]] < l2[col_start_idx[i]:col_end_idx[i]]:
                return -1
            elif l1[col_start_idx[i]:col_end_idx[i]] > l2[col_start_idx[i]:col_end_idx[i]]:
                return 1

        return -1
    else:
        for i in range(len(corrColIndex)):
            if l1[col_start_idx[i]:col_end_idx[i]] > l2[col_start_idx[i]:col_end_idx[i]]:
                return -1
            elif l1[col_start_idx[i]:col_end_idx[i]] < l2[col_start_idx[i]:col_end_idx[i]]:
                return 1

        return -1


#
#
#     return x - y
#
#
# def sort_file(fileName):


def split_file(fileName, top_row, last_row):
    w = open(fileName, "w+")
    tmp_content = ""
    tmp_list = []
    count = 0
    with open(INPUT_FILE, "r") as rFile:
        for line in itertools.islice(rFile, top_row, last_row):
            # if top_row == 0:
            # print(count)
            # count += 1
            # print(line)
            tmp_list.append(line)
            tmp_content += line
    # tmp_list[len(tmp_list) - 1] = tmp_list[len(tmp_list) - 1] + "\n"
    # tmp_list[len(tmp_list) - 1] = tmp_list[len(tmp_list) - 1] + "\n"
    tmp_list = sorted(tmp_list, key=functools.cmp_to_key(compare))
    # print(" ---------- ********** ------------------")
    # print(" ")
    # print(tmp_list)
    # print(" ")
    # print(" ---------- ********** ------------------")
    for i in tmp_list:
        w.write("%s" % i)

    w.close()


def sort_individual():
    global partition_count
    top_row = 0
    last_row = noOfTuples
    for i in range(partition_count):
        # for partition count make a new file which is sorted
        fileName = "inp" + str(i) + ".txt"
        split_file(fileName, top_row, last_row)
        top_row = last_row
        last_row += noOfTuples


# divide mem in two parts 1 input files and 2 output files
# from all files read first tuple and make a heap
total_file_size = math.floor(memSize / 2)  # 50000B
total_output_file_size = math.floor(memSize / 2)  # 50000B
tuplesCount = math.floor(total_file_size / tupleSize)  # I can take tuplesCount tuples at max # 500
tuples_to_read = math.floor(tuplesCount / partition_count)  # tuples to read from each file at a time # 100
outputTuples = math.floor(total_output_file_size / tupleSize)  # 500

current_file = 0
current_row = 1
output_list = []

current_file_idx = 0
start_row = 0
end_row = tuples_to_read
row_list_count = 0


def get_file_descriptors():
    file_descriptors = []

    for i in range(partition_count):
        file_to_read = "inp" + str(i) + ".txt"
        fd = open(file_to_read, "r")
        file_descriptors.append(fd)
    return file_descriptors


ROW_IDX = 0




def heapify(f_lines, loc):
    if len(f_lines) == 0:
        return []

    least_idx = loc
    lc = least_idx * 2 + 1
    rc = least_idx * 2 + 2
    if lc >= len(f_lines) and rc >= len(f_lines):
        # print("min heap printing")
        # print(f_lines)
        return f_lines

    if lc < len(f_lines) and comparep(f_lines[least_idx].tuple, f_lines[lc].tuple) == 1:
        least_idx = lc

    if rc < len(f_lines) and comparep(f_lines[least_idx].tuple, f_lines[rc].tuple) == 1:
        least_idx = rc

    # swap
    # f_lines[least_idx], f_lines[loc] = f_lines[loc], f_lines[least_idx]
    if loc != least_idx:
        tmp = f_lines[least_idx]
        f_lines[least_idx] = f_lines[loc]
        f_lines[loc] = tmp

    # if lc < len(f_lines) and rc < len(f_lines) and loc != least_idx:
    #
    #     heapify(f_lines, least_idx)
    if loc != least_idx:
        f_lines = heapify(f_lines, least_idx)
    # print("min heap printing")
    # print(f_lines)
    return f_lines



def append_to_output(output_list):
    # print("appending")
    fw = open(OUTPUT_FILE, "a+")
    for i in output_list:
        fw.write("%s" % i)
    fw.close()

#
# def del_all_records(f_lines, rows_list):
#     global output_list
#
#     while len(f_lines) > 0:
#         count = len(f_lines)
#         while count > 0:
#             tmp = f_lines[0]
#             output_list.append(copy.deepcopy(tmp))
#             count -= 1
#             f_lines[0] = str(f_lines[len(f_lines) - 1])
#             f_lines = f_lines[:len(f_lines) - 1]
#             f_lines = heapify(f_lines, 0)
#         # append_to_output(output_list)
#         # sys.exit("testing")
#         f_lines = get_first_lines(rows_list)
#         if len(f_lines) == 0:
#             if len(output_list) > 0:
#                 append_to_output(output_list)
#                 output_list = []
#
#             return
#         f_lines = build_heap(f_lines)
#         print("sorted")
#         print(f_lines)
#         print(" ---- ")
#         # f_lines = heapify(f_lines, 0)
#     append_to_output(output_list)
#     output_list = []
#
#
# #


def build_heap(f_lines):

    # print("entered build_heap function")
    for i in reversed(range(math.ceil(len(f_lines) / 2))):
        f_lines = heapify(f_lines, i)

    return f_lines


def get_first_lines(file_descriptors):
    global row_list_count, ROW_IDX

    f_lines = []
    for i in file_descriptors:
        obj = heapnode(i.readline(),i)
        f_lines.append(obj)

    return f_lines

def insert_from_file(f_lines, fd):

    tmp = fd.readline()
    if tmp:
        obj = heapnode(tmp, fd)
        f_lines[0] = obj
        f_lines = heapify(f_lines, 0)
    else:
        f_lines[0] = f_lines[len(f_lines)-1]
        f_lines = f_lines[:len(f_lines)-1]
        f_lines = heapify(f_lines, 0)

    # if fd.hasNextLine():
    #     obj = heapnode(fd.readline(), fd)
    #     f_lines[0] = obj
    #     f_lines = heapify(f_lines, 0)
    # else:
    #     f_lines[0] = f_lines[len(f_lines)-1]
    #     f_lines = f_lines[:len(f_lines)-1]
    #     f_lines = heapify(f_lines, 0)

    return f_lines

def del_from_heap(file_descriptors, f_lines):
    # print("enter delete")
    global output_list
    if len(f_lines) <= 0:
        return []
    tmp = f_lines[0].tuple
    fd = f_lines[0].fileHandler
    # f_lines = insert_from_file(f_lines,fd)

    # prev_len = len(f_lines)
    f_lines = insert_from_file(f_lines,fd)
    # curr_len = len(f_lines)
    # output_list.append(copy.deepcopy(tmp))
    #
    # if len(output_list) == tuplesCount:
    #     append_to_output(output_list)
    #     output_list = []
    fw = open(OUTPUT_FILE, "a+")

    fw.write("%s" % tmp)
    fw.close()

    return f_lines
    # if(curr_len < prev_len):

def close_file_descriptors(file_descriptors):
    for i in file_descriptors:
        i.close()

def create_heap():
    global ROW_IDX
    count = 0
    file_descriptors = get_file_descriptors()  # tick
    # while loop here
    rows_list = []
    # rows_list = make_list(file_descriptors)
    f_lines = get_first_lines(file_descriptors) # list of objects heapnode

    f_lines = build_heap(f_lines) # tick

    del_from_heap(file_descriptors, f_lines)

    print("TUPLE_COUNT_IN_INPUT")
    print(TUPLE_COUNT_IN_INPUT)
    while count != TUPLE_COUNT_IN_INPUT:
        f_lines = del_from_heap(file_descriptors,f_lines)
        count += 1
    close_file_descriptors(file_descriptors)
    # del_all_records(f_lines, rows_list)
    # while len(rows_list) > 0:
    #     print("while loop")
    #     ROW_IDX = 0
    #     rows_list = make_list(file_descriptors) # tick
    #     # f_lines = get_first_lines(file_descriptors,0)
    #     if len(rows_list[0]) <= 0:
    #         return
    #     f_lines = get_first_lines(rows_list) # tick
    #     f_lines = build_heap(f_lines)
    #     del_all_records(f_lines, rows_list)


# sorted([5, 2, 4, 1, 3], cmp=numeric_compare)

def final_sort():
    create_heap()


# print(memList)

# ----------------------------------------------------call

sort_individual()
final_sort()

end_time = time.time()
print("Time taken to run: ")
print(end_time - start_time)
