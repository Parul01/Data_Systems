import sys
import itertools
import math
import functools
import time
from prettytable import PrettyTable
import matplotlib.pyplot as plt

import os

# <path of R file> <path of S file> <sort/hash> <M>
type_map = {'sort':'merge_join','hash':'hash_join'}
arguments = sys.argv[1].split(" ")

type_of_join = arguments[2]

start_time = time.time()

if type_of_join == 'sort':
    print("JOIN ALGORITHM")
    print(type_of_join)
    class heapnode:

        def __init__(self,tuple,sublist, y, blockno):
            self.tuple = tuple
            self.sublist = sublist
            self.blockNo = blockno
            self.y = y

    # M memory block - each block 100 tuples


    # OUTPUT_FILE = 'out.txt'
    R = ""
    S = ""
    R = arguments[0]
    S = arguments[1]
    res1 = R.split("/")
    res1 = res1[len(res1) - 1]
    res2 = S.split("/")
    res2 = res2[len(res2) - 1]
    OUTPUT_FILE = res1 + "_" + res2 + "_" + "join.txt"

    # print(type_of_join)
    # no of buffers
    M = int(arguments[3])
    tuple_in_block = 100
    noOfTuples = M*tuple_in_block

    # no_of_types_in_R =
    # each sublist will be of size M blocks (M*100 tuples)
    # type_map[type_of_join]()
    y_R = []
    y_S = []

    def compareR(t1, t2):
        l1 = str(t1)
        l2 = str(t2)
        l1 = l1.split(" ")
        l2 = l2.split(" ")

        x1 = l1[1]
        x2 = l2[1]

        if x1 < x2:
            return -1
        else:
            return 1

    def compareS(t1, t2):
        l1 = str(t1)
        l2 = str(t2)
        l1 = l1.split(" ")
        l2 = l2.split(" ")

        x1 = l1[0]
        x2 = l2[0]

        if x1 < x2:
            return -1
        else:
            return 1


    def split_file(input_file, fileName, top_row, last_row):
        w = open(fileName, "w+")
        tmp_content = ""
        tmp_list = []
        count = 0
        with open(input_file, "r") as rFile:
            for line in itertools.islice(rFile, top_row, last_row):

                tmp_list.append(line)
                tmp_content += line

        if input_file == R:
            tmp_list = sorted(tmp_list, key=functools.cmp_to_key(compareR))
        else:
            tmp_list = sorted(tmp_list, key=functools.cmp_to_key(compareS))

        for i in tmp_list:
            w.write("%s" % i)

        w.close()

    def sort_individual(input_file, no_of_sublist_of_R,append_string):
        # global no_of_sublist_of_R
        top_row = 0
        last_row = noOfTuples
        sublists_name = []
        for i in range(no_of_sublist_of_R):
            # for partition count make a new file which is sorted
            # fileName = input_file
            fileName = input_file + str(i) + ".txt"
            # fileName = append_string + str(i) + ".txt"
            split_file(input_file, fileName, top_row, last_row)
            top_row = last_row
            last_row += noOfTuples
            sublists_name.append(fileName)
        return sublists_name

    def make_sublist(R,S):
        # find files to be created for R
        # no_tuples_in_R = 0
        no_tuples_in_R = sum(1 for line in open(R))
        no_of_sublist_R = math.ceil(no_tuples_in_R/noOfTuples)

        # no_of_tuples_in_S = 0
        no_tuples_in_S = sum(1 for line in open(S))
        no_of_sublist_S = math.ceil(no_tuples_in_S/noOfTuples)

        # find files to be created for S
        #create files for R
        R_sublists_name = sort_individual(R,no_of_sublist_R,"r")
        S_sublists_name = sort_individual(S, no_of_sublist_S,"s")

        return R_sublists_name, S_sublists_name

    def getFileDescriptors(sublist):
        desc_list = []
        for i in sublist:
            f = open(i,'r')
            desc_list.append(f)
        return desc_list



    def openCall(R,S):
        R_sublists_name, S_sublists_name = make_sublist(R,S)
        # check cond B(R) + B(S) <= M^2
        no_of_sublists_in_total = len(R_sublists_name) + len(S_sublists_name)
        if no_of_sublists_in_total * M > M*M: # becoz we are bringing 1 block from each sublist in memory (M blocks)
            sys.exit("Memory Exceeded")
        # open fileDescriptors
        fd_R = getFileDescriptors(R_sublists_name)
        fd_S = getFileDescriptors(S_sublists_name)
        return fd_R, fd_S, R_sublists_name, S_sublists_name

    def heapify(f_lines, loc, tbl):
        if len(f_lines) == 0:
            return

        least_idx = loc
        lc = least_idx * 2 + 1
        rc = least_idx * 2 + 2
        if lc >= len(f_lines) and rc >= len(f_lines):
            # print("min heap printing")
            # print(f_lines)
            return

        if tbl == R:
            if lc < len(f_lines) and compareR(f_lines[least_idx].tuple, f_lines[lc].tuple) == 1:
                least_idx = lc
            if rc < len(f_lines) and compareR(f_lines[least_idx].tuple, f_lines[rc].tuple) == 1:
                least_idx = rc
        else:
            if lc < len(f_lines) and compareS(f_lines[least_idx].tuple, f_lines[lc].tuple) == 1:
                least_idx = lc
            if rc < len(f_lines) and compareS(f_lines[least_idx].tuple, f_lines[rc].tuple) == 1:
                least_idx = rc

        # swap
        if loc != least_idx:
            f_lines[least_idx], f_lines[loc] = f_lines[loc], f_lines[least_idx]


        # f_lines[least_idx], f_lines[loc] = f_lines[loc], f_lines[least_idx]
        # if loc != least_idx:
        #     tmp = f_lines[least_idx]
        #     f_lines[least_idx] = f_lines[loc]
        #     f_lines[loc] = tmp

        # if lc < len(f_lines) and rc < len(f_lines) and loc != least_idx:
        #
        #     heapify(f_lines, least_idx)
        if loc != least_idx:
            heapify(f_lines, least_idx, tbl)
        # print("min heap printing")
        # print(f_lines)
        # f_lines

    def check(f_lines):
        tr = ""
        with open("sas.txt",'w+') as wr:
            for i in f_lines:
                tr += i.tuple
            wr.write(tr)


    def build_heap(f_lines, tbl):
        # print("entered build_heap function")
        for i in reversed(range(math.ceil(len(f_lines) / 2))):
            heapify(f_lines, i,tbl)

    def getNextBlock(fd_list,next_block, tbl, block_count):
        blockList = []
        # if next_block == 0:
        #     top_row = 0
        # else:
        #     top_row = next_block*100

        # last_row = top_row + 100
        #
        # for i in range(len(fd_list)):
        #     for line in itertools.islice(fd_list[i], top_row, last_row):
        #         if not line:
        #             return
        #         block_count[i] += 1
        #         f_list = line.split(" ")
        #         if tbl == R:
        #             y = f_list[1]
        #         else:
        #             y = f_list[0]
        #         tmp = heapnode(line,i, y)
        #         blockList.append(tmp)
        for i in range(len(fd_list)):
            # file1 = open(fd_list[i], "r")
            file1  = fd_list[i]
            # fd_r.append(file1)
            curr = 100
            for j in range(curr):
                line1 = file1.readline()
                if not line1:
                    break
                block_count[i] = block_count[i] + 1
                # tup=line1.split("  ")
                t1 = line1.strip().split(' ')
                if tbl == S:
                    y_val = t1[0]
                else:
                    y_val = t1[1]

                newnode = heapnode(line1, i, y_val, i)
                blockList.append(newnode)

        return blockList


    def modify_queue(R_pq, block_count, fd_R, tmp_file, fw, tbl):

        prev = R_pq[0].y

        block_count[R_pq[0].sublist] = block_count[R_pq[0].sublist] - 1

        l = len(R_pq) - 1
        pos = R_pq[0].sublist
        tupr = R_pq[0].tuple
        R_pq[0] = R_pq[l]
        del R_pq[-1]

        heapify(R_pq, 0, R)
        file1 = open(tmp_file, "r")
        # changed
        col = tupr.split(" ")[0]

        content = file1.readlines()

        for i in content:
            fw.write(col + " " + i)


        if block_count[pos] != 0:
            pass
        else:
            fill_up_pq(R_pq, fd_R, pos, block_count, R)
        file1.close()

        while len(R_pq) > 0:
            if prev != R_pq[0].y:
                break


            prev = R_pq[0].y

            tupr = R_pq[0].tuple
            col = tupr.split(" ")[0]
            if tbl == R:
                t = 0
            block_count[R_pq[0].sublist] = block_count[R_pq[0].sublist] - 1
            pos = R_pq[0].sublist

            l = len(R_pq) - 1
            R_pq[0] = R_pq[l]

            del R_pq[-1]

            heapify(R_pq, 0, R)
            file1 = open(tmp_file, "r")
            while (True):
                line1 = file1.readline()

                if not line1:
                    break
                fw.write(col + " " + line1)
            if block_count[pos] != 0:
                pass
            else:
                fill_up_pq(R_pq, fd_R, pos, block_count, R)
            file1.close()

    def fill_up_pq(pq_S, fd_S, sub_no, block_wise_s, tbl):

        fd = fd_S[sub_no]
        j = 0
        while j < tuple_in_block:

            row = fd.readline()
            if not row:
                break
            block_wise_s[sub_no] = block_wise_s[sub_no] + 1
            t1 = row.strip().split(' ')
            if tbl == S:
                yy = t1[0]
            else:
                yy = t1[1]

            newnode = heapnode(row, sub_no, yy, j)
            pq_S.append(newnode)

            j += 1
        build_heap(pq_S, tbl)

    def get_prev(pq_S, block_wise_s, fd_S, tbl):

        sub_no = pq_S[0].sublist
        block_wise_s[sub_no] -= 1
        tmp = pq_S[0].y

        l = len(pq_S) - 1
        pq_S[0] = pq_S[l]
        del pq_S[-1]

        heapify(pq_S, 0, tbl)

        if block_wise_s[sub_no] != 0:
            pass
        else:
            fill_up_pq(pq_S, fd_S, sub_no, block_wise_s, tbl)
        return tmp

    def del_from_pq(R_pq, yvalue, block_count, fd_R, tbl, tmp):

        prev = get_prev(R_pq, block_count, fd_R, tbl)

        while len(R_pq) > 0:
            if prev != R_pq[0].y:
                break

            prev = get_prev(R_pq, block_count, fd_R, tbl)


    def extract_all_from_S(pq_S,block_wise_s, fd_S, tbl, outW):


        fw = open("sas.txt", "w+")
        # row = pq_S[0].tuple
        fw.write(pq_S[0].tuple)

        prev = get_prev(pq_S, block_wise_s, fd_S, tbl)

        while len(pq_S) > 0:
            if prev != pq_S[0].y:
                break
            row = pq_S[0].tuple
            fw.write(row)
            prev = get_prev(pq_S, block_wise_s, fd_S, tbl)

        fw.close()
        return "sas.txt"


    def join(R_pq, S_pq, R_length, S_length, block_wise_r, block_wise_s, fd_R, fd_S):
        extra_buffer = M - (R_length + S_length)
        tr = ""
        outW = open(OUTPUT_FILE,'w+')
        while len(R_pq) > 0:
            if len(S_pq) <= 0:
                break

            if S_pq[0].y == R_pq[0].y:
                tmp_file = extract_all_from_S(S_pq, block_wise_s, fd_S, S,outW)
                modify_queue(R_pq, block_wise_r, fd_R, tmp_file, outW, R)

            else:
                if R_pq[0].y < S_pq[0].y:
                    del_from_pq(R_pq, R_pq[0].y, block_wise_r, fd_R, R, S_pq[0].y)
                else:
                    del_from_pq(S_pq, S_pq[0].y, block_wise_s, fd_S, S, R_pq[0].y)


        outW.close()
        # delete temp file
        os.remove(tmp_file)


    def getNext(fd_R, fd_S, R_sublists_name, S_sublists_name):
        R_length = len(R_sublists_name)
        S_length = len(S_sublists_name)
        next_block = 0
        block_wise_r = [0 for i in range(R_length)]
        block_wise_s = [0 for i in range(S_length)]

        # get first blocks of R
        R_pq = getNextBlock(fd_R, next_block, R, block_wise_r)
        # get first blocks of S
        S_pq = getNextBlock(fd_S, next_block, S, block_wise_s)
        # build priority queues
        build_heap(R_pq, R)
        build_heap(S_pq, S)
        join(R_pq, S_pq, R_length, S_length,block_wise_r, block_wise_s, fd_R, fd_S)



    fd_R, fd_S, R_sublists_name, S_sublists_name = openCall(R, S)
    getNext(fd_R, fd_S, R_sublists_name, S_sublists_name)

else:
    print("JOIN ALGORITHM")
    print(type_of_join)




    # <path of R file> <path of S file> <sort/hash> <M>
    def hashCode(key):
        mul = 1
        index = 0
        for i in range(len(key)):
            index = (index + ord(key[i]) * mul) % buckets
            mul = (mul * p) % buckets

        return int(index)


    # OUTPUT_FILE = 'out.txt'
    # R = ""
    # S = ""
    R = arguments[0]
    S = arguments[1]
    # type_of_join = sys.argv[3]
    # no of buffers
    res1 = R.split("/")
    res1 = res1[len(res1) - 1]
    res2 = S.split("/")
    res2 = res2[len(res2) - 1]
    OUTPUT_FILE = res1 + "_" + res2 + "_" + "join.txt"

    M = int(arguments[3])
    tuple_in_block = 100
    buckets = M-1
    p = 31
    noOfTuples = M * tuple_in_block
    block_wise_r = []
    block_wise_s = []


    def create_files(append_string):
        buckets_fds = []
        files = []
        for i in range(buckets):
            # for partition count make a new file which is sorted
            fileName = append_string + str(i) + ".txt"
            files.append(fileName)
            buckets_fds.append(open(fileName, 'w+'))
        return buckets_fds, files


    def fill_buckets(input_file, bucket_fds, block_wise):
        for i in range(len(bucket_fds)):
            block_wise.append(0)

        with open(input_file, 'r') as fread:
            lines = fread.readlines()

        for i in lines:
            if input_file == R:
                y = i.split(" ")[1][0:-1]
            else:
                y = i.split(" ")[0]

            idx = hashCode(y)
            bucket_fds[idx].write(i)
            block_wise[idx] += 1


    def close_fds(r_buckets, s_buckets):
        for i in r_buckets:
            i.close()
        for i in s_buckets:
            i.close()


    def open_fds(r_names, s_names):
        r_buckets = []
        s_buckets = []

        for i in range(len(r_names)):
            r_buckets.append(open(r_names[i], 'r'))
            s_buckets.append(open(s_names[i], 'r'))
        return r_buckets, s_buckets


    r_buckets, r_names = create_files(R)
    s_buckets, s_names = create_files(S)

    fill_buckets(R, r_buckets, block_wise_r)
    fill_buckets(S, s_buckets, block_wise_s)
    close_fds(r_buckets, s_buckets)
    r_buckets, s_buckets = open_fds(r_names, s_names)


    def check_for_error(block_wise_r, block_wise_s):
        smaller_block = []
        for i in range(len(block_wise_r)):
            if block_wise_r[i] < block_wise_s[i]:
                smaller_block.append(R)
            else:
                smaller_block.append(S)

            m = min(block_wise_s[i], block_wise_r[i])
            if m > noOfTuples:
                sys.exit("Memory exceeded!")
        return smaller_block


    def make_dictionary(fd, smaller):
        y_dictionary = dict()
        content = fd.readlines()
        if not content:
            return
        for i in content:
            if smaller == R:
                y = i.split(" ")[1][0:-1]
            else:
                y = i.split(" ")[0]

            if y in y_dictionary:
                y_dictionary[y].append(i)
            else:
                y_dictionary[y] = []
                y_dictionary[y].append(i)

        return y_dictionary


    def join(y_dictionary, fd, smaller, available_mem, outW):
        if len(y_dictionary) == 0:
            return
        content = fd.readlines()
        if not content:
            return
        for i in content:
            if smaller == R:
                y = i.split(" ")[0]
            else:
                y = i.split(" ")[1][0:-1]

            if y not in y_dictionary:
                continue
            to_join_list = y_dictionary[y]
            if smaller == R:
                for j in range(len(to_join_list)):
                    outW.write(to_join_list[j].split(" ")[0] + " " + i)
            else:
                for j in range(len(to_join_list)):
                    outW.write(i.split(" ")[0] + " " + to_join_list[j])


    def getNext(r_buckets, s_buckets, smaller_block, block_wise_r, block_wise_s):
        outW = open(OUTPUT_FILE, 'w+')
        for i in range(len(smaller_block)):
            if block_wise_r[i] == 0 or block_wise_s[i] == 0:
                continue
            if smaller_block[i] == R:
                y_dictionary = make_dictionary(r_buckets[i], smaller_block[i])
                available_mem = M * 100 - block_wise_r[i]
                join(y_dictionary, s_buckets[i], smaller_block[i], available_mem, outW)
            else:
                y_dictionary = make_dictionary(s_buckets[i], smaller_block[i])
                available_mem = M * 100 - block_wise_s[i]
                join(y_dictionary, r_buckets[i], smaller_block[i], available_mem, outW)
        outW.close()


    smaller_block = check_for_error(block_wise_r, block_wise_s)

    getNext(r_buckets, s_buckets, smaller_block, block_wise_r, block_wise_s)

end_time = time.time()
print("Time taken to run: ")
print(end_time - start_time)
