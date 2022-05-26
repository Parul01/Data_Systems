import sys
import bisect

inputFile = sys.argv[1]
outputFile = "output.txt"

class Node(object):

    def __init__(self):
        self.key = []  # 2
        self.ptr = []  # 3 pointers
        self.nextptr = None
        self.isLeaf = True
        self.key_count_i = {}
        self.count = 0


class BTree(object):

    def __init__(self):
        self.root = Node()


    def insertWrapper(self, root, key):
        key_len = len(root.key)

        if root.isLeaf == True:
            if root.count == 2:

                parent = self.split(root, key)

                return (True, parent)

            else:
                root.count += 1
                bisect.insort_left(root.key, key)
                bisect.insort_left(root.ptr, key)
                return (False, root)

        else:
            for i in range(key_len):
                if (key < root.key[i]):
                    flag, tmpNode = self.insertWrapper(root.ptr[i], key)
                    break
                elif i + 1 == key_len:
                    flag, tmpNode = self.insertWrapper(root.ptr[i + 1], key)
                    break

            if flag:
                if root.count + 1 == 3:
                    # root.ptr[1] = tmpNode.ptr[0]
                    t = tmpNode.key[0]
                    if t < root.key[0]:
                        root.ptr[0] = tmpNode.ptr[1]
                    elif t > root.key[0] and t < root.key[1]:
                        root.ptr[1] = tmpNode.ptr[1]
                    elif t > root.key[1]:
                        root.ptr[2] = tmpNode.ptr[0]

                    parent = self.merge_internal(root, tmpNode)

                    return True, parent
                else:
                    # left

                    if tmpNode.key[0] < root.key[0]:
                        root.key.insert(0, tmpNode.key[0])
                        tmpNode.ptr.append(root.ptr[1])

                        root.ptr = tmpNode.ptr
                        root.count += 1
                        return (False, root)
                    # right
                    if tmpNode.key[0] > root.key[0]:

                        root.key.append(tmpNode.key[0])
                        root.ptr[1] = tmpNode.ptr[0]
                        root.ptr.append(tmpNode.ptr[1])
                        root.count += 1
                        return (False, root)
            else:
                return (False, root)

        return (False, root)


    def if_exist(self,root, key):
        if root.isLeaf:
            if key in root.key:
                if key in root.key_count_i:
                    root.key_count_i[key] += 1
                else:
                    # create
                    root.key_count_i[key] = 2
                return True
            else:
                return False

        l = len(root.key)
        for i in range(l):
            if (key < root.key[i]):
                return self.if_exist(root.ptr[i], key)
            elif i + 1 == l:
                return self.if_exist(root.ptr[i + 1], key)

    def insert_h(self,root, key):
        return self.if_exist(root, key)

    def insert(self, key):

        counter = self.root.count
        if not counter == 0:
            if self.insert_h(self.root, key):

                return
            # if self.if_exist(self.root, key):
            #     return

        if self.root.isLeaf:
            if self.root.count <= 1:
                bisect.insort_left(self.root.key, key)
                bisect.insort_left(self.root.ptr, key)
                self.root.count += 1

            else:
                parent = self.split(self.root, key)
                self.root = parent


        else:
            flag, parent = self.insertWrapper(self.root, key)
            self.root = parent


    def makeNewNode(self, root, isLeaf, pos, lc, rc):
        tmp = Node()
        if isLeaf:
            tmp.key = root.key[pos:]
            tmp.ptr = root.ptr[pos:]
            for k in tmp.key:
                if k in root.key_count_i:
                    tmp.key_count_i[k] = root.key_count_i[k]

            tmp.isLeaf = True
            tmp.count = 2
        else:
            tmp.isLeaf = False
            tmp.key.append(root.key[pos])
            tmp.ptr.append(lc)
            tmp.ptr.append(rc)
            tmp.count = 1

        return tmp


    def make_internal_node(self, root, lptr, rptr):

        tmpNode = Node()
        tmpNode.isLeaf = False
        tmpNode.key.append(root.key[lptr])
        tmpNode.count = 1
        tmpNode.ptr = root.ptr[lptr:rptr]

        return tmpNode

    def split_internal(self, root):
        leftChild = self.make_internal_node(root, 0, 2)
        rightChild = self.make_internal_node(root, 2, 4)
        root.key = [root.key[1]]
        root.ptr = [leftChild, rightChild]
        root.count = 1
        return root

    def merge_left(self, root, tmpNode):

        pointer = tmpNode.ptr[0]
        root.ptr.insert(0, pointer)

        value = tmpNode.key[0]
        root.key.insert(0, value)

        root.ptr[1] = tmpNode.ptr[1]
        return root

    def merge_right(self, root, tmpNode):
        root.ptr.append(tmpNode.ptr[1])

        bisect.insort_left(root.key, tmpNode.key[0])

        return root

    def merge_mid(self, root, tmpNode):
        root.ptr[1] = tmpNode.ptr[0]
        root.key.insert(1, tmpNode.key[0])

        root.ptr.insert(2, tmpNode.ptr[1])
        return root

    def merge_internal(self, root1, root2):
        key = root2.key[0]
        # left
        if key < root1.key[0]:
            merged_root = self.merge_left(root1, root2)

        # mid
        elif key > root1.key[0] and key < root1.key[1]:
            merged_root = self.merge_mid(root1, root2)

        # right
        else:
            merged_root = self.merge_right(root1, root2)

        tmp = self.split_internal(merged_root)
        return tmp

    def insert_after_split(self,root, key):
        bisect.insort_left(root.key, key)
        bisect.insort_left(root.ptr, key)
        return root


    def split(self, root, key):
        if root.isLeaf:

            root = self.insert_after_split(root, key)

            siblingRight = self.makeNewNode(root, True, 1, -1, -1)

            siblingRight.nextptr = root.nextptr

            parent = self.makeNewNode(root, False, 1, root, siblingRight)
            root.nextptr = siblingRight

            root.key = [root.key[0]]
            for k in siblingRight.key:
                if k in root.key_count_i:
                    root.key_count_i.pop(k)

            root.ptr = [root.ptr[0]]

            root.count = 1
            return parent

    def countof(self, root, key):
        if root.isLeaf:
            if key in root.key:
                if key in root.key_count_i:
                    return root.key_count_i[key]
                else:
                    return 1
            else:
                return 0
        l = len(root.key)
        for i in range(l):
            if (key < root.key[i]):
                return self.countof(root.ptr[i], key)
            elif i + 1 == l:
                return self.countof(root.ptr[i + 1], key)


    def findf(self, root, key):

        root_list = root.key
        for i in range(len(root_list)):
            if key == root_list[i]:
                return True

        if root.isLeaf == True:
            return False

        l = len(root.key)
        for i in range(l):
            if (key < root.key[i]):
                return self.findf(root.ptr[i], key)
            elif i + 1 == l:
                return self.findf(root.ptr[i + 1], key)

    def find(self, key):
        if self.findf(self.root, key):
            return "YES"
        else:
            return "NO"

    def countCaller(self, key):
       return self.countof(self.root,key)

    def rangeHelper(self, root, x, y):
        while not root.isLeaf:
            root = root.ptr[0]
        count_nodes = 0
        if root.isLeaf:
            flag = False

            while root != None:
                for val in root.key:
                    if (val == x or val > x) and val <= y:
                        if val in root.key_count_i:
                            count_nodes = count_nodes + root.key_count_i[val]
                        else:
                            count_nodes = count_nodes + 1

                    if val >= y:
                        flag = True
                        break
                if flag:
                    break
                root = root.nextptr


        return count_nodes


    def range(self, x, y):
        return self.rangeHelper(self.root,x,y)


# take data form file

obj = BTree()


write_to_file = ""
with open(inputFile,"r") as fread:

    f = fread.readline()
    while f:

    # for f in fileData:
        tmp = f[:len(f)-1]
        # print(tmp)
        tmp = tmp.split(" ")
        # tmp = queryList[tmp]

        if tmp[0] == "INSERT":
            obj.insert(int(tmp[1]))
        elif tmp[0] == "FIND":
           write_to_file += str(obj.find(int(tmp[1])))
           write_to_file += "\n"
        elif tmp[0] == "COUNT":
            write_to_file += str(obj.countCaller(int(tmp[1])))
            write_to_file += "\n"
        elif tmp[0] == "RANGE":
            write_to_file += str(obj.range(int(tmp[1]), int(tmp[2])))
            write_to_file += "\n"
        else:
            write_to_file += "Invalid query!"
            write_to_file += "\n"

        f = fread.readline()



# write data to file
# print("out file data")
# print(write_to_file)
w = open(outputFile, "w")
w.write(write_to_file)
w.close()

