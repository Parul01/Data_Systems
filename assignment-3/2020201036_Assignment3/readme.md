# Assignment 3

    Order of B+tree is 3 
    Internal Node - child pointers = 3, keys = 2
    Leaf Node - 1 pointer to next leaf node, 2 pointers to record(storing temporary values), keys = 2
    
Execution
  - input - python3 Bplustree.py "InputFileName"
    - eg python3 Bplustree.py input.txt
  - output - output.txt file will be generated in current directory
  - Functions implemented are:
    - insert : will insert a key passed as an argument. It will search for an appropriate position to insert key and if the node is already full, will split node in two and the 2nd largest no. in that particular node will be shifted to the parent node.
    - find - It will call helper function which is recursive function. Helper function will be called for child node based on comparison btw key and list of keys at present level, if found, it will return "YES" else "NO"
    - count - similar logic as find, just that it returns occurrence of a key if found else will give 0.
    - range - will look for x first, if x is not present it will search for a first no. greater than x and will go till y or first no. which is smaller than y if y is not present calculating no of elements btw both. Leaf's 'nextptr' is used for traversing from one leaf node to its immediate next leaf node. 
