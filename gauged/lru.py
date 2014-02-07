'''
Gauged
https://github.com/chriso/gauged (MIT Licensed)
Copyright 2014 (c) Chris O'Hara <cohara87@gmail.com>
'''

class LRU(object):
    '''A least recently used cache'''

    def __init__(self, maximum):
        self.maximum = maximum
        self.data = {}
        self.head = None
        self.tail = None

    def clear(self):
        while self.head is not None:
            del self[self.head.key]

    def __contains__(self, key):
        return key in self.data

    def __getitem__(self, key):
        value = self.data[key].value
        self[key] = value
        return value

    def __setitem__(self, key, value):
        if key in self.data:
            del self[key]
        obj = LRUNode(self.tail, key, value)
        if not self.head:
            self.head = obj
        if self.tail:
            self.tail.next = obj
        self.tail = obj
        self.data[key] = obj
        if len(self.data) > self.maximum:
            head = self.head
            head.next.prev = None
            self.head = head.next
            head.next = None
            del self.data[head.key]
            del head

    def __delitem__(self, key):
        obj = self.data[key]
        if obj.prev:
            obj.prev.next = obj.next
        else:
            self.head = obj.next
        if obj.next:
            obj.next.prev = obj.prev
        else:
            self.tail = obj.prev
        del self.data[key]

class LRUNode(object):
    '''A node in the LRU cache'''

    __slots__ = ['prev', 'next', 'key', 'value']

    def __init__(self, prev, key, value):
        self.prev = prev
        self.key = key
        self.value = value
        self.next = None
