# The repr and str methods for the class BabelHash print the underlying key_ids
# used for the dict.
# For a representation within the keys inserted by the setitem method,
# it is recommended to use the methods from pprint or IPython.display modules.

class BabelHash():

    babel = {}
    __slots__ = ('_dict')

    def __init__(self, **kwargs):
        self._dict = {}

    def __len__(self):
        return len(self._dict)

    def __getidfromkey__(self,key):
        return BabelHash.babel[key]

    def __getkeyfromid__(self,key_id):
        key = BabelHash.babel[key_id]
        return key

    def __getitem__(self,key):
        key_id = self.__getidfromkey__(key)
        return self._dict[key_id]

    def __setitem__(self,key,value):
        try:
            key_id = BabelHash.babel[key]
        except KeyError:
            key_id = len(BabelHash.babel)+1

            BabelHash.babel[key] = key_id
            BabelHash.babel[key_id] = key

        self._dict[key_id] = value

    def __delitem__(self, key):
        key_id = self.__getidfromkey__(key)
        self._dict.__delitem__(key_id)

    def __missing__(self,key):
        key_id = self.__getidfromkey__(key)
        return key_id

    def __contains__(self, key):
        try:
            key_id = self.__getidfromkey__(key)
        except KeyError:
            return False

        return self._dict.__contains__(key_id)

    def __iter__(self):
        for key_id in self._dict.keys():
            yield self.__getkeyfromid__(key_id)

    def __repr__(self):
        items = ','.join(f'{key}:{value}' for key,value in self.items())
        return f'<BabelHash {{{items}}}>'

    def keys(self):
        yield from self.__iter__()

    def items(self):
        for key in self.__iter__():
            yield key, self.__getitem__(key)

    def get(self,key):
        value = None
        if key in self:
            value = self.__getitem__(key)
        return value

    def setdefault(self,key,default=None):
        if key not in self:
            self[key]=default
        return self[key]
