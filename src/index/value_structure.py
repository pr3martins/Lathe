from ctypes import *
from utils import StructHelper,ConfigHandler
config = ConfigHandler()
# config.value_list

class DynamicListStructure(LittleEndianStructure, StructHelper):    
    _pack_ = 1
    _fields_ = [
        ('len', c_uint16),
        ('ctids', POINTER(c_uint16))
    ]
    
    def __init__(self, size):
        self.ctids = (c_uint16 * size)()
        for i in range(size):
            self.ctids[i] = 0

# config.attribute_count
class DynamicValueStructure(LittleEndianStructure, StructHelper):
    _pack_ = 1
    _fields_ = [
        ('iaf', c_float),
        ('attribute_list', POINTER(DynamicListStructure)),
        ('attribute_size', c_uint32),
        ('ctids_size', c_uint32),
    ]

    def __init__(self, attribute_size, value_list):
        self.attribute_list = (DynamicListStructure * attribute_size)()
        for i in range(attribute_size):
            self.attribute_list[i] = DynamicListStructure(value_list)


    def to_static_strucuture(self):
        for i in range(self.attribute_size):
            type("StaticValueStructure", LittleEndianStructure, {"_fields_": fields})



class StaticListStructure(LittleEndianStructure, StructHelper):    
    _pack_ = 1
    _fields_ = [
        ('len', c_uint16),
        ('ctids', c_uint16 * config.max_ctids_count)
    ]
        
# config.attribute_count
class StaticValueStructure(LittleEndianStructure, StructHelper):
    _pack_ = 1
    _fields_ = [
        ('iaf', c_float),
        ('attribute_list', StaticListStructure * config.attribute_count)
    ]

   