from ctypes import *
from utils import StructHelper,ConfigHandler
config = ConfigHandler()

class ListStructure(LittleEndianStructure, StructHelper):    
    _pack_ = 1
   
    _fields_ = [
        ('len', c_uint16),
        ('ctids', c_uint16 * config.value_list)
    ]

class ValueStructure(LittleEndianStructure, StructHelper):
    _pack_ = 1
    _fields_ = [
        ('iaf', c_float),
        ('attribute_list', ListStructure * config.attribute_count)
    ] 