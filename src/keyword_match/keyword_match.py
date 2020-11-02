class KeywordMatch:
    
    def __init__(self, table, value_filter={},schema_filter={}):
        self.__slots__ =['table','schema_filter','value_filter']
        self.table = table
        self.schema_filter= frozenset({ (key,frozenset(keywords)) for key,keywords in schema_filter.items()})
        self.value_filter= frozenset({ (key,frozenset(keywords)) for key,keywords in value_filter.items()})

    def is_free(self):
        return len(self.schema_filter)==0 and len(self.value_filter)==0

    def schema_mappings(self):
        for attribute, keywords in self.schema_filter:
            yield (self.table,attribute,keywords)

    def value_mappings(self):
        for attribute, keywords in self.value_filter:
            yield (self.table,attribute,keywords)

    def mappings(self):
        for attribute, keywords in self.schema_filter:
            yield ('s',self.table,attribute,keywords)
        for attribute, keywords in self.value_filter:
            yield ('v',self.table,attribute,keywords)

    def keywords(self,schema_only=False):
        for attribute, keywords in self.schema_filter:
            yield from keywords
        if not schema_only:
            for attribute, keywords in self.value_filter:
                yield from keywords

    def len_keywords(self):
        total_len  = 0
        for attribute, keywords in self.schema_filter:
            total_len += len(keywords)
        for attribute, keywords in self.value_filter:
            total_len += len(keywords)

        return total_len

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        str_km = ""

        def str_filter(filter_type,fltr):
            if len(fltr) == 0:
                return ""

            return ".{}({})".format(
                filter_type,
                ','.join(
                    [ "{}{{{}}}".format(
                        attribute,
                        ','.join(keywords)
                        ) for attribute,keywords in fltr
                    ]
                )
            )

        return "{}{}{}".format(self.table.upper(),str_filter('s',self.schema_filter),str_filter('v',self.value_filter))

    def __eq__(self, other):
        return (isinstance(other, KeywordMatch)
                and self.table == other.table
                #and set(self.keywords(schema_only=True)) == set(other.keywords(schema_only=True))
                and self.schema_filter == other.schema_filter
                and self.value_filter == other.value_filter)

    def has_same_attribute(self, other):
        all_equal_attributes = False
        attrib = set([attrib for (attrib,keywords) in self.value_filter])
        other_attrib = set([attrib for (attrib,keywords) in other.value_filter])

        if attrib != other_attrib:
            return False
        
       
        attrib = set([attrib for (attrib,keywords) in self.schema_filter])
        other_attrib = set([attrib for (attrib,keywords) in other.schema_filter])

        if attrib == other_attrib:
            all_equal_attributes=True

        return all_equal_attributes 

    def __hash__(self):
        return hash( (self.table,frozenset(self.keywords(schema_only=True)),self.value_filter) )

    def to_json_serializable(self):

        def filter_object(fltr):
            return [{'attribute':attribute,
                            'keywords':list(keywords)} for attribute,keywords in fltr]

        return {'table':self.table,
                'schema_filter':filter_object(self.schema_filter),
                'value_filter':filter_object(self.value_filter),}

    def to_json(self):
        return json.dumps(self.to_json_serializable())

    @staticmethod
    def from_str(str_km):
        re_km = re.compile('([A-Z,_,1-9]+)(.*)')
        re_filters = re.compile('\.([vs])\(([^\)]*)\)')
        re_predicates = re.compile('([\w\*]*)\{([^\}]*)\}\,?')
        re_keywords = re.compile('(\w+)\,?')

        m_km=re_km.match(str_km)
        table = m_km.group(1).lower()
        schema_filter={}
        value_filter={}
        for filter_type,str_predicates in re_filters.findall(m_km.group(2)):

            if filter_type == 'v':
                predicates = value_filter
            else:
                predicates = schema_filter
            for attribute,str_keywords in re_predicates.findall(str_predicates):
                predicates[attribute]={key for key in re_keywords.findall(str_keywords)}
        return KeywordMatch(table,value_filter=value_filter,schema_filter=schema_filter,)


    def from_json_serializable(json_serializable):

        def filter_dict(filter_obj):
            return {predicate['attribute']:predicate['keywords'] for predicate in filter_obj}

        return KeywordMatch(json_serializable['table'],
                            value_filter  = filter_dict(json_serializable['value_filter']),
                            schema_filter  = filter_dict(json_serializable['schema_filter']),)

    def from_json(str_json):
        return KeywordMatch.from_json_serializable(json.loads(str_json))

   
        
