import os
import sys
import json

def get_segments(query):
    tokens = query.split("\"")
    segments = []
    for i, token in range(tokens):
        if i % 2 == 0:
            segments += tokens.split(" ")
        else:
            segments += [token]
    return segments


def mas_convert_gt_tsv_to_json(input_filename, output_filename): 
    ground_truth = []
    query_data = []

    dataset = input_filename
    
    with open(dataset) as f:
        
        for i,line in enumerate(f):
            if i == 0:
                continue

            line_segments = line.split('\r\n')[0].split('\t')
            
            result = line_segments[1].lower()
            candidates = result.split(';')
            generate_candidates = {}
            gt_item = {}
            gt_item['id'] = i
            gt_item['natural_language_query'] = line_segments[0]
            gt_item['sql_query'] = ""
            
            query_matches_by_table = {}
            keywords = []
            
            for candidate in candidates:
                candidate_tokens = candidate.split(':')
                segment = candidate_tokens[0].lstrip()
                table, attr = candidate_tokens[1].split('.')
                #print(candidate_tokens)
                type_word = candidate_tokens[2]
                data = query_matches_by_table.setdefault(table, {})

                filter_type = 'schema_filter' if type_word == 'NT' else 'value_filter'
                dict_to_fill = data.setdefault(filter_type, {}) 
                dict_to_fill.setdefault(attr, []).append(segment)
                keywords += [segment]
                
            gt_item['segments'] = keywords
            query_matches = []
            
            for table in query_matches_by_table:
                query_match_item = {}
                query_match_item['table'] = table
                for mapping_type  in ['schema_filter', 'value_filter']:
                    query_match_item[mapping_type] = []

                    for attr in query_matches_by_table[table].setdefault(mapping_type, []):
                        attr_item = {}
                        attr_item['attribute'] = attr
                        attr_item['keywords'] = query_matches_by_table[table][mapping_type][attr]
                        query_match_item[mapping_type] += [attr_item]
                
                query_matches += [query_match_item]
            gt_item['query_matches'] = query_matches
            ground_truth = ground_truth + [gt_item]
            
    with open(output_filename, 'w') as f:
        json.dump(ground_truth, f, indent=4)





if __name__ == "__main__":
    mas_convert_gt_tsv_to_json(sys.argv[1], sys.argv[2])
    