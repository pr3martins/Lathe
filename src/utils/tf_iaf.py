from math import log

'''
Weight Schemes recommended by Baeza-Yates

| Scheme | TF Weight                | IDF Weight        |
|--------|--------------------------|-------------------|
| 0      | raw frequency            | inverse frequency |
| 1      | log normalization        | unary             |
| 2      | log normalization        | inverse frequency |
| 3      | double normalization 0.5 | inverse frequency |

'''

def calculate_tf(weight_scheme,frequency,max_frequency=None):
    if weight_scheme == 0:
        # Raw Frequency
        return frequency
    if weight_scheme == 3:
        # Double Normalization 0.5
        return 0.5 + 0.5*frequency/max_frequency
    # Log Normalization
    return 1+log(frequency)

def calculate_inverse_frequency(num_total_attributes,num_attributes_with_this_word):
    return log(num_total_attributes/num_attributes_with_this_word)

def calculate_iaf(weight_scheme,inverse_frequency=None):
    if weight_scheme == 1:
        # Unary
        return 1
    # Inverse Frequency
    return inverse_frequency
    