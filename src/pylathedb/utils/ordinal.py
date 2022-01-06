def ordinal(n):
    return f'{n}{"tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4]}'