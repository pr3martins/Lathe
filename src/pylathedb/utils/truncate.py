def truncate(text,size=50):
    if len(text)<50:
        return text
    return text[:size-3]+'...'