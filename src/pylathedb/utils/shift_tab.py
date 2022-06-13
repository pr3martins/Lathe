def shift_tab(text,sep='\t'):
  return f'{sep}{text}'.replace('\n',f'\n{sep}')