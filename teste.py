import pandas as pd

# Criando o DataFrame a partir dos dados fornecidos
data = {'col1': [1, 2, 3], 'col3': [['p', 'm'], ['p', 'm'], ['p', 'm']], 'col2': [["0/1", "1/3"], ["0/1", "0/1"], ["11/12", "1/3"]]}
df = pd.DataFrame(data)

# Função para calcular o subtotal com base no padrão "x/y"
def calcular_subtotal(lista_padroes):
    subtotais = [sum(map(int, padrao.split('/'))) for padrao in lista_padroes]
    total = sum(subtotais)
    return total

# Calculando os subtotais para cada linha
df['subtotal'] = df['col2'].apply(calcular_subtotal)

# Adicionando 'total' à coluna3
df['col3'] = df['col3'].apply(lambda x: x + ['total'])

# Adicionando os subtotais a col2
df['col2'] = df.apply(lambda row: [f"{s}/{row['subtotal']}" for s in row['col2']], axis=1)

# Removendo a coluna de subtotal
df = df.drop('subtotal', axis=1)

# Convertendo as listas de strings para strings
df['col3'] = df['col3'].apply(lambda x: ', '.join(x))
df['col2'] = df['col2'].apply(lambda x: ', '.join(x))

# Exibindo o DataFrame após as operações
print(df[['col1', 'col3', 'col2']])
