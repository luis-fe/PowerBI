import pandas as pd


def RecessoGeral(data_inicio,data_fim):
    # Converter as strings de data para o formato DateTime brasileiro
    data_inicio = pd.to_datetime(data_inicio, format='%d/%m/%Y')
    data_fim = pd.to_datetime(data_fim, format='%d/%m/%Y')

    # Gerar a sequência de datas
    datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')

    # Criar um DataFrame com as datas
    df = pd.DataFrame({'data': datas})
    # Formatar as datas no estilo brasileiro
    df['data'] = df['data'].dt.strftime('%d/%m/%Y')

    df['Recesso'] = 'sim'

    return df

def gerar_dataframe(data_inicio, data_fim):
    # Converter as strings de data para o formato DateTime brasileiro
    data_inicio = pd.to_datetime(data_inicio, format='%d/%m/%Y')
    data_fim = pd.to_datetime(data_fim, format='%d/%m/%Y')

    # Gerar a sequência de datas
    datas = pd.date_range(start=data_inicio, end=data_fim, freq='D')

    # Criar um DataFrame com as datas
    df = pd.DataFrame({'data': datas})

    # Adicionar a coluna 'dia_da_semana'
    df['dia_da_semana'] = df['data'].dt.day_name()

    # Formatar as datas no estilo brasileiro
    df['data'] = df['data'].dt.strftime('%d/%m/%Y')

    Recesso24 = RecessoGeral('18/12/2023','02/01/2024')
    Feriado1 = RecessoGeral('01/11/2023', '02/11/2023')
    Carnaval = RecessoGeral('12/02/2024', '13/02/2024')
    Pascoa = RecessoGeral('29/03/2024', '29/03/2024')

    Recesso24 = pd.concat([Recesso24, Feriado1, Carnaval, Pascoa], ignore_index=True)


    df = pd.merge(df, Recesso24, on='data', how='left')





    df.fillna('-', inplace=True)

    df['Dia Util?'] = 1
    df['Dia Util?'] = df.apply(lambda row: 0 if row['Recesso'] == 'sim'
    else row['Dia Util?'], axis=1)

    # Adicionar a coluna 'Dia Util?'
    df['Dia Util?'] = df.apply(lambda row: 0 if row['dia_da_semana'] == 'Sunday' or
                                                row['dia_da_semana'] == 'Saturday'
    else row['Dia Util?'], axis=1)

    df['Indice'] = df['Dia Util?'].cumsum()

    df.to_csv('Feriados.csv')


    return df

# Exemplo de uso
data_inicio = '30/01/2023'
data_fim = '10/01/2025'

resultado = gerar_dataframe(data_inicio, data_fim)
print(resultado)
