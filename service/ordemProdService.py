import pandas as pd

import ConexaoCSW

def OrdemProd(empresa):
    conn = ConexaoCSW.Conexao()
    empresa = "'"+empresa+"'"
    consulta = pd.read_sql('SELECT codLote, numeroOP, codProduto, codTipoOP,  codFaseAtual, situacao, codSeqRoteiroAtual '
                          ' from tco.OrdemProd op '
                          " WHERE op.codEmpresa = "+ empresa+
                           " and "
                           " (op.codLote like '23%' or op.codLote like '24%'  ) "
                           "and situacao in (2, 3)",conn)

    consulta2 = pd.read_sql('SELECT codLote, numeroOP, codProduto, codTipoOP,  codFaseAtual '
                          ' from tco.OrdemProd op '
                                                    " WHERE op.codEmpresa = "+ empresa+
                            " and "
                           " (op.codLote not like '23%' and op.codLote not like '24%'  ) "
                           "and situacao in (3)",conn)

    consulta = pd.concat([consulta, consulta2], ignore_index=True)
    consulta.fillna('-', inplace=True)

    conn.close()
    return consulta

def RoteiroOP(dataframeLOTE):

    dataframeLOTE = dataframeLOTE[['codLote']]
    dataframeLOTE = dataframeLOTE.drop_duplicates(subset=['codLote'])

    # Passo 3: Transformar o dataFrame em lista
    resultado = '({})'.format(', '.join(["'{}'".format(valor) for valor in dataframeLOTE['codLote']]))

    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql('SELECT r.numeroOP , r.codSeqRoteiro , r.codFase FROM tco.RoteiroOP r '
                           'WHERE r.codempresa = 1 and  '
                           'r.codLote  in '+ resultado,conn)
    conn.close()
    return consulta


def MovimentoQuantidade(empresa):
    conn = ConexaoCSW.Conexao()

    get = pd.read_sql('SELECT op.numeroOP , op.codItem, seqTamanho  FROM TCO.OrdemProdTamanhos op '
                      "WHERE op.codEmpresa = " + empresa + "and (op.codLote  like '23%' or op.codLote  like '24%'  )", conn)
    return get


def ConjuntodeOP(empresa):

    ordemprod = OrdemProd(empresa)
    roteiro = RoteiroOP(ordemprod)
    conjunto = pd.merge(ordemprod,roteiro,on='numeroOP')
    conjunto['codFase'] = conjunto['codFase'].replace('-','0')
    conjunto['codFase'] = conjunto['codFase'].astype(str)
    conjunto['codFaseAtual'] = conjunto['codFaseAtual'].astype(str)
    conjunto['statusMovimento'] = conjunto.apply(lambda row: 'movimentado' if row['situacao'] == '2' else '-',
                                      axis=1)



    conjunto['codSeqRoteiroAtual'] = conjunto['codSeqRoteiroAtual'].replace('-','0')
    conjunto['codSeqRoteiroAtual'] = conjunto['codSeqRoteiroAtual'] .astype(int)
    conjunto['codSeqRoteiro'] = conjunto['codSeqRoteiro'] .astype(int)

    conjunto['statusMovimento'] = conjunto.apply(lambda row: 'em processo' if row['statusMovimento'] == '-' and row['codSeqRoteiroAtual'] ==row['codSeqRoteiro'] else row['statusMovimento'],
                                      axis=1)
    #conjunto['statusMovimento'] = conjunto.apply(lambda row: 'movimentado' if row['statusMovimento'] == '-' and row['codSeqRoteiroAtual'] > row['codSeqRoteiro'] else '-',
     #                                 axis=1)
    #conjunto['statusMovimento'] = conjunto.apply(lambda row: 'na fila' if row['statusMovimento'] == '-' and row['codSeqRoteiroAtual'] < row['codSeqRoteiro'] else '-',
     #                                 axis=1)

    Quantidde = MovimentoQuantidade('1')
    conjunto = pd.merge(conjunto,Quantidde,on='numeroOP', how='left')

    conjunto['codFase'] = conjunto.apply(lambda row: DeParaFases(row['codFase']),
                                      axis=1)
    nomeFase = NomeFase()
    conjunto = pd.merge(conjunto,nomeFase,on='codFase')

    conjunto1 = conjunto.loc[:1000000]
    conjunto1.to_csv('conjuntoOP_1.csv')

    conjunto2 = conjunto.loc[1000000:]
    conjunto2.to_csv('conjuntoOP_2.csv')

    print(conjunto)

def DeParaFases(faseAntes):
    if faseAntes == '429' :
        return '55'
    elif faseAntes == '413' :
        return '70'
    else:
        return faseAntes

def NomeFase():
    conn = ConexaoCSW.Conexao()

    get = pd.read_sql('SELECT f.codFase , f.nome as nomeFase  FROM tcp.FasesProducao f '
                      "WHERE f.codEmpresa = 1", conn)
    get['codFase'] = get['codFase'].astype(str)
    return get

print(ConjuntodeOP('1'))