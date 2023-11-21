import pandas as pd

import ConexaoCSW

def OrdemProd(empresa):
    conn = ConexaoCSW.Conexao()
    empresa = "'"+empresa+"'"
    consulta = pd.read_sql('SELECT codLote, numeroOP, codProduto, codTipoOP,  codFaseAtual '
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
    conjunto['statusMovimento'] = conjunto.apply(lambda row: 'em processo' if row['codFaseAtual'] == row['codFase'] else '-',
                                      axis=1)
    Quantidde = MovimentoQuantidade('1')
    conjunto = pd.merge(conjunto,Quantidde,on='numeroOP', how='left')

    conjunto1 = conjunto.loc[:1000000]
    conjunto1.to_csv('conjuntoOP_1.csv')

    conjunto2 = conjunto.loc[1000000:]
    conjunto2.to_csv('conjuntoOP_2.csv')

    print(conjunto)

print(ConjuntodeOP('1'))