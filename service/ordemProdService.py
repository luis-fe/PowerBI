import pandas as pd
import locale

import ConexaoCSW

def OrdemProd(empresa):
    conn = ConexaoCSW.Conexao()
    empresa = "'"+empresa+"'"
    consulta = pd.read_sql('SELECT codEmpresa, codLote, numeroOP, codProduto as Engenharia, codTipoOP as TipoOP,  codFaseAtual, situacao, codSeqRoteiroAtual,'
                           ' (select lt.descricao  from tcl.Lote lt WHERE lt.codEmpresa = 1  and op.codLote = lt.codLote) as desc_lote '
                          ' from tco.OrdemProd op '
                          " WHERE op.codEmpresa = "+ empresa+
                           " and "
                           " (op.codLote like '23%' or op.codLote like '24%'  ) "
                           "and situacao in (2, 3)",conn)

    consulta2 = pd.read_sql('SELECT codEmpresa, codLote, numeroOP, codProduto as Engenharia, codTipoOP as TipoOP,  codFaseAtual, situacao,  codSeqRoteiroAtual '
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

def RoteiroEngeharia(empresa):
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql("SELECT  r.codEngenharia as Engenharia, r.seqProcesso as codSeqRoteiro, r.codFase  FROM tcp.ProcessosEngenharia r "
                           "WHERE r.codEmpresa = 1 and r.codEngenharia like '%-0'",conn)
    consulta['numeroOP'] = '-'

    conn.close()
    return consulta

def MovimentoQuantidade(empresa):
    conn = ConexaoCSW.Conexao()

    get = pd.read_sql('SELECT op.numeroOP , seqTamanho, codSortimento as Sortimento, op.qtdePecas1Qualidade as qual_1T  FROM TCO.OrdemProdTamanhos op '
                      "WHERE op.codEmpresa = " + empresa + "and (op.codLote  like '23%' or op.codLote  like '24%' or op.codLote  like '22%'  )", conn)


    return get


def ConjuntodeOP(empresa):

    ordemprod = OrdemProd(empresa)
    roteiro = RoteiroOP(ordemprod)
    conjunto = pd.merge(ordemprod,roteiro,on='numeroOP')
    conjunto['codFase'] = conjunto['codFase'].replace('-','0')
    conjunto['codFase'] = conjunto['codFase'].astype(str)
    conjunto['codEmpresa'] = conjunto['codEmpresa'].astype(str)

    conjunto['codFaseAtual'] = conjunto['codFaseAtual'].astype(str)
    conjunto['Roteiro Status'] = conjunto.apply(lambda row: 'movimentado' if row['situacao'] == '2' else '-',
                                      axis=1)



    conjunto['codSeqRoteiroAtual'] = conjunto['codSeqRoteiroAtual'].replace('-','0')
    conjunto['codSeqRoteiroAtual'] = conjunto['codSeqRoteiroAtual'] .astype(int)
    conjunto['codSeqRoteiro'] = conjunto['codSeqRoteiro'] .astype(int)

    conjunto['Roteiro Status'] = conjunto.apply(lambda row: 'Em Produção' if row['Roteiro Status'] == '-' and row['codSeqRoteiroAtual'] ==row['codSeqRoteiro'] else row['Roteiro Status'],
                                      axis=1)
    conjunto['Roteiro Status'] = conjunto.apply(lambda row: 'Movimentado' if row['Roteiro Status'] == '-' and row['codSeqRoteiroAtual'] > row['codSeqRoteiro'] else row['Roteiro Status'],
                                      axis=1)
    conjunto['Roteiro Status'] = conjunto.apply(lambda row: 'A Produzir' if row['Roteiro Status'] == '-' and row['codSeqRoteiroAtual'] < row['codSeqRoteiro'] else row['Roteiro Status'],
                                      axis=1)

    Quantidde = MovimentoQuantidade('1')


    conjunto = pd.merge(conjunto,Quantidde,on='numeroOP', how='left')




    conjunto['codFase'] = conjunto.apply(lambda row: DeParaFases(row['codFase']),
                                      axis=1)
    nomeFase = NomeFase()
    conjunto = pd.merge(conjunto,nomeFase,on='codFase')

    reduzido = LocalizandoCodReduzido()


    def format_with_separator(value):
        return locale.format('%0.0f', value, grouping=True)

    conjunto['Sortimento']  = conjunto['Sortimento'].apply(format_with_separator)

    conjunto['Sortimento'] =conjunto['Sortimento'].astype(str)


    reduzido['Sortimento'] =reduzido['Sortimento'].astype(str)
    #reduzido['Sortimento'] = reduzido['Sortimento'].str.replace('.0', '', regex=True)
    conjunto['seqTamanho'] =conjunto['seqTamanho'].astype(str)
    conjunto['seqTamanho'] = conjunto['seqTamanho'].str.replace('.0', '', regex=True)

    reduzido['seqTamanho'] =reduzido['seqTamanho'].astype(str)
    reduzido['seqTamanho'] = reduzido['seqTamanho'].str.replace('.0', '', regex=True)

    reduzido.to_csv('reduzido2.csv')
    conjunto = pd.merge(conjunto,reduzido,on=['Engenharia','Sortimento','seqTamanho'], how='left')



    movimentadas = MovimentoRoteiro(empresa)
    conjunto = pd.merge(conjunto, movimentadas, on=['numeroOP', 'Sortimento', 'seqTamanho','codSeqRoteiro'], how='left')

    conjunto['1ºqual.'] = conjunto.apply(lambda row: row['qual_1Roteiro'] if row["Roteiro Status"] == 'Movimentado' else row['qual_1T'], axis=1 )

    conjunto1 = conjunto.loc[:1000000]
    conjunto1.to_csv('conjuntoOP_1.csv')

    conjunto2 = conjunto.loc[1000000:]
    conjunto2.to_csv('conjuntoOP_2.csv')

    print(conjunto)

def DeParaFases(faseAntes):
    if faseAntes == '55' :
        return '429'
    elif faseAntes == '1' :
        return '401'
    elif faseAntes == '10' :
        return '403'
    elif faseAntes == '15' :
        return '404'
    elif faseAntes == '30' :
        return '408'
    elif faseAntes == '67' :
        return '416'
    elif faseAntes == '68' :
        return '417'
    elif faseAntes == '40' :
        return '409'
    elif faseAntes == '155' :
        return '426'

    elif faseAntes == '70' :
        return '413'
    elif faseAntes == '77' :
        return '414'
    elif faseAntes == '71' :
        return '422'
    elif faseAntes == '61' :
        return '424'
    elif faseAntes == '74' :
        return '435'
    elif faseAntes == '85' :
        return '410'
    elif faseAntes == '125' :
        return '415'
    elif faseAntes == '35' :
        return '-'
    elif faseAntes == '160' :
        return '425'

    elif faseAntes == '50' :
        return '428'

    elif faseAntes == '62' :
        return '427'
    elif faseAntes == '320' :
        return '433'
    elif faseAntes == '195' :
        return '437'
    elif faseAntes == '210' :
        return '441'
    elif faseAntes == '236' :
        return '449'
    else:
        return faseAntes

def NomeFase():
    conn = ConexaoCSW.Conexao()

    get = pd.read_sql('SELECT f.codFase , f.nome as nomeFase  FROM tcp.FasesProducao f '
                      "WHERE f.codEmpresa = 1", conn)
    get['codFase'] = get['codFase'].astype(str)
    return get

def LocalizandoCodReduzido():
    conn = ConexaoCSW.Conexao()

    get = pd.read_sql('SELECT i.codItemPai as Engenharia, i.codItem, i.codSortimento as Sortimento, codSeqTamanho as seqTamanho '
                      'FROM Cgi.Item2 i WHERE i.Empresa = 1 and i.codSortimento  > 0 '
                      "and (codItemPai like  '1%' or codItemPai like '2%' or codItemPai like '5%' or codItemPai like '6%' )", conn)

    get['Engenharia'] = get.apply(lambda row: '0' + row['Engenharia'] + '-0' if row['Engenharia'][0] == '1' or row['Engenharia'][0] == '2' else row['Engenharia'] + '-0', axis=1)

    conn.close()
    return get

def MovimentoRoteiro(empresa):
    conn = ConexaoCSW.Conexao()
    get = pd.read_sql('SELECT seqRoteiro as codSeqRoteiro, convert(varchar(11), numeroOP) as numeroOP , m.codSortimento as Sortimento , m.seqTamanho, '
                      'm.qtdePecas1Qualidade as qual_1Roteiro, m.qtdePecasProgramadas as prog, m.qtdePecas2Qualidade as qual_2Roteiro  '
                      " FROM tco.MovimentacaoOPFaseTam m "
                      "where m.codEmpresa = 1 and (m.codLote  like '23%' or m.codLote  like '24%') ",conn)
    get['Sortimento'] =get['Sortimento'].astype(str)
    get['Sortimento'] = get['Sortimento'].str.replace('.0', '', regex=True)
    get['seqTamanho'] =get['seqTamanho'].astype(str)
    get['seqTamanho'] = get['seqTamanho'].str.replace('.0', '', regex=True)

    get.fillna('-', inplace=True)

    get['qual_1Roteiro'] = get.apply(lambda row: row['prog'] if row['qual_1Roteiro'] == '-' and row['qual_2Roteiro'] == '-' else row['qual_1Roteiro'] , axis=1
                                )


    return get
print(ConjuntodeOP('1'))






