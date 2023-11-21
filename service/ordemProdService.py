import pandas
import pandas as pd

import ConexaoCSW

def OrdemProd(empresa):
    conn = ConexaoCSW.Conexao()
    consulta = pd.read_sql('SELECT codLote, numeroOP, codProduto, codTipoOP,  codFaseAtual '
                          ' from tco.OrdemProd op '
                          " WHERE op.codEmpresa = 1 and op.codLote like '23%' and situacao in (2, 3)",conn)
    conn.close()
    return consulta

print(OrdemProd('1'))