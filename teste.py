
import datetime
import pytz
import locale


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    dia = agora.strftime('%Y-%m-%d')
    return hora_str, dia


def EncontrandoMesAtual():
    datahora, dia = obterHoraAtual()
    mes = dia[5:7]

    if mes == '01':
        return '01', '01'

    else:
        mesAtual = int(mes)
        mesFinal = mesAtual - 1
        mesFinal = str(mesFinal)

        return mesFinal, mes

print(EncontrandoMesAtual())