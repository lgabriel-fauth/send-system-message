select
    p.nome as NOME,
    P.celular as TELEFONE,
    '6798316369' as send_to_num,
    os.obs as OBS,
    os.data_encerramento as DATA
from os
    left join parceiros p on (p.parceiro = os.parceiro)
WHERE 
    os.OS = '8561'