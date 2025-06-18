with todasOS6meses as (
    select
        os.os,
        substring(p.nome from 1 for position(' ' in p.nome)-1) as NOME,
        P.celular as TELEFONE,
        case
            when char_length(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')) = 13
            then substring(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '') from 3 for 13)
            when char_length(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')) = 11
            then replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')
            else replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')
--            when char_length(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')) = 13
--            then substring(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '') from 1 for 3) || substring(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '') from 5 for 9)
--            when char_length(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')) = 11
--            then substring(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '') from 1 for 3) || substring(replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '') from 5 for 9)
--            else replace(replace(replace(replace(p.celular, '-', ''), '(', ''), ')', ''), ' ', '')
        end as send_to_num,
        os.obs as OBS,
        os.data_encerramento as DATA,
        substring(os.obs from position('Placa do veículo: ' in os.obs) + 18 for 7) as placa
    from os
        left join parceiros p on (p.parceiro = os.parceiro)
    where
        os.data_encerramento = current_date - 180 and os.parceiro <> 1
)
select
    os6.nome,
    os6.telefone,
    '67998316369' as send_to_num,
    os6.data,
    '' as OBS
from
    todasOS6meses os6
where not exists (
    select 1
    from
        os osRecentes
    where
        osRecentes.obs like '%' || os6.placa || '%' and osRecentes.data_encerramento > os6.data
)