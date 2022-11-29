import requests as r
import json
import fdb

class Sender():
    def __init__(self):
        self.token = 'faf66a0b-9ff1-4b55-ad2f-ab46381cb9be'
        self.url = 'https://api-base.whatstalk.com.br/api/messages/'
        self.send_to = self.config()['send_to']
        self.messages_to_send = self.get_messages_to_send()

    def connDb(self):
        conn = fdb.connect(
            host='127.0.0.1',
            database='C:/Ultra/Banco/Gestao - Energia Solar.fdb',
            user='SYSDBA',
            password='masterkey',
            port=3050,
            charset='UNICODE_FSS'
        )
        return conn

    def set_trigger(self):
        conn = self.connDb()
        cur = conn.cursor()
        cur.execute('''
        SET TERM ^ ;
        CREATE OR ALTER TRIGGER update_to_send_mesg FOR projetos_tarefas_apontamentos
        ACTIVE
        BEFORE UPDATE or INSERT
        POSITION 0
        AS

        BEGIN
            if (new.idn_reaberta = false and new.idn_concluido = true) then
                new.enviar = true;
        END^
        SET TERM ; ^
        ''')
        conn.commit()
        conn.close()

    def set_field(self):
        conn = self.connDb()
        cur = conn.cursor()
        cur.execute('''
        ALTER TABLE PROJETOS_TAREFAS_APONTAMENTOS
        ADD ENVIAR BOOLEAN
        ''')
        conn.commit()
        conn.close()

    def config(self):
        conf = json.loads(open('config.json', 'r').read())
        return conf

    def get_messages_to_send(self):
        conn = self.connDb()
        cur = conn.cursor()
        cur.execute('''
        select
            dime.id as dime_id,
            pj.projeto_id, pj.dscprojeto,
            f.id as fase_id, f.fase,
            pjf.percentual_representacao,
            t.id as tarefa_id, t.tarefa,
            pjt.percentual_representacao,
            osc.descricao,
            osci.descricao_item as pergunta,
            ptc.resposta,
            pta.operador as operador_id,
            ope.nome as operador,
            pta.responsavel as responsavel_id,
            resp.nome as responsavel,
            pta.idn_concluido, 
            pta.idn_reaberta,
            pta.enviar

        from projetos pj 
            left join fvt_dimensionamento dime on dime.obra_id = pj.parceiros_obra
            left join projetos_fases pjf on pjf.id_projeto = pj.projeto_id
            left join fases f on f.id = pjf.id_fase
            left join projetos_tarefas pjt on pjt.id_projeto_fase = pjf.id
            left join tarefas t on t.id = pjt.id_tarefa
            left join tarefas_checklist tc on tc.id_tarefa = t.id
            left join os_check_list osc on osc.check_list = tc.id_checklist
            left join os_check_list_itens osci on osci.check_list = osc.check_list and osci.descricao_item = tc.id_checklist_descricao_item
            left join projetos_tarefas_check ptc on ptc.id_projeto_tarefa = pjt.id and ptc.id_check_list = osc.check_list and ptc.id_check_list_descricao_item = osci.descricao_item
            left join projetos_tarefas_apontamentos pta on pta.id_projeto_tarefa = pjt.id and pta.idn_concluido = true and pta.idn_reaberta = false
            left join operadores ope on ope.operador = pta.operador
            left join parceiros resp on resp.parceiro = pta.responsavel

        where
            pta.enviar = true and
            pta.idn_reaberta = false
        ''')
        data = []
        for c in cur.fetchallmap():
            data.append(dict(c))
        return data

    def send_message(self):
        for mes in self.messages_to_send:
            req = r.post(url=self.url,
                headers={
                    "Content-Type": "application/json", 
                    "Accept": "application/json", 
                    "Authorization": f"Bearer {self.token}"
                },
                json={
                    "sender_phone": "556784540339",
                    "phone": "556798316369",
                    "type": "TEXT", 
                    "text_content": f"""*STATUS PROJETOS - DELTA SISTEMAS* ```
Dimensionamento: {self.status[0]['DIME_ID']}
Projeto:         {self.status[0]['PROJETO_ID']}
Fase:            {self.status[0]['FASE']}
Tarefa:          {self.status[0]['TAREFA']}
```"""
                })
            return json.loads(req.content)

Sender().send_message()
