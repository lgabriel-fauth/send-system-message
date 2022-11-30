import requests as r
import json
import fdb
import time

class Sender():
    def __init__(self):
        self.config = self.get_config()

    def connDb(self):
        try:
            conn = fdb.connect(
                host=self.config['database']['host'],
                database=self.config['database']['database'],
                user=self.config['database']['user'],
                password=self.config['database']['password'],
                port=int(self.config['database']['port']),
                charset=self.config['database']['charset']
            )
        except:
            input('Erro ao conectar no banco de dados, Verifique os dados.')
        return conn

    def set_trigger(self):
        sql = '''
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
        '''
        conn = self.connDb()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        except:
            input('Atualizou as tabelas?\nAplicativo será fechado!')
        finally:
            conn.close()

    def set_field(self):
        sql = '''
            ALTER TABLE PROJETOS_TAREFAS_APONTAMENTOS
            ADD ENVIAR BOOLEAN
            ADD ENVIADO BOOLEAN
        '''
        conn = self.connDb()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def get_config(self):
        try:
            conf = json.loads(open('config.json', 'r').read())
            return conf
        except:
            input('Erro ao localizar CONFIG.JSON\nAplicativo será fechado!\ncod 000')

    def get_messages_to_send(self):
        sql = '''
        select
            dime.id as dime_id,
            vend.parceiro as vendedor_id,
            rtrim(vend.celular) as vendedor_num,
            vend.nome as vendedor,
            pj.projeto_id, pj.dscprojeto,
            f.id as fase_id, f.fase,
            pjf.percentual_representacao,
            t.id as tarefa_id, t.tarefa,
            pjt.id as projeto_tarefa_id,
            pjt.percentual_representacao,
        --    osc.descricao,
        --    osci.descricao_item as pergunta,
        --    ptc.resposta,
            pta.operador as operador_id,
            ope.nome as operador,
            pta.responsavel as responsavel_id,
            resp.nome as responsavel,
            pta.id as apontamento_id,
            pta.idn_concluido, 
            pta.idn_reaberta,
            pta.enviar

        from projetos pj 
            left join fvt_dimensionamento dime on dime.id_projeto = pj.projeto_id
            left join projetos_fases pjf on pjf.id_projeto = pj.projeto_id
            left join fases f on f.id = pjf.id_fase
            left join projetos_tarefas pjt on pjt.id_projeto_fase = pjf.id
            left join tarefas t on t.id = pjt.id_tarefa
        --    left join tarefas_checklist tc on tc.id_tarefa = t.id
        --    left join os_check_list osc on osc.check_list = tc.id_checklist
        --    left join os_check_list_itens osci on osci.check_list = osc.check_list and osci.descricao_item = tc.id_checklist_descricao_item
        --    left join projetos_tarefas_check ptc on ptc.id_projeto_tarefa = pjt.id and ptc.id_check_list = osc.check_list and ptc.id_check_list_descricao_item = osci.descricao_item
            left join projetos_tarefas_apontamentos pta on pta.id_projeto_tarefa = pjt.id and pta.idn_concluido = true and pta.idn_reaberta = false
            left join operadores ope on ope.operador = pta.operador
            left join parceiros resp on resp.parceiro = pta.responsavel
            left join parceiros vend on vend.parceiro = dime.vendedor

        where
            pta.enviar = true and
            ((pta.enviado = false) or (pta.enviado is null)) and
            pta.idn_reaberta = false
        '''
        conn = self.connDb()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            data = []
            for c in cur.fetchallmap():
                data.append(dict(c))
        except:
            input('Atualizou as tabelas?\nAplicativo será fechado!\ncod 001')
        finally:
            conn.close()
            return data

    def perc_project(self, dime):
        sql = f'''
            select
                (select
                    count(pjt.id)
                
                from projetos pj 
                    left join fvt_dimensionamento dime on dime.obra_id = pj.parceiros_obra
                    left join projetos_fases pjf on pjf.id_projeto = pj.projeto_id
                    left join fases f on f.id = pjf.id_fase
                    left join projetos_tarefas pjt on pjt.id_projeto_fase = pjf.id
                    left join tarefas t on t.id = pjt.id_tarefa
                
                where
                    dime.id = {dime}) as total,

                (select
                    count(pjt.id)
                
                from projetos pj 
                    left join fvt_dimensionamento dime on dime.obra_id = pj.parceiros_obra
                    left join projetos_fases pjf on pjf.id_projeto = pj.projeto_id
                    left join fases f on f.id = pjf.id_fase
                    left join projetos_tarefas pjt on pjt.id_projeto_fase = pjf.id
                    left join tarefas t on t.id = pjt.id_tarefa
                    left join projetos_tarefas_apontamentos pta on pta.id_projeto_tarefa = pjt.id
                
                where
                    dime.id = {dime} and
                    pta.idn_concluido = true) as concluido

            from (select first 1 * from filiais)
        '''
        conn = self.connDb()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchallmap()
        except:
            input('Atualizou as tabelas?\nAplicativo será fechado!\ncod 002')
        finally:
            conn.close()
            return data

    def send_message(self):
        for mes in self.get_messages_to_send():
            print(mes)
            progress = self.perc_project(dime=mes["DIME_ID"])[0]
            try: 
                progress = round(int(progress['CONCLUIDO'])/int(progress['TOTAL'])*100, 2)
            except:
                progress = 'N/A'
            message = f"""*STATUS PROJETOS - DELTA SISTEMAS* ```
Dimen.:   {mes['DIME_ID']}
Proj.:    {mes['PROJETO_ID']}
Fase:     {mes['FASE']}
Tarefa:   {mes['TAREFA']}

Progresso do Projeto:``` *{progress}%*
"""      
            
            print(mes['VENDEDOR_NUM'],mes['VENDEDOR_ID'])
            req = r.post(url=self.config['url'],
                headers={
                    "Content-Type": "application/json", 
                    "Accept": "application/json", 
                    "Authorization": f"Bearer {self.config['token']}"
                },
                json={
                    "sender_phone": f"{self.config['sender_phone']}",
                    "phone": f"{mes['VENDEDOR_NUM']}",
                    "type": "TEXT", 
                    "text_content": str(message)
                })
            print(req.status_code, req.content)

            if req.status_code == 201 or req.status_code == 200:
                conn = self.connDb()
                cur = conn.cursor()
                cur.execute(f'''
                    UPDATE PROJETOS_TAREFAS_APONTAMENTOS SET
                    ENVIADO = true
                    WHERE ID = {mes['APONTAMENTO_ID']}
                ''')
                conn.commit()
                conn.close()

# Sender().send_message()
if __name__ == '__main__':
    while True:
        Sender().send_message()
        time.sleep(10)
