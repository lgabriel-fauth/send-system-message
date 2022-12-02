import requests as r
import json
import fdb
import time
from datetime import datetime

class Sender():
    def __init__(self):
        self.config = self.get_config()
        self.delimiter = self.config['delimiter']

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
            conn.execute_immediate(sql)
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
        try:
            sql = open('proj_to_send.sql', 'r').read()
        except:
            input('Erro ao localizar proj_to_send.sql\nAplicativo será fechado!')
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
    
    def set_messages(self):
        messages = []
        for msg in self.get_messages_to_send():
            modelo = open('modelo.txt', 'r').read().split(self.delimiter)
            message = ''
            for id, m in enumerate(modelo):
                if id%2 == 1:
                    print(m)
                    message += str(msg[m])
                else:
                    message += m
            messages.append({ 'text': message, 'send_to_num': msg['SEND_TO_NUM'], 'apontamento': msg['APONTAMENTO_ID']})
        return messages

    def send_message(self):
        for msg in self.set_messages():
            print(msg)
            
            req = r.post(url=self.config['url'],
                headers={
                    "Content-Type": "application/json", 
                    "Accept": "application/json", 
                    "Authorization": f"Bearer {self.config['token']}"
                },
                json={
                    "sender_phone": f"{self.config['sender_phone']}",
                    "phone": f"{msg['send_to_num']}",
                    "type": "TEXT", 
                    "text_content": str(msg['text'])
                })
            print(req.status_code, req.content)

            if req.status_code == 201 or req.status_code == 200:
                conn = self.connDb()
                cur = conn.cursor()
                cur.execute(f'''
                    UPDATE PROJETOS_TAREFAS_APONTAMENTOS SET
                    ENVIADO = true
                    WHERE ID = {msg['apontamento']}
                ''')
                conn.commit()
                conn.close()

# Sender().send_message()
if __name__ == '__main__':
    while True:
        Sender().send_message()
        # Sender().set_messages()
        time.sleep(10)
