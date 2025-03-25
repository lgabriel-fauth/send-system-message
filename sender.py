import requests as r
import json
import fdb
import time

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
        except Exception as e:
            self.add_new_log(f'Erro ao conectar no banco de dados, Verifique os dados. \n {e}')
        return conn

    def get_config(self):
        try:
            with open('config.json', 'r') as f:
                conf = json.loads(f.read())
            return conf
        except Exception as e:
            self.add_new_log(f'Erro ao localizar CONFIG.JSON \n {e}')
            

    def add_new_log(self, log):
        try:
            with open('logs.json', 'r') as f:
                logs: dict = json.loads(f.read())
        except FileNotFoundError as e:
            logs = {}
        
        key = time.strftime('%H:%M:%S')
        logs[key] = log

        with open('logs.json', 'w') as f:
            json.dump(logs, f, indent=2)

    def get_messages_to_send(self):
        try:
            with open('query.sql', 'r') as f:
                sql = f.read()
        except:
            self.add_new_log(f'Erro ao localizar QUERY.SQL \n {e}')
        conn = self.connDb()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            data = []
            for c in cur.fetchallmap():
                data.append(dict(c))
        except Exception as e:
            self.add_new_log(f'Erro ao executar SQL \n {e}')
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
                    message += str(msg[m])
                else:
                    message += m
            messages.append({ 'text': message, 'send_to_num': msg['SEND_TO_NUM'] })
        return messages
    
    def verify_to_send_message(self):
        horario_atual = time.strftime('%H:%M:%S').split(':')
        if (
            self.config['hour_to_send'] == horario_atual[0] and
            self.config['min_to_send'] == horario_atual[1]
        ):
            return True
        return False

    def send_message(self):
        if not self.verify_to_send_message():
            return

        self.add_new_log("Antes do set_messages")
        for msg in self.set_messages():
            self.add_new_log(json.dumps({
                    "Content-Type": "application/json", 
                    "Accept": "application/json", 
                    "Authorization": f"Bearer {self.config['token']}"
                }))
            self.add_new_log(json.dumps([
                    {
                        "sender_phone": f"{self.config['sender_phone']}",
                        "phone": f"{msg['send_to_num']}",
                        "type": "TEXT", 
                        "text_content": str(msg['text'])
                    }
                ]))
            
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
                }
            )
            if (req.status_code == 201):
                self.add_new_log(f'Message sent to {msg['send_to_num']}')
            else:
                self.add_new_log(req.json()['message'])
        time.sleep(60)

###### INTERFACE ######
import tkinter as tk
from time import strftime

# Função para atualizar o horário
def atualizar_horario():
    horario_atual = strftime('%H:%M:%S')  # Formato de 24 horas
    label_relogio.config(text=horario_atual)
    Sender().send_message()
    label_relogio.after(1000, atualizar_horario)  # Atualiza a cada 1 segundo

# Configuração da janela principal
janela = tk.Tk()
janela.title("Automação mensagens")
janela.geometry("300x100")  # Tamanho da janela
janela.resizable(False, False)  # Impede o redimensionamento da janela

# Configuração do rótulo (label) para exibir o horário
label_relogio = tk.Label(
    janela,
    font=('Arial', 40, 'bold'),  # Fonte e tamanho
    background='black',  # Cor de fundo
    foreground='white'   # Cor do texto
)
label_relogio.pack(expand=True, fill='both')  # Centraliza o rótulo na janela

# Inicia a atualização do horário
atualizar_horario()

# Inicia o loop principal da interface
janela.mainloop()
