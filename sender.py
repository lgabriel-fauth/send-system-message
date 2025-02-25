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
        except:
            input('Erro ao conectar no banco de dados, Verifique os dados.')
        return conn

    def get_config(self):
        try:
            conf = json.loads(open('config.json', 'r').read())
            return conf
        except:
            input('Erro ao localizar CONFIG.JSON\nAplicativo será fechado!\ncod 000')

    def get_messages_to_send(self):
        try:
            sql = open('query.sql', 'r').read()
        except:
            input('Erro ao localizar query.sql\nAplicativo será fechado!')
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

        for msg in self.set_messages():
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
