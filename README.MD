# Configurações para rodar
## Configurações importantes
- Instalar Firebird (x64)
- A pasta com os dados da aplicação de envio podem se localizar em qualquer pasta do computador
### em QUERY.SQL
- SEND_TO_NUM: coluna informando o número do contato que irá receber a mensagem
- SEND_TO_NUM deve seguir a formatação conforme exemplo: 556799991111
### em CONFIG.JSON
- DELIMITER: idenfica a separação usada no MODELO.TXT para inclusão das várias do SQL
- HOUR_TO_SEND e MIN_TO_SEND identificam o horário do dia de envio
- TOKEN: representa o token gerando na plataforma em Configureções>Funcionalidades>Token
- URL: endpoint que irá receber as requisições
- SENDER_PHONE: número da integração que vai disparar as mensagens

- DATABASE: dados para acesso ao banco de dados
- Caminho para o banco de dados deve conter apenas "/" ou "\\". Caso informe "\" irá gerar erro
### MODELO.TXT
- Caso o envio das mensagens gere caracteres diferente do informado na mensagem. Realizar conversão do arquivo
  no NOTEPAD++ para outro formato de texto, exemplo utf-8 -> ascii

## Após realizado o processo deve funcionar devidamente