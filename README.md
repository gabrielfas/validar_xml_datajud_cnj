# Validar envio XML DATAJUD-CNJ
Projeto criado para validar o XML de processos enviados ao CNJ usando a API de validação do DATAJUD.

Serão validados arquivos XML de processos no validador do próprio CNJ, afim de validar com relação a estrutura do XML, assim como os valores semânticos dos parâmetros passados nas tags.

## Como executar

Primeiramente é necessário acessar a pasta "config" e inserir nos arquivos .txt as configuarações do seu ambiente.

* caminho.txt - Colocar o caminho onde estão os arquivos .xml
* credenciais.txt - Colocar as credenciais do banco onde serão inseridos os dados da validação, no seguinte padrão *USUARIO;SENHA;SERVIDOR;BANCO*
* reprocessar.txt - Preencher apenas caso queria reprocessar algum processo, senão, deixar vazio mesmo. Caso queira reprocessar, basta colocar os números dos processos separados pela quebra de linha

Realizada a configuração dos arquivos, basta executar, o arquivo. É possivel passar como parâmetro no comando de execução o valor 1, caso queira deletar todos os dados inseridos nas tabelas do banco que irão armazenar o retorno da validação, caso não passe nada o programa irá inserir junto aos dados já existentes nas tabelas.

## Observações

* Os arquivos XML devem seguir o seguinte padrão de nomenclatura *TRIBUNAL_GRAU_CODCLASSE_SISTEMA_NUMEROPROCESSO.xml*.
* Schema e tabela do banco estão pré-inseridos no código, basta alterar caso queira mudar.
* O mesmo vale para o endereço do validador, está configurado para o validador do CNJ, caso queira mudar para um link interno, basta alterar o código.
