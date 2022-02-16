# Dados Abertos da ANS
Diretório destinado a trabalhos referentes aos dados abertos da Agência Nacional de Saúde Suplementar (ANS) disponíveis no repositório "dados.gov.br".

Observações:

O script foi feito para armazenar os dados em um banco de dados SQL, dessa forma, certifique-se de que você tem o SQL instalado e saiba manuseá-lo para evitar problemas.

Para a primeira conexão (objeto "con") você precisará definir qual o database que será usado, então esteja certo de que você criou o database e inseriu o nome, assim como demais informações de conexão, corretamente.

No script "Criando as Bases de Dados.R" tenha atenção para a tabela referente a "Característica dos planos". Cada vez que a ANS atualiza os dados no repositório aparecem erros diferentes, então tenha cuidado ao rodas a parte 03 do script citado. Aconselho que rode a leitura dos dados e veja se aparece algum erro na tela de console, geralmente o R retorna a linha que está com defeito ou gerando dificuldades na leitura.
