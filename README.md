# convest

Trabalho desenvolvido durante o processo seletivo da Convest Consultoria de Investimentos Ltda.

Para que a aplicação funcione, deve-se inserir somente sua API-KEY da Alpha Vantage no arquivo "KEY.txt".

Condiões: 
    * Se o ativo estiver como habilitado em TRUE, a importação será executada;
    * A aplicação criará duas tabelas, caso não existam, dos ativos B3SA3 e PETR4;
    * Caso já exista uma tabela para cada ativo a aplicação fará um UPDATE nos valores do preço (close) nas datas já existentes e INSERT de todos os campos se ainda não houver a data.
