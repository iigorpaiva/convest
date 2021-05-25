## Convest

Para que a aplicação funcione, deve-se inserir somente sua API-KEY da Alpha Vantage no arquivo "KEY.txt".

## Condições: 

- `Se o ativo estiver como habilitado em TRUE, a importação será executada;`
- `A aplicação criará uma ou duas tabelas dos ativos B3SA3 e PETR4, caso não existam;`
- `Se já existe uma tabela para cada ativo a aplicação executa o UPDATE nos valores do preço (close) nas datas já existentes e executa o INSERT de todos os campos se ainda não houver a data.`