Esse código cria uma pipeline de consulta aos dados do Site B3 (negociação de ações/ dia).
Consulta diariamente o volume de transações das ações e salva em um parquet particionado.
É um código que inicialmente foi criado para rodar localmente mas com pequenas adaptações pode ser transcrito em uma conta AWS para criar um job de processamento.

As adaptações estão nesse mesmo repositótio mas no arquivo code02.py
