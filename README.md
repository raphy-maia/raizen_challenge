# Raízen Challenge

Neste desafio, o objetivo é abrir um arquivo, tratar e criar uma pipeline utilizando o Airflow.  


## Itens contidos neste repositório
- Colab
- Arquivo Python com as dags


## Minha jornada

1. O primeiro desafio foi encontrar uma maneira de ler o arquivo, que estava em xls. Seus dados estavam em abas ocultas em cache. Tentei abrir o arquivo de diversas formas, mas só foi possível visualizar os dados via LibreOffice.
2. Após conseguir visualizar o arquivo, utilizei o Colab para fazer uma análise inicial. Para isso, utilizei o próprio LibreOffice no editor para converter o arquivo para xlsx.
3. Durante a análise, foi possível identificar que apenas três das tabelas seriam de fato utilizadas. Assim, criei um novo DF para juntá-las.
4. Os dados foram tratados em Pandas. As principais alterações foram realizadas para adequar o dataset aos requisitos do desafio:
- Junção de 3 tabelas em um único df;
- Substituição de valores nulos por zero, a fim de evitar conflitos;
- Alterar o eixo do DF a fim de cumprir com os requisitos do desafio;
- Renomear e traduzir nomes das colunas e alguns valores;
- Dropar colunas desnecessárias;
- Reordenar colunas conforme o solicitado pelo desafio;
- Transformação de types.
- Validação do esquema.
5. Em seguida, utilizei o editor de texto Atom para transformar em funções os códigos que utilizei para tratar o arquivo. Foram criadas três funções:
- Baixar o arquivo;
- Criar os DFs das três tabelas utilizadas e concatená-los em apenas um;
- Alterar tabela: todo o tratamento realizado no Colab foi transformado em uma função.
6. Em seguida, passei a organizar o código para a criação das dags. Verifiquei as bibliotecas que seriam necessárias e as incluí no código. Em seguida, trouxe as funções.
7. Logo após, criei quatro tasks de uma dag para chamar essas funções:
- A primeira para baixar o arquivo;
- A segunda para usar um container para rodar o LibreOffice e converter o arquivo;
- A terceira para criar e concatenar o DF;
- A quarta para tratar o arquivo.


## Conclusões

Após a consolidação do arquivo, utilizei a GCP para criar um projeto ('raizen'). Nele, subi o Composer (que é o Airflow gerenciado da Google). Em seguida, subi a dag em um bucket para ser executado no Composer.

![Composer](./assets/criacao_composer.png 'Criação do Composer')

![Composer](./assets/Composer_criado.png 'Composer criado')

![Composer](./assets/codigo_dag_composer.png 'Código da Dag no Composer')

Tentei executar o projeto, mas percebi um problema para transportar o arquivo entre a primeira task e a task do container. Por conta do tempo, não foi possível finalizar.
No entanto, este é um To Do. Finalizarei com maior tempo de pesquisa.
