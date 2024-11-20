# projeto-db-cassandra
Projeto criado para a disciplina de Tópicos Avançados de Banco de Dados - FEI 6 semestre

## Sobre
Este projeto cria e popula um banco de dados de uma universidade utilizando ScyllaDB. O banco de dados atua no modelo wide-column, e o arquivo executável traz os dados das seguintes queries: <br>
1. Histórico escolar dos alunos <br>
2. Histórico de disciplinas ministradas pelos professores <br>
3. Alunos formados <br>
4. Chefes de departamento <br>
5. Grupos de TCC e orientador

## Grupo
Gianluca Mariano Sobreiro - 22.122.011-4<br>
Guilherme Fornagiero de Carvalho - 22.122.016-3<br>
Paulo Vinícius Bessa de Brito - 22.122.005-6<br>
Pedro Augusto Bento Rocha - 22.122.028-8<br>

## Estrutura do projeto
- docker-compose.yml: arquivo de configuração dos containers Docker para o SyllaDB
- main.py: arquivo de execução do projeto, onde é criada toda a estrutura do banco, onde é populado, e onde são selecionadas as queries
- data/: arquivo de volumes do Docker

## Tables
Para as tabelas, foi criada uma para cada query, considerando o modelo Querie -> Data -> Model
### aluno_historico
```
CREATE TABLE IF NOT EXISTS aluno_historico (
    RA text,
    aluno text,
    curso_id int,
    curso_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    nota float,
    PRIMARY KEY (RA, ano, semestre, disciplina_codigo)
) WITH CLUSTERING ORDER BY (ano DESC, semestre DESC);
```
### professor_disciplinas
```
CREATE TABLE IF NOT EXISTS professor_disciplinas (
    professor_id text,
    professor_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    PRIMARY KEY (professor_id, ano, semestre, disciplina_codigo)
) WITH CLUSTERING ORDER BY (ano DESC, semestre DESC);
```
### alunos_formados
```
CREATE TABLE IF NOT EXISTS alunos_formados (
    curso_id int,
    curso_nome text,
    ano_formatura int,
    semestre_formatura int,
    RA text,
    aluno_nome text,
    PRIMARY KEY (curso_id, ano_formatura, semestre_formatura, RA)
) WITH CLUSTERING ORDER BY (ano_formatura DESC, semestre_formatura DESC);
```
### chefes_departamento
```
CREATE TABLE IF NOT EXISTS chefes_departamento (
    departamento text,
    professor_id text,
    professor_nome text,
    PRIMARY KEY (departamento, professor_id)
);
```
### tcc_grupos
```
CREATE TABLE IF NOT EXISTS tcc_grupos (
    tcc_id int,
    tcc_nome text,
    professor_id text,
    professor_nome text,
    aluno_RA text,
    aluno_nome text,
    PRIMARY KEY (tcc_id, professor_id, aluno_RA)
);
```

## Validação de Queries
As queries estão localizadas no final do arquivo main.py, a partir da linha 256, apresentando o que ela faz e a query CQL

## Como rodar o projeto?
1. Clone o projeto com o seguinte comando ```git clone https://github.com/guifornagiero/projeto-db-cassandra.git``` <br>
2. Entre na pasta do projeto com o comando ```cd projeto-db-cassandra```
3. Instale o Docker <br>
4. Instale o interpretador do Python <br>
5. Instale o driver do Neo4j e do Pandas com o comando ```pip install scylla-driver && pip install pandas``` <br>
6. Suba o container Docker com o comando ```docker-compose up -d``` <br>
7. Execute o arquivo main.py com o comando ```python main.py``` <br>

## Problemas comuns
- Pode ser que, na primeira vez que o comando ```python main.py``` for executado, mostre uma mensagem de erro.
- Basta executar novamente, que o código funcionará de forma correta, trazendo os selects das queries.














