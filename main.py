from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Configuração de conexão com o Cassandra
cluster = Cluster(
    contact_points=["localhost"], 
    port=9042, 
    auth_provider=PlainTextAuthProvider(username='admin', password='admin')
)

session = cluster.connect()

# Seleciona ou cria o keyspace
session.execute("""
CREATE KEYSPACE IF NOT EXISTS universidade
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}
""")
session.set_keyspace("universidade")

# Criação das tabelas
session.execute("""
CREATE TABLE IF NOT EXISTS aluno_historico (
    RA text,
    curso_id int,
    curso_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    nota float,
    PRIMARY KEY (RA, ano, semestre, disciplina_codigo)
) WITH CLUSTERING ORDER BY (ano DESC, semestre DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS professor_disciplinas (
    professor_id text,
    professor_nome text,
    disciplina_codigo text,
    disciplina_nome text,
    semestre int,
    ano int,
    PRIMARY KEY (professor_id, ano, semestre, disciplina_codigo)
) WITH CLUSTERING ORDER BY (ano DESC, semestre DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS alunos_formados (
    curso_id int,
    curso_nome text,
    ano_formatura int,
    semestre_formatura int,
    RA text,
    aluno_nome text,
    PRIMARY KEY (curso_id, ano_formatura, semestre_formatura, RA)
) WITH CLUSTERING ORDER BY (ano_formatura DESC, semestre_formatura DESC);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS chefes_departamento (
    departamento text,
    professor_id text,
    professor_nome text,
    PRIMARY KEY (departamento, professor_id)
);
""")

session.execute("""
CREATE TABLE IF NOT EXISTS tcc_grupos (
    tcc_id int,
    tcc_nome text,
    professor_id text,
    professor_nome text,
    aluno_RA text,
    aluno_nome text,
    PRIMARY KEY (tcc_id, professor_id, aluno_RA)
);
""")

# Dados JSON de exemplo para cada tabela
aluno_historico_data = [
    {"RA": "123456", "curso_id": 1, "curso_nome": "Engenharia de Computação", "disciplina_codigo": "COMP101", "disciplina_nome": "Introdução à Computação", "semestre": 1, "ano": 2022, "nota": 8.5},
    {"RA": "123456", "curso_id": 1, "curso_nome": "Engenharia de Computação", "disciplina_codigo": "MATH101", "disciplina_nome": "Cálculo I", "semestre": 1, "ano": 2022, "nota": 7.0},
    {"RA": "654321", "curso_id": 2, "curso_nome": "Engenharia Elétrica", "disciplina_codigo": "ELEC201", "disciplina_nome": "Circuitos Elétricos", "semestre": 2, "ano": 2023, "nota": 9.0}
]

professor_disciplinas_data = [
    {"professor_id": "prof123", "professor_nome": "Dr. João Silva", "disciplina_codigo": "COMP101", "disciplina_nome": "Introdução à Computação", "semestre": 1, "ano": 2022},
    {"professor_id": "prof123", "professor_nome": "Dr. João Silva", "disciplina_codigo": "COMP202", "disciplina_nome": "Estruturas de Dados", "semestre": 2, "ano": 2022},
    {"professor_id": "prof456", "professor_nome": "Dra. Maria Souza", "disciplina_codigo": "ELEC201", "disciplina_nome": "Circuitos Elétricos", "semestre": 2, "ano": 2023}
]

alunos_formados_data = [
    {"curso_id": 1, "curso_nome": "Engenharia de Computação", "ano_formatura": 2024, "semestre_formatura": 1, "RA": "123456", "aluno_nome": "Carlos Pereira"},
    {"curso_id": 2, "curso_nome": "Engenharia Elétrica", "ano_formatura": 2024, "semestre_formatura": 1, "RA": "654321", "aluno_nome": "Ana Oliveira"}
]

chefes_departamento_data = [
    {"departamento": "Computação", "professor_id": "prof123", "professor_nome": "Dr. João Silva"},
    {"departamento": "Engenharia Elétrica", "professor_id": "prof456", "professor_nome": "Dra. Maria Souza"}
]

tcc_grupos_data = [
    {"tcc_id": 101, "tcc_nome": "Inteligência Artificial Aplicada", "professor_id": "prof123", "professor_nome": "Dr. João Silva", "aluno_RA": "123456", "aluno_nome": "Carlos Pereira"},
    {"tcc_id": 101, "tcc_nome": "Inteligência Artificial Aplicada", "professor_id": "prof123", "professor_nome": "Dr. João Silva", "aluno_RA": "789012", "aluno_nome": "Luiz Martins"}
]

# Inserção de dados usando loops
# Função para verificar se uma tabela está vazia
def tabela_esta_vazia(tabela):
    rows = session.execute(f"SELECT COUNT(*) FROM {tabela}")
    return rows.one()[0] == 0

# Inserindo dados em aluno_historico, se estiver vazia
if tabela_esta_vazia("aluno_historico"):
    for data in aluno_historico_data:
        session.execute("""
        INSERT INTO aluno_historico (RA, curso_id, curso_nome, disciplina_codigo, disciplina_nome, semestre, ano, nota)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (data['RA'], data['curso_id'], data['curso_nome'], data['disciplina_codigo'], data['disciplina_nome'], data['semestre'], data['ano'], data['nota']))

# Inserindo dados em professor_disciplinas, se estiver vazia
if tabela_esta_vazia("professor_disciplinas"):
    for data in professor_disciplinas_data:
        session.execute("""
        INSERT INTO professor_disciplinas (professor_id, professor_nome, disciplina_codigo, disciplina_nome, semestre, ano)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['professor_id'], data['professor_nome'], data['disciplina_codigo'], data['disciplina_nome'], data['semestre'], data['ano']))

# Inserindo dados em alunos_formados, se estiver vazia
if tabela_esta_vazia("alunos_formados"):
    for data in alunos_formados_data:
        session.execute("""
        INSERT INTO alunos_formados (curso_id, curso_nome, ano_formatura, semestre_formatura, RA, aluno_nome)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['curso_id'], data['curso_nome'], data['ano_formatura'], data['semestre_formatura'], data['RA'], data['aluno_nome']))

# Inserindo dados em chefes_departamento, se estiver vazia
if tabela_esta_vazia("chefes_departamento"):
    for data in chefes_departamento_data:
        session.execute("""
        INSERT INTO chefes_departamento (departamento, professor_id, professor_nome)
        VALUES (%s, %s, %s)
        """, (data['departamento'], data['professor_id'], data['professor_nome']))

# Inserindo dados em tcc_grupos, se estiver vazia
if tabela_esta_vazia("tcc_grupos"):
    for data in tcc_grupos_data:
        session.execute("""
        INSERT INTO tcc_grupos (tcc_id, tcc_nome, professor_id, professor_nome, aluno_RA, aluno_nome)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['tcc_id'], data['tcc_nome'], data['professor_id'], data['professor_nome'], data['aluno_RA'], data['aluno_nome']))

print("Dados inseridos com sucesso.")

print("1. Histórico escolar de um aluno \n\n")
rows = session.execute("""
SELECT disciplina_codigo, disciplina_nome, semestre, ano, nota, ra, curso_nome
FROM aluno_historico
WHERE RA = '123456';
""")
for row in rows:
    print(f"RA: {row.ra}, Curso: {row.curso_nome}, Disciplina: {row.disciplina_nome}, Semestre: {row.semestre}, Ano: {row.ano}, Nota: {row.nota}")
print("\n\n")
print("2. Histórico de disciplinas ministradas por um professor \n\n")
rows = session.execute("""
SELECT disciplina_codigo, disciplina_nome, semestre, ano, professor_nome
FROM professor_disciplinas
WHERE professor_id = 'prof123';
""")
for row in rows:
    print(f"Professor: {row.professor_nome}, Disciplina: {row.disciplina_nome}, Semestre: {row.semestre}, Ano: {row.ano}")

print("\n\n")

print("3. Alunos formados em determinado semestre e ano \n\n")
rows = session.execute("""
SELECT RA, aluno_nome
FROM alunos_formados
WHERE curso_id = 1 AND ano_formatura = 2024 AND semestre_formatura = 1;
""")
for row in rows:
    print(f"RA: {row.ra}, Nome: {row.aluno_nome}")

print("\n\n")

print("4. Professores que são chefes de departamento \n\n")
rows = session.execute("""
SELECT professor_id, professor_nome, departamento
FROM chefes_departamento;
""")
for row in rows:
    print(f"ID: {row.professor_id}, Nome: {row.professor_nome}, Departamento: {row.departamento}")

print("\n\n")

print("5. Alunos em grupo de TCC e seus orientadores \n\n")
rows = session.execute("""
SELECT aluno_RA, aluno_nome, professor_nome, tcc_nome
FROM tcc_grupos
WHERE tcc_id = 101;
""")
for row in rows:
    print(f"Aluno: {row.aluno_nome}, Orientador: {row.professor_nome}, TCC: {row.tcc_nome}")

print("\n\n")
