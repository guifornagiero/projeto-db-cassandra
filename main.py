from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

# Configuração de conexão com o Cassandra
cluster = Cluster(
    contact_points=["127.0.0.1"], 
    port=9042, 
    auth_provider=PlainTextAuthProvider(username='admin', password='admin')
)


session = cluster.connect()

session.execute("DROP KEYSPACE IF EXISTS universidade")

# Create the keyspace again
session.execute("""
CREATE KEYSPACE universidade
WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}
""")

# Set the active keyspace
session.set_keyspace("universidade")


session.execute("""
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
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "LPG", "disciplina_nome": "Lógica de Programação", "semestre": 1, "ano": 2020, "nota": 9.0},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ESD", "disciplina_nome": "Estruturas de Dados", "semestre": 2, "ano": 2020, "nota": 8.5},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ALG", "disciplina_nome": "Algoritmos", "semestre": 3, "ano": 2021, "nota": 9.2},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ERQ", "disciplina_nome": "Engenharia de Requisitos", "semestre": 4, "ano": 2021, "nota": 8.0},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ARS", "disciplina_nome": "Arquitetura de Software", "semestre": 5, "ano": 2022, "nota": 9.3},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "TSF", "disciplina_nome": "Testes de Software", "semestre": 6, "ano": 2022, "nota": 8.0},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "MAG", "disciplina_nome": "Metodologias Ágeis", "semestre": 7, "ano": 2023, "nota": 9.0},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "DWX", "disciplina_nome": "Desenvolvimento Web", "semestre": 8, "ano": 2023, "nota": 9.5},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "BDB", "disciplina_nome": "Banco de Dados", "semestre": 9, "ano": 2024, "nota": 5.9},
    {"RA": "22.122.016-3", "aluno": "Gulherme F.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "GPJ", "disciplina_nome": "Gerência de Projetos", "semestre": 10, "ano": 2024, "nota": 6.0},
    {"RA": "22.122.017-4", "aluno": "Ana L.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "ANH", "disciplina_nome": "Anatomia Humana", "semestre": 1, "ano": 2020, "nota": 9.0},
    {"RA": "22.122.017-4", "aluno": "Ana L.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "FIS", "disciplina_nome": "Fisiologia", "semestre": 2, "ano": 2020, "nota": 8.5},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "BQC", "disciplina_nome": "Bioquímica", "semestre": 3, "ano": 2021, "nota": 9.2},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "FAR", "disciplina_nome": "Farmacologia", "semestre": 4, "ano": 2021, "nota": 9.0},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "PAT", "disciplina_nome": "Patologia", "semestre": 5, "ano": 2022, "nota": 8.7},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "CLM", "disciplina_nome": "Clínica Médica", "semestre": 6, "ano": 2022, "nota": 8.5},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "CIR", "disciplina_nome": "Cirurgia", "semestre": 7, "ano": 2023, "nota": 9.1},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "PED", "disciplina_nome": "Pediatria", "semestre": 8, "ano": 2023, "nota": 9.2},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "GOB", "disciplina_nome": "Ginecologia e Obstetrícia", "semestre": 9, "ano": 2024, "nota": 8.9},
    {"RA": "22.122.017-4", "aluno": "Ana L.","curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "SPB", "disciplina_nome": "Saúde Pública", "semestre": 10, "ano": 2024, "nota": 9.4}, 
    {"RA": "22.122.020-7", "aluno": "Rafael S.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "LPG", "disciplina_nome": "Lógica de Programação", "semestre": 1, "ano": 2020, "nota": 9.0},
    {"RA": "22.122.020-7", "aluno": "Rafael S.","curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ESD", "disciplina_nome": "Estruturas de Dados", "semestre": 2, "ano": 2020, "nota": 8.5},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ALG", "disciplina_nome": "Algoritmos", "semestre": 3, "ano": 2021, "nota": 9.2},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ERQ", "disciplina_nome": "Engenharia de Requisitos", "semestre": 4, "ano": 2021, "nota": 8.0},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "ARS", "disciplina_nome": "Arquitetura de Software", "semestre": 5, "ano": 2022, "nota": 9.3},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "TSF", "disciplina_nome": "Testes de Software", "semestre": 6, "ano": 2022, "nota": 8.0},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "MAG", "disciplina_nome": "Metodologias Ágeis", "semestre": 7, "ano": 2023, "nota": 9.0},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "DWX", "disciplina_nome": "Desenvolvimento Web", "semestre": 8, "ano": 2023, "nota": 9.5},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "BDB", "disciplina_nome": "Banco de Dados", "semestre": 9, "ano": 2024, "nota": 5.9},
    {"RA": "22.122.020-7", "aluno": "Rafael S.", "curso_id": 10, "curso_nome": "Ciência da Computação", "disciplina_codigo": "GPJ", "disciplina_nome": "Gerência de Projetos", "semestre": 10, "ano": 2024, "nota": 6.0},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "ANH", "disciplina_nome": "Anatomia Humana", "semestre": 1, "ano": 2020, "nota": 9.1},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "FIS", "disciplina_nome": "Fisiologia", "semestre": 2, "ano": 2020, "nota": 8.8},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "BQC", "disciplina_nome": "Bioquímica", "semestre": 3, "ano": 2021, "nota": 9.3},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "FAR", "disciplina_nome": "Farmacologia", "semestre": 4, "ano": 2021, "nota": 9.0},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "PAT", "disciplina_nome": "Patologia", "semestre": 5, "ano": 2022, "nota": 8.5},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "CLM", "disciplina_nome": "Clínica Médica", "semestre": 6, "ano": 2022, "nota": 9.2},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "CIR", "disciplina_nome": "Cirurgia", "semestre": 7, "ano": 2023, "nota": 9.4},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "PED", "disciplina_nome": "Pediatria", "semestre": 8, "ano": 2023, "nota": 9.1},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "GOB", "disciplina_nome": "Ginecologia e Obstetrícia", "semestre": 9, "ano": 2024, "nota": 9.0},
    {"RA": "22.122.021-8", "aluno": "Juliana P.", "curso_id": 20, "curso_nome": "Medicina", "disciplina_codigo": "SPB", "disciplina_nome": "Saúde Pública", "semestre": 10, "ano": 2024, "nota": 9.5},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "TCB", "disciplina_nome": "Teoria das Cores", "semestre": 1, "ano": 2020, "nota": 2.4},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "TIP", "disciplina_nome": "Tipografia", "semestre": 2, "ano": 2020, "nota": 8.5},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "DGD", "disciplina_nome": "Design Digital", "semestre": 3, "ano": 2021, "nota": 9.2},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "ILS", "disciplina_nome": "Ilustração", "semestre": 4, "ano": 2021, "nota": 8.8},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "FOT", "disciplina_nome": "Fotografia", "semestre": 5, "ano": 2022, "nota": 9.1},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "DIH", "disciplina_nome": "Design de Interface", "semestre": 6, "ano": 2022, "nota": 9.3},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "BRD", "disciplina_nome": "Branding", "semestre": 7, "ano": 2023, "nota": 8.9},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "ANI", "disciplina_nome": "Animação", "semestre": 8, "ano": 2023, "nota": 9.4},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "PFN", "disciplina_nome": "Projeto Final", "semestre": 9, "ano": 2024, "nota": 9.5},
    {"RA": "22.122.022-9", "aluno": "Fernanda C.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "GPD", "disciplina_nome": "Gestão de Projetos de Design", "semestre": 10, "ano": 2024, "nota": 9.2},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "TCB", "disciplina_nome": "Teoria das Cores", "semestre": 1, "ano": 2020, "nota": 8.0},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "TIP", "disciplina_nome": "Tipografia", "semestre": 2, "ano": 2020, "nota": 9.3},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "DGD", "disciplina_nome": "Design Digital", "semestre": 3, "ano": 2021, "nota": 8.5},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "ILS", "disciplina_nome": "Ilustração", "semestre": 4, "ano": 2021, "nota": 9.0},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "FOT", "disciplina_nome": "Fotografia", "semestre": 5, "ano": 2022, "nota": 8.7},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "DIH", "disciplina_nome": "Design de Interface", "semestre": 6, "ano": 2022, "nota": 3.2},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "BRD", "disciplina_nome": "Branding", "semestre": 7, "ano": 2023, "nota": 8.4},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "ANI", "disciplina_nome": "Animação", "semestre": 8, "ano": 2023, "nota": 9.2},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "PFN", "disciplina_nome": "Projeto Final", "semestre": 9, "ano": 2024, "nota": 9.0},
    {"RA": "22.122.023-0", "aluno": "Lucas M.", "curso_id": 30, "curso_nome": "Design Gráfico", "disciplina_codigo": "GPD", "disciplina_nome": "Gestão de Projetos de Design", "semestre": 10, "ano": 2024, "nota": 4.6}
]

professor_disciplinas_data = [
    {"professor_id": "1", "professor_nome": "João da Silva", "disciplina_codigo": "LPG", "disciplina_nome": "Lógica de Programação", "semestre": 1, "ano": 2020},
    {"professor_id": "1", "professor_nome": "João da Silva", "disciplina_codigo": "ESD", "disciplina_nome": "Estruturas de Dados", "semestre": 2, "ano": 2020},
    {"professor_id": "1", "professor_nome": "João da Silva", "disciplina_codigo": "LPG", "disciplina_nome": "Lógica de Programação", "semestre": 1, "ano": 2021},
    {"professor_id": "1", "professor_nome": "João da Silva", "disciplina_codigo": "LPG", "disciplina_nome": "Lógica de Programação", "semestre": 1, "ano": 2022},
    {"professor_id": "1", "professor_nome": "João da Silva", "disciplina_codigo": "TSF", "disciplina_nome": "Testes de Software", "semestre": 6, "ano": 2022},
    {"professor_id": "2", "professor_nome": "Natália Almeida", "disciplina_codigo": "BQC", "disciplina_nome": "Bioquímica", "semestre": 3, "ano": 2021},
    {"professor_id": "2", "professor_nome": "Natália Almeida", "disciplina_codigo": "CLM", "disciplina_nome": "Clínica Médica", "semestre": 6, "ano": 2022},
    {"professor_id": "2", "professor_nome": "Natália Almeida", "disciplina_codigo": "GOB", "disciplina_nome": "Ginecologia e Obstetrícia", "semestre": 9, "ano": 2024},
    {"professor_id": "3", "professor_nome": "Carlos Ferreira", "disciplina_codigo": "TCB", "disciplina_nome": "Teoria das Cores", "semestre": 1, "ano": 2020},
    {"professor_id": "3", "professor_nome": "Carlos Ferreira", "disciplina_codigo": "GPD", "disciplina_nome": "Gestão de Projetos de Design", "semestre": 10, "ano": 2024}
]

alunos_formados_data = [
    {"curso_id": 1, "curso_nome": "Engenharia de Computação", "ano_formatura": 2024, "semestre_formatura": 1, "RA": "123456", "aluno_nome": "Carlos Pereira"},
    {"curso_id": 2, "curso_nome": "Engenharia Elétrica", "ano_formatura": 2024, "semestre_formatura": 1, "RA": "654321", "aluno_nome": "Ana Oliveira"}
]

chefes_departamento_data = [
    
    {"departamento": "Computação", "professor_id": "1", "professor_nome": "João da Silva"},
    {"departamento": "Medicina", "professor_id": "2", "professor_nome": "Natália Almeida"},
    {"departamento": "Humanas", "professor_id": "3", "professor_nome": "Carlos Ferreira"}
]

tcc_grupos_data = [
    {"tcc_id": 101, "tcc_nome": "Aplicação de Finanças", "professor_id": "1", "professor_nome": "João da Silva", "aluno_RA": "22.122.016-3", "aluno_nome": "Guilherme F."},
    {"tcc_id": 101, "tcc_nome": "Aplicação de Finanças", "professor_id": "1", "professor_nome": "João da Silva", "aluno_RA": "22.122.020-7", "aluno_nome": "Rafael S."},
    {"tcc_id": 102, "tcc_nome": "Impacto da Nutrição na Saúde", "professor_id": "2", "professor_nome": "Natália Almeida", "aluno_RA": "22.122.017-4", "aluno_nome": "Ana L."},
    {"tcc_id": 102, "tcc_nome": "Impacto da Nutrição na Saúde", "professor_id": "2", "professor_nome": "Natália Almeida", "aluno_RA": "22.122.021-8", "aluno_nome": "Juliana P."},
    {"tcc_id": 105, "tcc_nome": "Desenvolvimento de Campanha Publicitária", "professor_id": "3", "professor_nome": "Carlos Ferreira", "aluno_RA": "22.122.022-9", "aluno_nome": "Fernanda C."},
    {"tcc_id": 106, "tcc_nome": "Impacto do Design na Comunicação Visual", "professor_id": "3", "professor_nome": "Carlos Ferreira", "aluno_RA": "22.122.023-0", "aluno_nome": "Lucas M."}
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
        INSERT INTO aluno_historico (RA, aluno, curso_id, curso_nome, disciplina_codigo, disciplina_nome, semestre, ano, nota)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (data['RA'], data['aluno'], data['curso_id'], data['curso_nome'], data['disciplina_codigo'], data['disciplina_nome'], data['semestre'], data['ano'], data['nota']))

# Inserindo dados em professor_disciplinas, se estiver vazia
if tabela_esta_vazia("professor_disciplinas"):
    for data in professor_disciplinas_data:
        session.execute("""
        INSERT INTO professor_disciplinas (professor_id, professor_nome, disciplina_codigo, disciplina_nome, semestre, ano)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (data['professor_id'], data['professor_nome'], data['disciplina_codigo'], data['disciplina_nome'], data['semestre'], data['ano']))

def aluno_completo(aluno_data):
    semestres = {}
    for data in aluno_data:
        RA = data["RA"]
        semestre = data["semestre"]
        nota = data["nota"]
        # Filtra alunos que têm a nota maior ou igual a 5
        if nota >= 5:
            if RA not in semestres:
                semestres[RA] = set()
            semestres[RA].add(semestre)

    return {RA: semestres[RA] for RA in semestres if len(semestres[RA]) == 10}

# Filtra alunos que possuem 10 semestres completos com nota maior ou igual a 5
alunos_qualificados = aluno_completo(aluno_historico_data)

for RA, semestres in alunos_qualificados.items():
    # Obtém informações do aluno (exemplo: pegando o primeiro registro)
    aluno_info = next(item for item in aluno_historico_data if item["RA"] == RA)
    curso_id = aluno_info["curso_id"]
    curso_nome = aluno_info["curso_nome"]
    ano_formatura = 2024  # Substitua com o ano de formatura real
    semestre_formatura = 2  # Substitua com o semestre de formatura real

    # Inserir na tabela
    session.execute("""
    INSERT INTO alunos_formados (curso_id, curso_nome, ano_formatura, semestre_formatura, RA, aluno_nome)
    VALUES (%s, %s, %s, %s, %s, %s)
    """, (curso_id, curso_nome, ano_formatura, semestre_formatura, RA, aluno_info["aluno"]))

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

print("\nQuery 1: Histórico escolar de qualquer aluno")
rows = session.execute("""
SELECT ra, aluno, disciplina_nome, semestre, ano, nota
FROM aluno_historico
""")
sorted_rows = sorted(rows, key=lambda x: (x.ra, x.aluno, x.semestre))
df = pd.DataFrame(sorted_rows, columns=['ra', 'aluno', 'disciplina_nome', 'semestre', 'ano', 'nota'])
print(df)
    


print("\nQuery 2: Histórico de disciplinas ministradas por qualquer professor")
rows = session.execute("""
SELECT professor_nome, disciplina_nome, semestre, ano
FROM professor_disciplinas
;
""")

sorted_rows = sorted(rows, key=lambda x: (x.professor_nome, x.semestre))
df = pd.DataFrame(sorted_rows, columns=['professor_nome', 'disciplina_nome', 'semestre', 'ano'])
print(df)

print("\n Query 3: Alunos que se formaram em determinado semestre e ano")
rows = session.execute("""
SELECT RA, aluno_nome, ano_formatura, semestre_formatura
FROM alunos_formados
""")

sorted_rows = sorted(rows, key=lambda x: (x.ra, x.aluno_nome))
df = pd.DataFrame(sorted_rows, columns=['RA', 'aluno_nome', 'ano_formatura', 'semestre_formatura'])
print(df)

print("\nQuery 4: Chefes de departamento")
rows = session.execute("""
SELECT professor_id, professor_nome, departamento
FROM chefes_departamento;
""")
df = pd.DataFrame(rows, columns=['professor_id', 'professor_nome', 'departamento'])
print(df)


print("\n5. Query 5: Alunos de tcc e orientador")
rows = session.execute("""
SELECT aluno_RA, aluno_nome, professor_nome, tcc_nome
FROM tcc_grupos
""")

df = pd.DataFrame(rows, columns=['aluno_RA', 'aluno_nome', 'professor_nome', 'tcc_nome'])
print(df)
