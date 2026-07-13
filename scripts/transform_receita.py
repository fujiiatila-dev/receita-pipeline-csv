"""
Transformacao: cria a tabela final mt_empresa_socios_enriquecido (tabela viva, sem sufixo de mes).
Ao substituir, a tabela anterior e arquivada com o sufixo do seu ano-mes:
mt_empresa_socios_enriquecido_<periodo_antigo>.
Uso: python transform_receita.py <periodo> [periodo_antigo]
  ex: python transform_receita.py 202603 202602
    periodo        = ano-mes dos dados novos (tabelas staging *_<periodo>)
    periodo_antigo = ano-mes da tabela em producao que sera substituida/arquivada (opcional)
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
FINAL_TABLE_NAME = "mt_empresa_socios_enriquecido"
FINAL_TABLE = f"{DATABASE}.{FINAL_TABLE_NAME}"


def _archive_table_name(periodo, periodo_antigo):
    """Nome da tabela arquivada ao substituir a tabela viva.

    Usa o ano-mes da tabela antiga quando informado (arquivo permanente);
    na ausencia, cai para um backup temporario com o periodo novo.
    """
    if periodo_antigo:
        return f"{FINAL_TABLE}_{periodo_antigo}"
    return f"{FINAL_TABLE}_{periodo}_bkp"

UFS = [
    'AC', 'AL', 'AM', 'AP', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA',
    'MG', 'MS', 'MT', 'PA', 'PB', 'PE', 'PI', 'PR', 'RJ', 'RN',
    'RO', 'RR', 'RS', 'SC', 'SE', 'SP', 'TO', 'EX', '',
]


def _get_ddl():
    """DDL exato da tabela final — identico ao original."""
    return f"""
    CREATE TABLE {FINAL_TABLE}
    (
        `cnpj` String,
        `cnpj_basico` String,
        `cnpj_ordem` String,
        `cnpj_dv` String,
        `matriz_filial` String,
        `desc_matriz_filial` String,
        `nome_fantasia` String,
        `situacao_cadastral` String,
        `desc_situacao_cadastral` String,
        `data_situacao_cadastral` Nullable(DateTime),
        `motivo_situacao_cadastral` String,
        `cidade_exterior` String,
        `pais` String,
        `data_inicio_atividade` Nullable(DateTime),
        `cnae_principal` String,
        `desc_cnae_principal` Nullable(String),
        `cnae_secundario` String,
        `tipo_logradouro` String,
        `logradouro` String,
        `numero` String,
        `complemento` String,
        `bairro` String,
        `cep` String,
        `uf` String,
        `municipio` String,
        `desc_municipio` Nullable(String),
        `ddd_1` String,
        `telefone_1` String,
        `ddd_2` String,
        `telefone_2` String,
        `ddd_fax` String,
        `fax` String,
        `email` String,
        `situacao_especial` String,
        `data_situacao_especial` Nullable(DateTime),
        `latitude` Nullable(Float32),
        `longitude` Nullable(Float32),
        `razao_social` String,
        `natureza_juridica` String,
        `desc_natureza_juridica` Nullable(String),
        `qualificacao_responsavel` String,
        `desc_qualificacao_responsavel` Nullable(String),
        `capital_social` Nullable(Decimal(18, 2)),
        `porte` String,
        `desc_porte` String,
        `ente_federativo_responsavel` String,
        `nome_socio_1` String,
        `cpf_cnpj_socio_1` String,
        `identificador_socio_1` String,
        `desc_identificador_socio_1` String,
        `qualificacao_socio_1` String,
        `desc_qualificacao_socio_1` String,
        `data_entrada_sociedade_1` Nullable(DateTime),
        `faixa_etaria_1` String,
        `desc_faixa_etaria_1` String,
        `nome_representante_1` String,
        `qualificacao_representante_1` String,
        `desc_qualificacao_representante_1` String,
        `nome_socio_2` String,
        `cpf_cnpj_socio_2` String,
        `identificador_socio_2` String,
        `desc_identificador_socio_2` String,
        `qualificacao_socio_2` String,
        `desc_qualificacao_socio_2` String,
        `data_entrada_sociedade_2` Nullable(DateTime),
        `faixa_etaria_2` String,
        `desc_faixa_etaria_2` String,
        `nome_representante_2` String,
        `qualificacao_representante_2` String,
        `desc_qualificacao_representante_2` String,
        `nome_socio_3` String,
        `cpf_cnpj_socio_3` String,
        `identificador_socio_3` String,
        `desc_identificador_socio_3` String,
        `qualificacao_socio_3` String,
        `desc_qualificacao_socio_3` String,
        `data_entrada_sociedade_3` Nullable(DateTime),
        `faixa_etaria_3` String,
        `desc_faixa_etaria_3` String,
        `nome_representante_3` String,
        `qualificacao_representante_3` String,
        `desc_qualificacao_representante_3` String,
        `opcao_simples` String,
        `data_opcao_simples` Nullable(DateTime),
        `data_exclusao_simples` Nullable(DateTime),
        `opcao_mei` String,
        `data_opcao_mei` Nullable(DateTime),
        `data_exclusao_mei` Nullable(DateTime),
        `tipo_simples_mei` String,
        `porte_empresa` String,
        `desc_porteempresa_corrigido` String,
        `faixa_faturamento_anual_ajustada` String,
        `faixa_idade_empresa` String,
        `regiao_geo` String,
        `uf_geo` String,
        `linha_unica` UInt64
    )
    ENGINE = MergeTree
    PARTITION BY uf
    ORDER BY (uf, cnpj_basico)
    SETTINGS index_granularity = 8192
    """


def _create_socios_pivot(client, periodo):
    """Cria tabela de socios pivoteada (top 3 por empresa, ordenados por antiguidade)."""
    soc = f"{DATABASE}.socios_{periodo}"
    pivot = f"{DATABASE}.socios_{periodo}_pivot"
    ranked = f"{DATABASE}.socios_{periodo}_ranked"

    print(f"[Transform] Criando pivot de socios: {pivot}")

    # 1. Criar tabela ranqueada (row_number por cnpj_basico, ordenado por data ASC + nome ASC)
    client.command(f"DROP TABLE IF EXISTS {ranked}")
    client.command(f"""
    CREATE TABLE {ranked}
    ENGINE = MergeTree
    ORDER BY (cnpj_basico, rn)
    AS
    SELECT *,
        row_number() OVER (
            PARTITION BY cnpj_basico
            ORDER BY data_entrada_sociedade ASC, nome_socio_razao_social ASC
        ) AS rn
    FROM {soc}
    """)
    print("[Transform] Tabela ranqueada criada")

    # 2. Criar pivot a partir dos top 3
    client.command(f"DROP TABLE IF EXISTS {pivot}")
    sql = f"""
    CREATE TABLE {pivot}
    ENGINE = MergeTree
    ORDER BY cnpj_basico
    AS
    SELECT
        cnpj_basico,
        groupArray(nome_socio_razao_social)[1] AS nome_socio_1,
        groupArray(cpf_cnpj_socio)[1] AS cpf_cnpj_socio_1,
        groupArray(identificador_socio)[1] AS identificador_socio_1,
        groupArray(qualificacao_socio)[1] AS qualificacao_socio_1,
        groupArray(data_entrada_sociedade)[1] AS data_entrada_sociedade_1,
        groupArray(faixa_etaria)[1] AS faixa_etaria_1,
        groupArray(nome_representante)[1] AS nome_representante_1,
        groupArray(qualificacao_representante_legal)[1] AS qualificacao_representante_1,
        groupArray(nome_socio_razao_social)[2] AS nome_socio_2,
        groupArray(cpf_cnpj_socio)[2] AS cpf_cnpj_socio_2,
        groupArray(identificador_socio)[2] AS identificador_socio_2,
        groupArray(qualificacao_socio)[2] AS qualificacao_socio_2,
        groupArray(data_entrada_sociedade)[2] AS data_entrada_sociedade_2,
        groupArray(faixa_etaria)[2] AS faixa_etaria_2,
        groupArray(nome_representante)[2] AS nome_representante_2,
        groupArray(qualificacao_representante_legal)[2] AS qualificacao_representante_2,
        groupArray(nome_socio_razao_social)[3] AS nome_socio_3,
        groupArray(cpf_cnpj_socio)[3] AS cpf_cnpj_socio_3,
        groupArray(identificador_socio)[3] AS identificador_socio_3,
        groupArray(qualificacao_socio)[3] AS qualificacao_socio_3,
        groupArray(data_entrada_sociedade)[3] AS data_entrada_sociedade_3,
        groupArray(faixa_etaria)[3] AS faixa_etaria_3,
        groupArray(nome_representante)[3] AS nome_representante_3,
        groupArray(qualificacao_representante_legal)[3] AS qualificacao_representante_3
    FROM {ranked}
    WHERE rn <= 3
    GROUP BY cnpj_basico
    """
    client.command(sql)

    # Limpar tabela intermediaria
    client.command(f"DROP TABLE IF EXISTS {ranked}")

    count = client.query(f"SELECT count() FROM {pivot}").result_rows[0][0]
    print(f"[Transform] Pivot criado: {count:,} empresas com socios (top 3 por antiguidade)")


def _get_select_sql(periodo, uf_filter=None):
    """SELECT usando tabela pivot pre-criada para socios."""
    estab = f"{DATABASE}.estabelecimentos_{periodo}"
    emp = f"{DATABASE}.empresas_{periodo}"
    simp = f"{DATABASE}.simples_{periodo}"
    pivot = f"{DATABASE}.socios_{periodo}_pivot"

    where_clause = "WHERE e.situacao_cadastral = '02'"
    if uf_filter is not None:
        where_clause += f" AND e.uf = '{uf_filter}'"

    return f"""
    SELECT
        concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
        e.cnpj_basico AS cnpj_basico,
        e.cnpj_ordem AS cnpj_ordem,
        e.cnpj_dv AS cnpj_dv,
        e.identificador_matriz_filial AS matriz_filial,
        multiIf(
            e.identificador_matriz_filial = '1', 'Matriz',
            e.identificador_matriz_filial = '2', 'Filial',
            ''
        ) AS desc_matriz_filial,
        e.nome_fantasia,
        e.situacao_cadastral,
        multiIf(
            e.situacao_cadastral = '01', 'Nula',
            e.situacao_cadastral = '02', 'Ativa',
            e.situacao_cadastral = '03', 'Suspensa',
            e.situacao_cadastral = '04', 'Inapta',
            e.situacao_cadastral = '08', 'Baixada',
            ''
        ) AS desc_situacao_cadastral,
        parseDateTimeBestEffortOrNull(nullIf(e.data_situacao_cadastral, '')) AS data_situacao_cadastral,
        e.motivo_situacao_cadastral,
        e.nome_cidade_exterior AS cidade_exterior,
        e.pais,
        parseDateTimeBestEffortOrNull(nullIf(e.data_inicio_atividade, '')) AS data_inicio_atividade,
        e.cnae_fiscal_principal AS cnae_principal,
        dc.descricao_cnae AS desc_cnae_principal,
        e.cnae_fiscal_secundaria AS cnae_secundario,
        e.tipo_logradouro,
        e.logradouro,
        e.numero,
        e.complemento,
        e.bairro,
        e.cep,
        e.uf AS uf,
        e.municipio AS municipio,
        dm.descricao_municipios AS desc_municipio,
        e.ddd_1,
        e.telefone_1,
        e.ddd_2,
        e.telefone_2,
        e.ddd_fax,
        e.fax,
        e.correio_eletronico AS email,
        e.situacao_especial,
        parseDateTimeBestEffortOrNull(nullIf(e.data_situacao_especial, '')) AS data_situacao_especial,

        CAST(NULL AS Nullable(Float32)) AS latitude,
        CAST(NULL AS Nullable(Float32)) AS longitude,

        emp.razao_social,
        emp.natureza_juridica,
        dn.descricao_natureza_juridica AS desc_natureza_juridica,
        emp.qualificacao_responsavel,
        dq.descricao_qualificacao_socio AS desc_qualificacao_responsavel,
        CAST(nullIf(replaceAll(emp.capital_social, ',', '.'), '') AS Nullable(Decimal(18, 2))) AS capital_social,
        emp.porte_empresa AS porte,
        multiIf(
            emp.porte_empresa = '00', 'Nao informado',
            emp.porte_empresa = '01', 'Micro empresa',
            emp.porte_empresa = '03', 'Empresa de pequeno porte',
            emp.porte_empresa = '05', 'Demais',
            ''
        ) AS desc_porte,
        emp.ente_federativo_responsavel,

        -- Socios (da tabela pivot)
        ifNull(sp.nome_socio_1, '') AS nome_socio_1,
        ifNull(sp.cpf_cnpj_socio_1, '') AS cpf_cnpj_socio_1,
        ifNull(sp.identificador_socio_1, '') AS identificador_socio_1,
        multiIf(sp.identificador_socio_1 = '1', 'Pessoa Juridica', sp.identificador_socio_1 = '2', 'Pessoa Fisica', sp.identificador_socio_1 = '3', 'Estrangeiro', '') AS desc_identificador_socio_1,
        ifNull(sp.qualificacao_socio_1, '') AS qualificacao_socio_1,
        dqs1.descricao_qualificacao_socio AS desc_qualificacao_socio_1,
        parseDateTimeBestEffortOrNull(nullIf(sp.data_entrada_sociedade_1, '')) AS data_entrada_sociedade_1,
        ifNull(sp.faixa_etaria_1, '') AS faixa_etaria_1,
        dfe1.descricao AS desc_faixa_etaria_1,
        ifNull(sp.nome_representante_1, '') AS nome_representante_1,
        ifNull(sp.qualificacao_representante_1, '') AS qualificacao_representante_1,
        dqr1.descricao_qualificacao_socio AS desc_qualificacao_representante_1,

        ifNull(sp.nome_socio_2, '') AS nome_socio_2,
        ifNull(sp.cpf_cnpj_socio_2, '') AS cpf_cnpj_socio_2,
        ifNull(sp.identificador_socio_2, '') AS identificador_socio_2,
        multiIf(sp.identificador_socio_2 = '1', 'Pessoa Juridica', sp.identificador_socio_2 = '2', 'Pessoa Fisica', sp.identificador_socio_2 = '3', 'Estrangeiro', '') AS desc_identificador_socio_2,
        ifNull(sp.qualificacao_socio_2, '') AS qualificacao_socio_2,
        dqs2.descricao_qualificacao_socio AS desc_qualificacao_socio_2,
        parseDateTimeBestEffortOrNull(nullIf(sp.data_entrada_sociedade_2, '')) AS data_entrada_sociedade_2,
        ifNull(sp.faixa_etaria_2, '') AS faixa_etaria_2,
        dfe2.descricao AS desc_faixa_etaria_2,
        ifNull(sp.nome_representante_2, '') AS nome_representante_2,
        ifNull(sp.qualificacao_representante_2, '') AS qualificacao_representante_2,
        dqr2.descricao_qualificacao_socio AS desc_qualificacao_representante_2,

        ifNull(sp.nome_socio_3, '') AS nome_socio_3,
        ifNull(sp.cpf_cnpj_socio_3, '') AS cpf_cnpj_socio_3,
        ifNull(sp.identificador_socio_3, '') AS identificador_socio_3,
        multiIf(sp.identificador_socio_3 = '1', 'Pessoa Juridica', sp.identificador_socio_3 = '2', 'Pessoa Fisica', sp.identificador_socio_3 = '3', 'Estrangeiro', '') AS desc_identificador_socio_3,
        ifNull(sp.qualificacao_socio_3, '') AS qualificacao_socio_3,
        dqs3.descricao_qualificacao_socio AS desc_qualificacao_socio_3,
        parseDateTimeBestEffortOrNull(nullIf(sp.data_entrada_sociedade_3, '')) AS data_entrada_sociedade_3,
        ifNull(sp.faixa_etaria_3, '') AS faixa_etaria_3,
        dfe3.descricao AS desc_faixa_etaria_3,
        ifNull(sp.nome_representante_3, '') AS nome_representante_3,
        ifNull(sp.qualificacao_representante_3, '') AS qualificacao_representante_3,
        dqr3.descricao_qualificacao_socio AS desc_qualificacao_representante_3,

        -- Simples / MEI
        ifNull(simp.opcao_simples, '') AS opcao_simples,
        parseDateTimeBestEffortOrNull(nullIf(simp.data_opcao_simples, '')) AS data_opcao_simples,
        parseDateTimeBestEffortOrNull(nullIf(simp.data_exclusao_simples, '')) AS data_exclusao_simples,
        ifNull(simp.opcao_mei, '') AS opcao_mei,
        parseDateTimeBestEffortOrNull(nullIf(simp.data_opcao_mei, '')) AS data_opcao_mei,
        parseDateTimeBestEffortOrNull(nullIf(simp.data_exclusao_mei, '')) AS data_exclusao_mei,
        multiIf(
            simp.opcao_mei = 'S', 'MEI',
            simp.opcao_simples = 'S', 'SIMPLES',
            'OUTROS'
        ) AS tipo_simples_mei,

        -- Porte empresa corrigido (MEI sobrescreve)
        ifNull(pe.porte_empresa, emp.porte_empresa) AS porte_empresa,
        multiIf(
            simp.opcao_mei = 'S', 'MICRO EMPRESA',
            emp.porte_empresa = '00', 'Nao se aplica',
            emp.porte_empresa = '01', 'ME - Microempresa',
            emp.porte_empresa = '03', 'EPP - Empresa de Pequeno Porte',
            emp.porte_empresa = '05', 'Demais (Medio/Grande Porte)',
            ''
        ) AS desc_porteempresa_corrigido,
        multiIf(
            simp.opcao_mei = 'S', 'ATE R$ 81.000',
            emp.porte_empresa = '01', 'ATE R$ 360.000',
            emp.porte_empresa = '03', 'R$ 360.000 A R$ 4.800.000',
            emp.porte_empresa = '05', 'ACIMA DE R$ 4.800.000',
            ''
        ) AS faixa_faturamento_anual_ajustada,
        multiIf(
            dateDiff('year', parseDateTimeBestEffortOrNull(nullIf(e.data_inicio_atividade, '')), now()) <= 2, '0-2 ANOS',
            dateDiff('year', parseDateTimeBestEffortOrNull(nullIf(e.data_inicio_atividade, '')), now()) <= 5, '2-5 ANOS',
            dateDiff('year', parseDateTimeBestEffortOrNull(nullIf(e.data_inicio_atividade, '')), now()) <= 10, '5-10 ANOS',
            parseDateTimeBestEffortOrNull(nullIf(e.data_inicio_atividade, '')) IS NOT NULL, 'ACIMA DE 10 ANOS',
            ''
        ) AS faixa_idade_empresa,

        -- Regiao geografica
        multiIf(
            e.uf IN ('AC','AM','AP','PA','RO','RR','TO'), 'Norte',
            e.uf IN ('AL','BA','CE','MA','PB','PE','PI','RN','SE'), 'Nordeste',
            e.uf IN ('DF','GO','MT','MS'), 'Centro-Oeste',
            e.uf IN ('ES','MG','RJ','SP'), 'Sudeste',
            e.uf IN ('PR','RS','SC'), 'Sul',
            e.uf = 'EX', 'Exterior',
            'Nao Informado'
        ) AS regiao_geo,
        CASE WHEN e.uf != '' THEN e.uf ELSE 'nao informado' END AS uf_geo,

        toUInt64(rowNumberInAllBlocks() + 1) AS linha_unica

    FROM {estab} e
    LEFT JOIN {emp} emp ON trim(e.cnpj_basico) = trim(emp.cnpj_basico)
    LEFT JOIN {pivot} sp ON trim(e.cnpj_basico) = trim(sp.cnpj_basico)
    LEFT JOIN {simp} simp ON trim(e.cnpj_basico) = trim(simp.cnpj_basico)
    LEFT JOIN {DATABASE}.empresa_faixas_opt pe ON trim(e.cnpj_basico) = trim(pe.cnpj_basico)
    LEFT JOIN {DATABASE}.dict_cnae dc ON trim(e.cnae_fiscal_principal) = trim(dc.codigo_cnae)
    LEFT JOIN {DATABASE}.dict_municipios dm ON trim(e.municipio) = trim(dm.codigo_municipios)
    LEFT JOIN {DATABASE}.dict_naturezas dn ON trim(emp.natureza_juridica) = trim(dn.codigo_natureza_juridica)
    LEFT JOIN {DATABASE}.dict_qualificacoes dq ON trim(emp.qualificacao_responsavel) = trim(dq.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs1 ON trim(sp.qualificacao_socio_1) = trim(dqs1.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs2 ON trim(sp.qualificacao_socio_2) = trim(dqs2.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs3 ON trim(sp.qualificacao_socio_3) = trim(dqs3.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr1 ON trim(sp.qualificacao_representante_1) = trim(dqr1.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr2 ON trim(sp.qualificacao_representante_2) = trim(dqr2.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr3 ON trim(sp.qualificacao_representante_3) = trim(dqr3.codigo_qualificacao_socio)
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe1 ON trim(sp.faixa_etaria_1) = trim(dfe1.codigo)
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe2 ON trim(sp.faixa_etaria_2) = trim(dfe2.codigo)
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe3 ON trim(sp.faixa_etaria_3) = trim(dfe3.codigo)
    {where_clause}
    SETTINGS join_use_nulls = 1
    """


def create_final_table(client, periodo, periodo_antigo=None):
    """Cria a tabela final com INSERT por UF para evitar limite de particoes."""

    # Criar pivot de socios primeiro
    _create_socios_pivot(client, periodo)

    # Arquiva a tabela viva existente com o sufixo do ano-mes dela
    archive_table = _archive_table_name(periodo, periodo_antigo)
    print("[Transform] Verificando tabela existente para arquivamento...")
    existing = client.query(
        f"SELECT count() FROM system.tables "
        f"WHERE database = '{DATABASE}' AND name = '{FINAL_TABLE_NAME}'"
    )
    if existing.result_rows[0][0] > 0:
        client.command(f"DROP TABLE IF EXISTS {archive_table}")
        client.command(f"RENAME TABLE {FINAL_TABLE} TO {archive_table}")
        print(f"[Transform] Tabela anterior arquivada como: {archive_table}")

    # Criar tabela vazia com DDL explicito
    print("[Transform] Criando tabela final...")
    client.command(_get_ddl())
    print("[Transform] Tabela vazia criada. Inserindo dados por UF...")

    # INSERT por UF para respeitar limite de particoes
    total = 0
    for uf in UFS:
        select_sql = _get_select_sql(periodo, uf_filter=uf)
        insert_sql = f"INSERT INTO {FINAL_TABLE} {select_sql}"
        client.command(insert_sql)

        count = client.query(
            f"SELECT count() FROM {FINAL_TABLE} WHERE uf = '{uf}'"
        ).result_rows[0][0]
        total += count
        if count > 0:
            print(f"  UF {uf}: {count:,} registros")

    print(f"[Transform] Tabela final criada: {total:,} registros total")
    return True


def main(periodo, periodo_antigo=None):
    """Executa criacao da tabela final."""
    client = get_clickhouse_client()
    if client is None:
        print("[Transform] Sem conexao com ClickHouse. Pulando transformacao.")
        return False

    try:
        create_final_table(client, periodo, periodo_antigo)
        print("[Transform] Transformacao concluida com sucesso!")
        return True

    except Exception as e:
        print(f"[Transform] ERRO na transformacao: {e}")
        archive_table = _archive_table_name(periodo, periodo_antigo)
        archive_name = archive_table.split(".", 1)[-1]
        try:
            existing_new = client.query(
                f"SELECT count() FROM system.tables "
                f"WHERE database = '{DATABASE}' AND name = '{FINAL_TABLE_NAME}'"
            )
            existing_bkp = client.query(
                f"SELECT count() FROM system.tables "
                f"WHERE database = '{DATABASE}' AND name = '{archive_name}'"
            )
            if existing_bkp.result_rows[0][0] > 0:
                if existing_new.result_rows[0][0] > 0:
                    client.command(f"DROP TABLE {FINAL_TABLE}")
                client.command(f"RENAME TABLE {archive_table} TO {FINAL_TABLE}")
                print(f"[Transform] Tabela {archive_table} restaurada com sucesso apos erro.")
            else:
                print("[Transform] ATENCAO: Nenhuma tabela arquivada encontrada para restaurar!")
        except Exception as restore_err:
            print(f"[Transform] CRITICO: Falha ao restaurar tabela arquivada: {restore_err}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: transform_receita.py <periodo> [periodo_antigo]")
        print("  ex: transform_receita.py 202603 202602")
        sys.exit(1)

    periodo = sys.argv[1]
    periodo_antigo = sys.argv[2] if len(sys.argv) > 2 else None
    success = main(periodo, periodo_antigo)
    if not success:
        sys.exit(1)
