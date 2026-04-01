"""
Transformacao: cria pivot de socios e tabela final mt_empresa_socios_enriquecido.
Uso: python transform_receita.py <periodo>
  ex: python transform_receita.py 202603
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
FINAL_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"


def create_socios_pivot(client, periodo):
    """Cria tabela de socios pivoteada (top 3 por empresa)."""
    socios_table = f"{DATABASE}.socios_{periodo}"
    pivot_table = f"{DATABASE}.socios_{periodo}_pivot"

    print(f"[Transform] Criando pivot de socios: {pivot_table}")

    client.command(f"DROP TABLE IF EXISTS {pivot_table}")

    sql = f"""
    CREATE TABLE {pivot_table}
    ENGINE = MergeTree
    ORDER BY cnpj_basico
    AS
    SELECT
        cnpj_basico,
        groupArray(nome_socio_razao_social)[1]          AS nome_socio_1,
        groupArray(cpf_cnpj_socio)[1]                   AS cpf_cnpj_socio_1,
        groupArray(identificador_socio)[1]               AS identificador_socio_1,
        groupArray(qualificacao_socio)[1]                AS qualificacao_socio_1,
        groupArray(data_entrada_sociedade)[1]            AS data_entrada_sociedade_1,
        groupArray(faixa_etaria)[1]                      AS faixa_etaria_1,
        groupArray(nome_representante)[1]                AS nome_representante_1,
        groupArray(qualificacao_representante_legal)[1]  AS qualificacao_representante_1,

        groupArray(nome_socio_razao_social)[2]           AS nome_socio_2,
        groupArray(cpf_cnpj_socio)[2]                    AS cpf_cnpj_socio_2,
        groupArray(identificador_socio)[2]               AS identificador_socio_2,
        groupArray(qualificacao_socio)[2]                AS qualificacao_socio_2,
        groupArray(data_entrada_sociedade)[2]            AS data_entrada_sociedade_2,
        groupArray(faixa_etaria)[2]                      AS faixa_etaria_2,
        groupArray(nome_representante)[2]                AS nome_representante_2,
        groupArray(qualificacao_representante_legal)[2]  AS qualificacao_representante_2,

        groupArray(nome_socio_razao_social)[3]           AS nome_socio_3,
        groupArray(cpf_cnpj_socio)[3]                    AS cpf_cnpj_socio_3,
        groupArray(identificador_socio)[3]               AS identificador_socio_3,
        groupArray(qualificacao_socio)[3]                AS qualificacao_socio_3,
        groupArray(data_entrada_sociedade)[3]            AS data_entrada_sociedade_3,
        groupArray(faixa_etaria)[3]                      AS faixa_etaria_3,
        groupArray(nome_representante)[3]                AS nome_representante_3,
        groupArray(qualificacao_representante_legal)[3]  AS qualificacao_representante_3
    FROM {socios_table}
    GROUP BY cnpj_basico
    """
    client.command(sql)

    count = client.query(f"SELECT count() FROM {pivot_table}").result_rows[0][0]
    print(f"[Transform] Pivot criado: {count:,} empresas com socios")
    return True


def create_final_table(client, periodo):
    """Cria a tabela final mt_empresa_socios_enriquecido_nov."""
    estab = f"{DATABASE}.estabelecimentos_{periodo}"
    emp = f"{DATABASE}.empresas_{periodo}"
    simp = f"{DATABASE}.simples_{periodo}"
    pivot = f"{DATABASE}.socios_{periodo}_pivot"

    # Backup da tabela existente com sufixo _YYYYMM_bkp
    backup_table = f"{FINAL_TABLE}_{periodo}_bkp"
    print("[Transform] Verificando tabela existente para backup...")
    existing = client.query(
        f"SELECT count() FROM system.tables "
        f"WHERE database = '{DATABASE}' AND name = 'mt_empresa_socios_enriquecido_nov'"
    )
    if existing.result_rows[0][0] > 0:
        client.command(f"DROP TABLE IF EXISTS {backup_table}")
        client.command(f"RENAME TABLE {FINAL_TABLE} TO {backup_table}")
        print(f"[Transform] Backup criado: {backup_table}")

    print("[Transform] Criando tabela final...")

    # Primeiro cria a tabela vazia, depois insere com setting de partições
    select_sql = f"""
    SELECT
        -- CNPJ completo
        concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
        e.cnpj_basico AS cnpj_basico,
        e.cnpj_ordem AS cnpj_ordem,
        e.cnpj_dv AS cnpj_dv,

        -- Matriz/Filial
        e.identificador_matriz_filial AS matriz_filial,
        CASE e.identificador_matriz_filial
            WHEN '1' THEN 'Matriz'
            WHEN '2' THEN 'Filial'
            ELSE ''
        END AS desc_matriz_filial,

        e.nome_fantasia,

        -- Situacao cadastral
        e.situacao_cadastral,
        CASE e.situacao_cadastral
            WHEN '01' THEN 'Nula'
            WHEN '02' THEN 'Ativa'
            WHEN '03' THEN 'Suspensa'
            WHEN '04' THEN 'Inapta'
            WHEN '08' THEN 'Baixada'
            ELSE ''
        END AS desc_situacao_cadastral,

        parseDateTimeBestEffortOrNull(e.data_situacao_cadastral) AS data_situacao_cadastral,
        e.motivo_situacao_cadastral,
        e.nome_cidade_exterior AS cidade_exterior,
        e.pais,
        parseDateTimeBestEffortOrNull(e.data_inicio_atividade) AS data_inicio_atividade,

        -- CNAE
        e.cnae_fiscal_principal AS cnae_principal,
        dc.descricao_cnae AS desc_cnae_principal,
        e.cnae_fiscal_secundaria AS cnae_secundario,

        -- Endereco
        e.tipo_logradouro,
        e.logradouro,
        e.numero,
        e.complemento,
        e.bairro,
        e.cep,
        e.uf AS uf,
        e.municipio AS municipio,
        dm.descricao_municipios AS desc_municipio,

        -- Contato
        e.ddd_1,
        e.telefone_1,
        e.ddd_2,
        e.telefone_2,
        e.ddd_fax,
        e.fax,
        e.correio_eletronico AS email,
        e.situacao_especial,
        parseDateTimeBestEffortOrNull(e.data_situacao_especial) AS data_situacao_especial,

        -- Geocoding (NULL por enquanto - preenchido por outro processo)
        CAST(NULL AS Nullable(Float32)) AS latitude,
        CAST(NULL AS Nullable(Float32)) AS longitude,

        -- Dados da empresa
        emp.razao_social,
        emp.natureza_juridica,
        dn.descricao_natureza_juridica AS desc_natureza_juridica,
        emp.qualificacao_responsavel,
        dq.descricao_qualificacao_socio AS desc_qualificacao_responsavel,
        toDecimal64OrNull(emp.capital_social, 2) AS capital_social,
        emp.porte_empresa AS porte,

        -- Porte descricao
        CASE emp.porte_empresa
            WHEN '00' THEN 'Nao informado'
            WHEN '01' THEN 'Micro Empresa'
            WHEN '03' THEN 'Empresa de Pequeno Porte'
            WHEN '05' THEN 'Demais'
            ELSE ''
        END AS desc_porte,

        emp.ente_federativo_responsavel,

        -- Socios (pivot - top 3)
        ifNull(sp.nome_socio_1, '') AS nome_socio_1,
        ifNull(sp.cpf_cnpj_socio_1, '') AS cpf_cnpj_socio_1,
        ifNull(sp.identificador_socio_1, '') AS identificador_socio_1,
        CASE sp.identificador_socio_1
            WHEN '1' THEN 'Pessoa Juridica'
            WHEN '2' THEN 'Pessoa Fisica'
            WHEN '3' THEN 'Estrangeiro'
            ELSE ''
        END AS desc_identificador_socio_1,
        ifNull(sp.qualificacao_socio_1, '') AS qualificacao_socio_1,
        dqs1.descricao_qualificacao_socio AS desc_qualificacao_socio_1,
        parseDateTimeBestEffortOrNull(sp.data_entrada_sociedade_1) AS data_entrada_sociedade_1,
        ifNull(sp.faixa_etaria_1, '') AS faixa_etaria_1,
        dfe1.descricao AS desc_faixa_etaria_1,
        ifNull(sp.nome_representante_1, '') AS nome_representante_1,
        ifNull(sp.qualificacao_representante_1, '') AS qualificacao_representante_1,
        dqr1.descricao_qualificacao_socio AS desc_qualificacao_representante_1,

        ifNull(sp.nome_socio_2, '') AS nome_socio_2,
        ifNull(sp.cpf_cnpj_socio_2, '') AS cpf_cnpj_socio_2,
        ifNull(sp.identificador_socio_2, '') AS identificador_socio_2,
        CASE sp.identificador_socio_2
            WHEN '1' THEN 'Pessoa Juridica'
            WHEN '2' THEN 'Pessoa Fisica'
            WHEN '3' THEN 'Estrangeiro'
            ELSE ''
        END AS desc_identificador_socio_2,
        ifNull(sp.qualificacao_socio_2, '') AS qualificacao_socio_2,
        dqs2.descricao_qualificacao_socio AS desc_qualificacao_socio_2,
        parseDateTimeBestEffortOrNull(sp.data_entrada_sociedade_2) AS data_entrada_sociedade_2,
        ifNull(sp.faixa_etaria_2, '') AS faixa_etaria_2,
        dfe2.descricao AS desc_faixa_etaria_2,
        ifNull(sp.nome_representante_2, '') AS nome_representante_2,
        ifNull(sp.qualificacao_representante_2, '') AS qualificacao_representante_2,
        dqr2.descricao_qualificacao_socio AS desc_qualificacao_representante_2,

        ifNull(sp.nome_socio_3, '') AS nome_socio_3,
        ifNull(sp.cpf_cnpj_socio_3, '') AS cpf_cnpj_socio_3,
        ifNull(sp.identificador_socio_3, '') AS identificador_socio_3,
        CASE sp.identificador_socio_3
            WHEN '1' THEN 'Pessoa Juridica'
            WHEN '2' THEN 'Pessoa Fisica'
            WHEN '3' THEN 'Estrangeiro'
            ELSE ''
        END AS desc_identificador_socio_3,
        ifNull(sp.qualificacao_socio_3, '') AS qualificacao_socio_3,
        dqs3.descricao_qualificacao_socio AS desc_qualificacao_socio_3,
        parseDateTimeBestEffortOrNull(sp.data_entrada_sociedade_3) AS data_entrada_sociedade_3,
        ifNull(sp.faixa_etaria_3, '') AS faixa_etaria_3,
        dfe3.descricao AS desc_faixa_etaria_3,
        ifNull(sp.nome_representante_3, '') AS nome_representante_3,
        ifNull(sp.qualificacao_representante_3, '') AS qualificacao_representante_3,
        dqr3.descricao_qualificacao_socio AS desc_qualificacao_representante_3,

        -- Simples/MEI
        ifNull(s.opcao_simples, '') AS opcao_simples,
        parseDateTimeBestEffortOrNull(s.data_opcao_simples) AS data_opcao_simples,
        parseDateTimeBestEffortOrNull(s.data_exclusao_simples) AS data_exclusao_simples,
        ifNull(s.opcao_mei, '') AS opcao_mei,
        parseDateTimeBestEffortOrNull(s.data_opcao_mei) AS data_opcao_mei,
        parseDateTimeBestEffortOrNull(s.data_exclusao_mei) AS data_exclusao_mei,

        -- tipo_simples_mei
        CASE
            WHEN s.opcao_simples = 'S' AND s.opcao_mei = 'S' AND emp.porte_empresa = '04' THEN 'MEI'
            WHEN s.opcao_simples = 'S' AND s.opcao_mei = 'S' THEN 'Simples'
            ELSE ''
        END AS tipo_simples_mei,

        -- Porte empresa corrigido (com MEI)
        CASE
            WHEN emp.porte_empresa = '04' OR (s.opcao_mei = 'S' AND s.opcao_simples = 'S' AND emp.porte_empresa IN ('00', '01')) THEN '04'
            WHEN emp.porte_empresa = '00' AND s.opcao_simples = 'S' THEN '01'
            ELSE emp.porte_empresa
        END AS porte_empresa,

        CASE
            WHEN emp.porte_empresa = '04' OR (s.opcao_mei = 'S' AND s.opcao_simples = 'S' AND emp.porte_empresa IN ('00', '01')) THEN 'MEI - Microempreendedor Individual'
            WHEN emp.porte_empresa = '01' OR (emp.porte_empresa = '00' AND s.opcao_simples = 'S') THEN 'Micro Empresa'
            WHEN emp.porte_empresa = '03' THEN 'Empresa de Pequeno Porte'
            WHEN emp.porte_empresa = '05' THEN 'Demais'
            WHEN emp.porte_empresa = '00' THEN 'Nao informado'
            ELSE 'Nao informado'
        END AS desc_porteempresa_corrigido,

        CASE
            WHEN emp.porte_empresa = '04' OR (s.opcao_mei = 'S' AND s.opcao_simples = 'S' AND emp.porte_empresa IN ('00', '01')) THEN 'ate 81.000,00'
            WHEN emp.porte_empresa = '01' OR (emp.porte_empresa = '00' AND s.opcao_simples = 'S') THEN 'de 81.000,00 a 360.000,00'
            WHEN emp.porte_empresa = '03' THEN '360.000,01 a 4.800.000,00'
            WHEN emp.porte_empresa = '05' THEN 'acima de 4.800.000,00'
            ELSE 'nao informado'
        END AS faixa_faturamento_anual_ajustada,

        -- Faixa idade empresa
        CASE
            WHEN dateDiff('year', parseDateTimeBestEffortOrNull(e.data_inicio_atividade), now()) <= 2 THEN '0-2'
            WHEN dateDiff('year', parseDateTimeBestEffortOrNull(e.data_inicio_atividade), now()) <= 5 THEN '3-5'
            WHEN dateDiff('year', parseDateTimeBestEffortOrNull(e.data_inicio_atividade), now()) <= 10 THEN '6-10'
            WHEN dateDiff('year', parseDateTimeBestEffortOrNull(e.data_inicio_atividade), now()) <= 20 THEN '11-20'
            WHEN parseDateTimeBestEffortOrNull(e.data_inicio_atividade) IS NOT NULL THEN '>20'
            ELSE ''
        END AS faixa_idade_empresa,

        -- Regiao geografica
        CASE e.uf
            WHEN 'AC' THEN 'Norte'    WHEN 'AM' THEN 'Norte'   WHEN 'AP' THEN 'Norte'
            WHEN 'PA' THEN 'Norte'    WHEN 'RO' THEN 'Norte'   WHEN 'RR' THEN 'Norte'
            WHEN 'TO' THEN 'Norte'
            WHEN 'AL' THEN 'Nordeste' WHEN 'BA' THEN 'Nordeste' WHEN 'CE' THEN 'Nordeste'
            WHEN 'MA' THEN 'Nordeste' WHEN 'PB' THEN 'Nordeste' WHEN 'PE' THEN 'Nordeste'
            WHEN 'PI' THEN 'Nordeste' WHEN 'RN' THEN 'Nordeste' WHEN 'SE' THEN 'Nordeste'
            WHEN 'DF' THEN 'Centro-Oeste' WHEN 'GO' THEN 'Centro-Oeste'
            WHEN 'MS' THEN 'Centro-Oeste' WHEN 'MT' THEN 'Centro-Oeste'
            WHEN 'ES' THEN 'Sudeste'  WHEN 'MG' THEN 'Sudeste'
            WHEN 'RJ' THEN 'Sudeste'  WHEN 'SP' THEN 'Sudeste'
            WHEN 'PR' THEN 'Sul'      WHEN 'RS' THEN 'Sul'     WHEN 'SC' THEN 'Sul'
            WHEN 'EX' THEN 'Exterior'
            ELSE 'Nao Informado'
        END AS regiao_geo,

        CASE WHEN e.uf != '' THEN e.uf ELSE 'nao informado' END AS uf_geo,

        -- LinkedIn
        lk.id AS linkedin_id,
        lk.date_created AS linkedin_date_created,
        lk.ceo AS linkedin_ceo,
        lk.comercial AS linkedin_comercial,
        lk.compras AS linkedin_compras,
        lk.coordenator AS linkedin_coordenator,
        lk.diretor AS linkedin_diretor,
        lk.gerente AS linkedin_gerente,
        lk.representante AS linkedin_representante,
        lk.socio AS linkedin_socio,
        lk.score AS linkedin_score,
        lk.found_in_title AS linkedin_found_in_title,
        lk.person_name AS linkedin_person_name,
        lk.occupation AS linkedin_occupation,
        lk.company_found AS linkedin_company_found,
        lk.url AS linkedin_url,
        lk.company_name AS linkedin_company_name,
        lk.title AS linkedin_title,
        lk.description AS linkedin_description,
        lk.sub_title AS linkedin_sub_title,
        lk.search_key AS linkedin_search_key,
        lk.search_url AS linkedin_search_url,
        lk.page_index AS linkedin_page_index,
        ifNull(lk.cnpj, '') AS linkedin_cnpj,
        lk.is_leader AS linkedin_is_leader,
        lk.id1 AS linkedin_id1,
        lk.id2 AS linkedin_id2,
        lk.id3 AS linkedin_id3,
        lk.score1 AS linkedin_score1,
        lk.score2 AS linkedin_score2,
        lk.score3 AS linkedin_score3,
        lk.person_name1 AS linkedin_person_name1,
        lk.person_name2 AS linkedin_person_name2,
        lk.person_name3 AS linkedin_person_name3,
        lk.occupation1 AS linkedin_occupation1,
        lk.occupation2 AS linkedin_occupation2,
        lk.occupation3 AS linkedin_occupation3,
        lk.company_name1 AS linkedin_company_name1,
        lk.company_name2 AS linkedin_company_name2,
        lk.company_name3 AS linkedin_company_name3,
        lk.url1 AS linkedin_url1,
        lk.url2 AS linkedin_url2,
        lk.url3 AS linkedin_url3,

        -- linha unica (row number)
        rowNumberInAllBlocks() + 1 AS linha_unica

    FROM {estab} e
    LEFT JOIN {emp} emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN {simp} s ON e.cnpj_basico = s.cnpj_basico
    LEFT JOIN {pivot} sp ON e.cnpj_basico = sp.cnpj_basico
    LEFT JOIN {DATABASE}.dict_cnae dc ON e.cnae_fiscal_principal = dc.codigo_cnae
    LEFT JOIN {DATABASE}.dict_municipios dm ON e.municipio = dm.codigo_municipios
    LEFT JOIN {DATABASE}.dict_naturezas dn ON emp.natureza_juridica = dn.codigo_natureza_juridica
    LEFT JOIN {DATABASE}.dict_qualificacoes dq ON emp.qualificacao_responsavel = dq.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs1 ON sp.qualificacao_socio_1 = dqs1.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs2 ON sp.qualificacao_socio_2 = dqs2.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs3 ON sp.qualificacao_socio_3 = dqs3.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr1 ON sp.qualificacao_representante_1 = dqr1.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr2 ON sp.qualificacao_representante_2 = dqr2.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr3 ON sp.qualificacao_representante_3 = dqr3.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe1 ON sp.faixa_etaria_1 = dfe1.codigo
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe2 ON sp.faixa_etaria_2 = dfe2.codigo
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe3 ON sp.faixa_etaria_3 = dfe3.codigo
    LEFT JOIN {DATABASE}.dict_identificador_socio dis1 ON sp.identificador_socio_1 = dis1.codigo
    LEFT JOIN {DATABASE}.linkedin lk ON concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) = lk.cnpj
    WHERE e.situacao_cadastral = '02'
    """

    # Criar tabela e inserir dados em um unico comando
    create_sql = f"""
    CREATE TABLE {FINAL_TABLE}
    ENGINE = MergeTree
    ORDER BY (uf, cnpj_basico)
    SETTINGS index_granularity = 8192
    AS {select_sql}
    """
    client.command(create_sql)

    # Validacao
    result = client.query(f"SELECT count() FROM {FINAL_TABLE}")
    total = result.result_rows[0][0]
    print(f"[Transform] Tabela final criada: {total:,} registros")

    return True


def main(periodo):
    """Executa pivot de socios + criacao da tabela final."""
    client = get_clickhouse_client()
    if client is None:
        print("[Transform] Sem conexao com ClickHouse. Pulando transformacao.")
        return False

    try:
        # 1. Pivot de socios
        create_socios_pivot(client, periodo)

        # 2. Tabela final
        create_final_table(client, periodo)

        print("[Transform] Transformacao concluida com sucesso!")
        return True

    except Exception as e:
        print(f"[Transform] ERRO na transformacao: {e}")
        # Restaurar backup automaticamente
        backup_table = f"{FINAL_TABLE}_{periodo}_bkp"
        try:
            existing_new = client.query(
                f"SELECT count() FROM system.tables "
                f"WHERE database = '{DATABASE}' AND name = 'mt_empresa_socios_enriquecido_nov'"
            )
            existing_bkp = client.query(
                f"SELECT count() FROM system.tables "
                f"WHERE database = '{DATABASE}' "
                f"AND name = 'mt_empresa_socios_enriquecido_nov_{periodo}_bkp'"
            )
            if existing_bkp.result_rows[0][0] > 0:
                if existing_new.result_rows[0][0] > 0:
                    client.command(f"DROP TABLE {FINAL_TABLE}")
                client.command(f"RENAME TABLE {backup_table} TO {FINAL_TABLE}")
                print(f"[Transform] Backup {backup_table} restaurado com sucesso apos erro.")
            else:
                print("[Transform] ATENCAO: Nenhum backup encontrado para restaurar!")
        except Exception as restore_err:
            print(f"[Transform] CRITICO: Falha ao restaurar backup: {restore_err}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: transform_receita.py <periodo>")
        print("  ex: transform_receita.py 202603")
        sys.exit(1)

    periodo = sys.argv[1]
    success = main(periodo)
    if not success:
        sys.exit(1)
