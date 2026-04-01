"""
Transformacao: cria tabela final mt_empresa_socios_enriquecido_nov.
Uso: python transform_receita.py <periodo>
  ex: python transform_receita.py 202603
"""
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_clickhouse import get_clickhouse_client

DATABASE = "empresas_ativas_do_brasil"
FINAL_TABLE = f"{DATABASE}.mt_empresa_socios_enriquecido_nov"

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
        `linkedin_id` Nullable(Decimal(38, 19)),
        `linkedin_date_created` Nullable(DateTime64(6)),
        `linkedin_ceo` Nullable(UInt8),
        `linkedin_comercial` Nullable(UInt8),
        `linkedin_compras` Nullable(UInt8),
        `linkedin_coordenator` Nullable(UInt8),
        `linkedin_diretor` Nullable(UInt8),
        `linkedin_gerente` Nullable(UInt8),
        `linkedin_representante` Nullable(UInt8),
        `linkedin_socio` Nullable(UInt8),
        `linkedin_score` Nullable(Decimal(38, 19)),
        `linkedin_found_in_title` Nullable(UInt8),
        `linkedin_person_name` Nullable(String),
        `linkedin_occupation` Nullable(String),
        `linkedin_company_found` Nullable(String),
        `linkedin_url` Nullable(String),
        `linkedin_company_name` Nullable(String),
        `linkedin_title` Nullable(String),
        `linkedin_description` Nullable(String),
        `linkedin_sub_title` Nullable(String),
        `linkedin_search_key` Nullable(String),
        `linkedin_search_url` Nullable(String),
        `linkedin_page_index` Nullable(Int64),
        `linkedin_cnpj` String,
        `linkedin_is_leader` Nullable(UInt8),
        `linkedin_id1` Nullable(Int64),
        `linkedin_id2` Nullable(Int64),
        `linkedin_id3` Nullable(Int64),
        `linkedin_score1` Nullable(Decimal(38, 19)),
        `linkedin_score2` Nullable(Decimal(38, 19)),
        `linkedin_score3` Nullable(Decimal(38, 19)),
        `linkedin_person_name1` Nullable(String),
        `linkedin_person_name2` Nullable(String),
        `linkedin_person_name3` Nullable(String),
        `linkedin_occupation1` Nullable(String),
        `linkedin_occupation2` Nullable(String),
        `linkedin_occupation3` Nullable(String),
        `linkedin_company_name1` Nullable(String),
        `linkedin_company_name2` Nullable(String),
        `linkedin_company_name3` Nullable(String),
        `linkedin_url1` Nullable(String),
        `linkedin_url2` Nullable(String),
        `linkedin_url3` Nullable(String),
        `link_id` Nullable(Int64),
        `link_date_created` Nullable(DateTime64(6)),
        `link_score` Nullable(Decimal(38, 19)),
        `link_found_in_title` Nullable(Int32),
        `link_person_name` Nullable(String),
        `link_occupation` Nullable(String),
        `link_company_found` Nullable(String),
        `link_url` Nullable(String),
        `link_company_name` Nullable(String),
        `link_title` Nullable(String),
        `link_description` Nullable(String),
        `link_sub_title` Nullable(String),
        `link_search_key` Nullable(String),
        `link_search_url` Nullable(String),
        `link_cnpj` Nullable(String),
        `lc_url` Nullable(String),
        `lc_company_name` Nullable(String),
        `lc_cnpj_linkedin_total` Nullable(String),
        `lc_person_name` Nullable(String),
        `lc_occupation` Nullable(String),
        `linha_unica` UInt64
    )
    ENGINE = MergeTree
    PARTITION BY uf
    ORDER BY (uf, cnpj_basico)
    SETTINGS index_granularity = 8192
    """


def _get_select_sql(periodo, uf_filter=None):
    """SELECT baseado na view original vw_empresa_socios_mat_nov."""
    estab = f"{DATABASE}.estabelecimentos_{periodo}"
    emp = f"{DATABASE}.empresas_{periodo}"
    soc = f"{DATABASE}.socios_{periodo}"
    simp = f"{DATABASE}.simples_{periodo}"

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
        parseDateTimeBestEffortOrNull(e.data_situacao_cadastral) AS data_situacao_cadastral,
        e.motivo_situacao_cadastral,
        e.nome_cidade_exterior AS cidade_exterior,
        e.pais,
        parseDateTimeBestEffortOrNull(e.data_inicio_atividade) AS data_inicio_atividade,
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
        parseDateTimeBestEffortOrNull(e.data_situacao_especial) AS data_situacao_especial,

        -- Latitude/Longitude do geocoding (vem do estabelecimento se existir)
        CAST(NULL AS Nullable(Float32)) AS latitude,
        CAST(NULL AS Nullable(Float32)) AS longitude,

        -- Empresa
        emp.razao_social,
        emp.natureza_juridica,
        dn.descricao_natureza_juridica AS desc_natureza_juridica,
        emp.qualificacao_responsavel,
        dq.descricao_qualificacao_socio AS desc_qualificacao_responsavel,
        toDecimal64OrNull(emp.capital_social, 2) AS capital_social,
        emp.porte_empresa AS porte,
        multiIf(
            emp.porte_empresa = '00', 'Nao informado',
            emp.porte_empresa = '01', 'Micro empresa',
            emp.porte_empresa = '03', 'Empresa de pequeno porte',
            emp.porte_empresa = '05', 'Demais',
            ''
        ) AS desc_porte,
        emp.ente_federativo_responsavel,

        -- Socios (pivot inline top 3)
        groupArray(soc.nome_socio_razao_social)[1] AS nome_socio_1,
        groupArray(soc.cpf_cnpj_socio)[1] AS cpf_cnpj_socio_1,
        groupArray(soc.identificador_socio)[1] AS identificador_socio_1,
        multiIf(
            groupArray(soc.identificador_socio)[1] = '1', 'Pessoa Juridica',
            groupArray(soc.identificador_socio)[1] = '2', 'Pessoa Fisica',
            groupArray(soc.identificador_socio)[1] = '3', 'Estrangeiro',
            ''
        ) AS desc_identificador_socio_1,
        groupArray(soc.qualificacao_socio)[1] AS qualificacao_socio_1,
        dqs1.descricao_qualificacao_socio AS desc_qualificacao_socio_1,
        parseDateTimeBestEffortOrNull(groupArray(soc.data_entrada_sociedade)[1]) AS data_entrada_sociedade_1,
        groupArray(soc.faixa_etaria)[1] AS faixa_etaria_1,
        dfe1.descricao AS desc_faixa_etaria_1,
        groupArray(soc.nome_representante)[1] AS nome_representante_1,
        groupArray(soc.qualificacao_representante_legal)[1] AS qualificacao_representante_1,
        dqr1.descricao_qualificacao_socio AS desc_qualificacao_representante_1,

        groupArray(soc.nome_socio_razao_social)[2] AS nome_socio_2,
        groupArray(soc.cpf_cnpj_socio)[2] AS cpf_cnpj_socio_2,
        groupArray(soc.identificador_socio)[2] AS identificador_socio_2,
        multiIf(
            groupArray(soc.identificador_socio)[2] = '1', 'Pessoa Juridica',
            groupArray(soc.identificador_socio)[2] = '2', 'Pessoa Fisica',
            groupArray(soc.identificador_socio)[2] = '3', 'Estrangeiro',
            ''
        ) AS desc_identificador_socio_2,
        groupArray(soc.qualificacao_socio)[2] AS qualificacao_socio_2,
        dqs2.descricao_qualificacao_socio AS desc_qualificacao_socio_2,
        parseDateTimeBestEffortOrNull(groupArray(soc.data_entrada_sociedade)[2]) AS data_entrada_sociedade_2,
        groupArray(soc.faixa_etaria)[2] AS faixa_etaria_2,
        dfe2.descricao AS desc_faixa_etaria_2,
        groupArray(soc.nome_representante)[2] AS nome_representante_2,
        groupArray(soc.qualificacao_representante_legal)[2] AS qualificacao_representante_2,
        dqr2.descricao_qualificacao_socio AS desc_qualificacao_representante_2,

        groupArray(soc.nome_socio_razao_social)[3] AS nome_socio_3,
        groupArray(soc.cpf_cnpj_socio)[3] AS cpf_cnpj_socio_3,
        groupArray(soc.identificador_socio)[3] AS identificador_socio_3,
        multiIf(
            groupArray(soc.identificador_socio)[3] = '1', 'Pessoa Juridica',
            groupArray(soc.identificador_socio)[3] = '2', 'Pessoa Fisica',
            groupArray(soc.identificador_socio)[3] = '3', 'Estrangeiro',
            ''
        ) AS desc_identificador_socio_3,
        groupArray(soc.qualificacao_socio)[3] AS qualificacao_socio_3,
        dqs3.descricao_qualificacao_socio AS desc_qualificacao_socio_3,
        parseDateTimeBestEffortOrNull(groupArray(soc.data_entrada_sociedade)[3]) AS data_entrada_sociedade_3,
        groupArray(soc.faixa_etaria)[3] AS faixa_etaria_3,
        dfe3.descricao AS desc_faixa_etaria_3,
        groupArray(soc.nome_representante)[3] AS nome_representante_3,
        groupArray(soc.qualificacao_representante_legal)[3] AS qualificacao_representante_3,
        dqr3.descricao_qualificacao_socio AS desc_qualificacao_representante_3,

        -- Simples / MEI
        any(simp.opcao_simples) AS opcao_simples,
        parseDateTimeBestEffortOrNull(any(simp.data_opcao_simples)) AS data_opcao_simples,
        parseDateTimeBestEffortOrNull(any(simp.data_exclusao_simples)) AS data_exclusao_simples,
        any(simp.opcao_mei) AS opcao_mei,
        parseDateTimeBestEffortOrNull(any(simp.data_opcao_mei)) AS data_opcao_mei,
        parseDateTimeBestEffortOrNull(any(simp.data_exclusao_mei)) AS data_exclusao_mei,
        multiIf(
            (parseDateTimeBestEffortOrNull(any(simp.data_exclusao_mei)) IS NOT NULL)
                AND (parseDateTimeBestEffortOrNull(any(simp.data_exclusao_simples)) IS NOT NULL), '',
            (any(simp.opcao_mei) = 'S') AND (any(simp.opcao_simples) = 'S'),
                multiIf(
                    parseDateTimeBestEffortOrNull(any(simp.data_exclusao_mei)) IS NOT NULL, 'Simples',
                    parseDateTimeBestEffortOrNull(any(simp.data_exclusao_simples)) IS NOT NULL, 'MEI',
                    'MEI'
                ),
            any(simp.opcao_mei) = 'S', 'MEI',
            any(simp.opcao_simples) = 'S', 'Simples',
            ''
        ) AS tipo_simples_mei,

        -- Porte empresa (de empresa_faixas_opt)
        any(pe.porte_empresa) AS porte_empresa,
        multiIf(
            any(pe.porte_empresa) = '00', 'Nao se aplica',
            any(pe.porte_empresa) = '01', 'ME - Microempresa',
            any(pe.porte_empresa) = '03', 'EPP - Empresa de Pequeno Porte',
            any(pe.porte_empresa) = '05', 'Demais (Medio/Grande Porte)',
            ''
        ) AS desc_porteempresa_corrigido,
        any(pe.faixa_faturamento_anual) AS faixa_faturamento_anual_ajustada,
        any(pe.faixa_idade_empresa) AS faixa_idade_empresa,

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

        -- LinkedIn (linkedin_completo_opt como lk)
        any(lk.id) AS linkedin_id,
        any(lk.date_created) AS linkedin_date_created,
        any(lk.ceo) AS linkedin_ceo,
        any(lk.comercial) AS linkedin_comercial,
        any(lk.compras) AS linkedin_compras,
        any(lk.coordenator) AS linkedin_coordenator,
        any(lk.diretor) AS linkedin_diretor,
        any(lk.gerente) AS linkedin_gerente,
        any(lk.representante) AS linkedin_representante,
        any(lk.socio) AS linkedin_socio,
        any(lk.score) AS linkedin_score,
        any(lk.found_in_title) AS linkedin_found_in_title,
        any(lk.person_name) AS linkedin_person_name,
        any(lk.occupation) AS linkedin_occupation,
        any(lk.company_found) AS linkedin_company_found,
        any(lk.url) AS linkedin_url,
        any(lk.company_name) AS linkedin_company_name,
        any(lk.title) AS linkedin_title,
        any(lk.description) AS linkedin_description,
        any(lk.sub_title) AS linkedin_sub_title,
        any(lk.search_key) AS linkedin_search_key,
        any(lk.search_url) AS linkedin_search_url,
        any(lk.page_index) AS linkedin_page_index,
        ifNull(any(lk.cnpj), '') AS linkedin_cnpj,
        multiIf(any(lk.url) IS NOT NULL, 1, 0) AS linkedin_is_leader,
        coalesce(groupArray(lk.id)[1], 0) AS linkedin_id1,
        coalesce(groupArray(lk.id)[2], 0) AS linkedin_id2,
        coalesce(groupArray(lk.id)[3], 0) AS linkedin_id3,
        coalesce(groupArray(lk.score)[1], 0) AS linkedin_score1,
        coalesce(groupArray(lk.score)[2], 0) AS linkedin_score2,
        coalesce(groupArray(lk.score)[3], 0) AS linkedin_score3,
        coalesce(groupArray(lk.person_name)[1], '') AS linkedin_person_name1,
        coalesce(groupArray(lk.person_name)[2], '') AS linkedin_person_name2,
        coalesce(groupArray(lk.person_name)[3], '') AS linkedin_person_name3,
        coalesce(groupArray(lk.occupation)[1], '') AS linkedin_occupation1,
        coalesce(groupArray(lk.occupation)[2], '') AS linkedin_occupation2,
        coalesce(groupArray(lk.occupation)[3], '') AS linkedin_occupation3,
        coalesce(groupArray(lk.company_name)[1], '') AS linkedin_company_name1,
        coalesce(groupArray(lk.company_name)[2], '') AS linkedin_company_name2,
        coalesce(groupArray(lk.company_name)[3], '') AS linkedin_company_name3,
        coalesce(groupArray(lk.url)[1], '') AS linkedin_url1,
        coalesce(groupArray(lk.url)[2], '') AS linkedin_url2,
        coalesce(groupArray(lk.url)[3], '') AS linkedin_url3,

        -- Link (pessoas_linkedin como pl)
        any(pl.id) AS link_id,
        any(pl.date_created) AS link_date_created,
        any(pl.score) AS link_score,
        any(pl.found_in_title) AS link_found_in_title,
        any(pl.person_name) AS link_person_name,
        any(pl.occupation) AS link_occupation,
        any(pl.company_found) AS link_company_found,
        any(pl.url) AS link_url,
        any(pl.company_name) AS link_company_name,
        any(pl.title) AS link_title,
        any(pl.description) AS link_description,
        any(pl.sub_title) AS link_sub_title,
        any(pl.search_key) AS link_search_key,
        any(pl.search_url) AS link_search_url,
        any(pl.cnpj) AS link_cnpj,

        -- LC (linkedin_completo_opt como lc)
        any(lc.url) AS lc_url,
        any(lc.company_name) AS lc_company_name,
        any(lc.cnpj) AS lc_cnpj_linkedin_total,
        any(lc.person_name) AS lc_person_name,
        any(lc.occupation) AS lc_occupation,

        toUInt64(rowNumberInAllBlocks() + 1) AS linha_unica

    FROM {estab} e
    LEFT JOIN {emp} emp ON e.cnpj_basico = emp.cnpj_basico
    LEFT JOIN {soc} soc ON e.cnpj_basico = soc.cnpj_basico
    LEFT JOIN {simp} simp ON e.cnpj_basico = simp.cnpj_basico
    LEFT JOIN {DATABASE}.empresa_faixas_opt pe ON e.cnpj_basico = pe.cnpj_basico
    LEFT JOIN {DATABASE}.linkedin_completo_opt lk ON concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) = lk.cnpj
    LEFT JOIN {DATABASE}.pessoas_linkedin pl ON concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) = pl.cnpj
    LEFT JOIN {DATABASE}.linkedin_completo_opt lc ON concat(e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) = lc.cnpj
    LEFT JOIN {DATABASE}.dict_cnae dc ON e.cnae_fiscal_principal = dc.codigo_cnae
    LEFT JOIN {DATABASE}.dict_municipios dm ON e.municipio = dm.codigo_municipios
    LEFT JOIN {DATABASE}.dict_naturezas dn ON emp.natureza_juridica = dn.codigo_natureza_juridica
    LEFT JOIN {DATABASE}.dict_qualificacoes dq ON emp.qualificacao_responsavel = dq.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs1 ON groupArray(soc.qualificacao_socio)[1] = dqs1.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs2 ON groupArray(soc.qualificacao_socio)[2] = dqs2.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqs3 ON groupArray(soc.qualificacao_socio)[3] = dqs3.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr1 ON groupArray(soc.qualificacao_representante_legal)[1] = dqr1.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr2 ON groupArray(soc.qualificacao_representante_legal)[2] = dqr2.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_qualificacao_socio dqr3 ON groupArray(soc.qualificacao_representante_legal)[3] = dqr3.codigo_qualificacao_socio
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe1 ON groupArray(soc.faixa_etaria)[1] = dfe1.codigo
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe2 ON groupArray(soc.faixa_etaria)[2] = dfe2.codigo
    LEFT JOIN {DATABASE}.dict_faixa_etaria dfe3 ON groupArray(soc.faixa_etaria)[3] = dfe3.codigo
    {where_clause}
    GROUP BY
        e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv,
        e.identificador_matriz_filial, e.nome_fantasia,
        e.situacao_cadastral, e.data_situacao_cadastral,
        e.motivo_situacao_cadastral, e.nome_cidade_exterior, e.pais,
        e.data_inicio_atividade, e.cnae_fiscal_principal, e.cnae_fiscal_secundaria,
        e.tipo_logradouro, e.logradouro, e.numero, e.complemento, e.bairro,
        e.cep, e.uf, e.municipio,
        e.ddd_1, e.telefone_1, e.ddd_2, e.telefone_2, e.ddd_fax, e.fax,
        e.correio_eletronico, e.situacao_especial, e.data_situacao_especial,
        emp.razao_social, emp.natureza_juridica, emp.qualificacao_responsavel,
        emp.capital_social, emp.porte_empresa, emp.ente_federativo_responsavel,
        dc.descricao_cnae, dm.descricao_municipios,
        dn.descricao_natureza_juridica, dq.descricao_qualificacao_socio,
        dqs1.descricao_qualificacao_socio, dqs2.descricao_qualificacao_socio,
        dqs3.descricao_qualificacao_socio,
        dqr1.descricao_qualificacao_socio, dqr2.descricao_qualificacao_socio,
        dqr3.descricao_qualificacao_socio,
        dfe1.descricao, dfe2.descricao, dfe3.descricao
    """


def create_final_table(client, periodo):
    """Cria a tabela final com INSERT por UF para evitar limite de particoes."""

    # Backup da tabela existente
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


def main(periodo):
    """Executa criacao da tabela final."""
    client = get_clickhouse_client()
    if client is None:
        print("[Transform] Sem conexao com ClickHouse. Pulando transformacao.")
        return False

    try:
        create_final_table(client, periodo)
        print("[Transform] Transformacao concluida com sucesso!")
        return True

    except Exception as e:
        print(f"[Transform] ERRO na transformacao: {e}")
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
