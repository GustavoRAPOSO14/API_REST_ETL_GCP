config {
    type: "table",
    schema: "REFINED_FLATFILE",
    tags: ['refined', 'processo_full'],
    name: "datamart_secretaria",
    description: "Datamart criado a partir das tabelas da TRUSTED",
    dependencies: [
        'api_escolas_trusted', 'api_estatisticas_trusted', 'api_infraestrutura_trusted'
    ]
}

WITH
  DADOS_INFRA AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY nome) AS incremento,
    nome AS ESCOLA,
    nome_municipio AS MUNICIPIO,
    CASE
      WHEN ideb_af = 0.0 THEN 'Não informado'
      ELSE CAST(ideb_af AS STRING)
  END
    AS IDEB_ANOS_FINAIS,
    enem_portugues AS ENEM_PORTUGUES,
    enem_matematica AS ENEM_MATEMATICA,
    enem_humanas AS ENEM_HUMANAS,
    enem_naturais AS ENEM_NATURAIS,
    enem_redacao AS ENEM_REDACAO,
    enem_media_geral AS ENEM_MEDIA_GERAL,
    CASE
      WHEN alimentacao = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS ALIMENTACAO,
    CASE
      WHEN agua_publica = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS AGUA_PUBLICA,
    CASE
      WHEN agua_poco_artesiano = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS AGUA_POCO_ARTESIANO,
    CASE
      WHEN esgoto_publico = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS ESGOTO_PUBLICO,
    CASE
      WHEN esgoto_fossa = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS ESGOTO_FOSSA,
    CASE
      WHEN lixo_coleta_periodica = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS LIXO_COLETA_PERIODICA,
    CASE
      WHEN lixo_queima = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS LIXO_QUEIMA,
    CASE
      WHEN lixo_recicla = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS LIXO_RECICLA,
    CASE
      WHEN energia_publica = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS ENERGIA_PUBLICA,
    CASE
      WHEN energia_inexistente = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS ENERGIA_INEXISTENTE,
    CASE
      WHEN cozinha = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS COZINHA,
    CASE
      WHEN biblioteca = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS BIBLIOTECA,
    CASE
      WHEN sanitario_dentro_predio = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS SANITARIO_DENTRO_PREDIO,
    CASE
      WHEN internet = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS INTERNET,
    CASE
      WHEN laboratorio_informatica = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS LABORATORIO_INFORMATICA,
    CASE
      WHEN laboratorio_ciencias = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS LABORATORIO_CIENCIAS,
    CASE
      WHEN sala_professores = FALSE THEN 'Não'ELSE 'Sim'
  END
    AS SALA_PROFESSORES,
    impressoras AS IMPRESSORAS,
    retroprojetores AS RETROPROJETORES,
    televisores AS TELEVISORES,
    computadores_alunos AS COMPUTADORES_ALUNOS,
    salas_existentes AS SALAS_EXISTENTES,
    funcionarios AS FUNCIONARIOS,
    CASE WHEN quadra_coberta = FALSE THEN 'Não' ELSE 'Sim'
    END 
    AS QUADRA_COBERTA,
    CASE WHEN quadra_descoberta = FALSE THEN 'Não' ELSE 'Sim'
    END 
    AS QUADRA_DESCOBERTA,
    CASE WHEN sala_leitura = FALSE THEN 'Não' ELSE 'Sim'
    END AS SALA_LEITURA,
    CASE WHEN refeitorio = FALSE THEN 'Não' ELSE 'Sim'
    END AS REFEITORIO,
    CASE WHEN auditorio = FALSE THEN 'Não' ELSE 'Sim'
    END AS AUDITORIO,
    CASE WHEN patio_coberto = FALSE THEN 'Não' ELSE 'Sim'
    END AS PATIO_COBERTO,
    CASE WHEN sala_diretoria = FALSE THEN 'Não' ELSE 'Sim'
    END AS SALA_DIRETORIA,
    dependencia_administrativa_txt AS DEPENDENCIA_ADMINSTRATIVA,
    CASE WHEN atendimento_especial = FALSE THEN 'Não' ELSE 'Sim'
    END AS ATENDIMENTO_ESPECIAL,
  FROM
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted` ),
  CRESC_PERCENT AS (
  WITH
    dados AS (
    SELECT
      ano,
      secao,
      tema,
      entidade,
      porcentagem,
      sigla_uf
    FROM
      `logical-essence-433414-r5.TRUSTED_FLATFILE.api_infraestrutura_trusted`
    WHERE
       ano IN (2012, 2013, 2019, 2023) AND tipo_escolas = 'Pública')
  SELECT
    1 AS valor_fixo,
    ROW_NUMBER() OVER (ORDER BY tema) AS incremento,
    TEMA,
    MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2012 THEN porcentagem END) AS PORCENTAGEM_SP_2012,
    MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2013 THEN porcentagem END) AS PORCENTAGEM_SP_2013,
    MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2019 THEN porcentagem END) AS PORCENTAGEM_SP_2019,
    MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2023 THEN porcentagem END) AS PORCENTAGEM_SP_2023,
    SAFE_DIVIDE((MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2023 THEN porcentagem END) - 
                     MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2019 THEN porcentagem END)),
                   MAX(CASE WHEN sigla_uf = 'SP' AND ano = 2019 THEN porcentagem END)) * 100 AS CRESCIMENTO_PERCENTUAL_SP_2019_2023,
    MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2012 THEN porcentagem END) AS PORCENTAGEM_BR_2012,
    MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2013 THEN porcentagem END) AS PORCENTAGEM_BR_2013,
    MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2019 THEN porcentagem END) AS PORCENTAGEM_BR_2019,
    MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2023 THEN porcentagem END) AS PORCENTAGEM_BR_2023,
    SAFE_DIVIDE((MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2023 THEN porcentagem END) - 
                     MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2019 THEN porcentagem END)),
                   MAX(CASE WHEN sigla_uf = 'BR' AND ano = 2019 THEN porcentagem END)) * 100 AS CRESCIMENTO_PERCENTUAL_BR_2019_2023,
  FROM
    dados
  GROUP BY
    tema ),
  MEDIA_ESTADOS AS (
  SELECT
    1 AS valor_fixo,
    nome_local AS ESTADO,
    enem_media_geral AS ESTADO_ENEM_MEDIA_GERAL,
    ideb_af AS ESTADO_IDEB_MEDIA_GERAL,
    esgoto_fossa AS ESTADO_PORCENTAGEM_ESGOTO_FOSSA,
    energia_publica AS ESTADO_PORCENTAGEM_ENERGIA_PUBLICA,
    agua_cacimba AS ESTADO_PORCENTAGEM_AGUA_CACIMBA,
    agua_inexistente AS ESTADO_PORCENTAGEM_AGUA_INEXISTENTE,
    internet AS ESTADO_PORCENTAGEM_INTERNET,
    sanitario_dentro_predio AS ESTADO_PORCENTAGEM_SANITARIO_DENTRO_PREDIO,
    lixo_outros AS ESTADO_PORCENTAGEM_LIXO_OUTROS,
    laboratorio_informatica AS ESTADO_PORCENTAGEM_LABORATORIO_INFORMATICA,
    dependencias_pne AS ESTADO_PORCENTAGEM_DEPENDENCIA_COM_ACESSIBILIDADE,
    lixo_recicla AS ESTADO_PORCENTAGEM_LIXO_RECICLA,
    sanitario_pne AS ESTADO_PORCENTAGEM_SANITARIO_ACESSIBILIDADE,
    energia_outros AS ESTADO_PORCENTAGEM_ENERGIA_OUTROS,
    esgoto_publico AS ESTADO_PORCENTAGEM_ESGOTO_PUBLICO,
    cozinha AS ESTADO_PORCENTAGEM_COZINHA,
    lixo_enterra AS ESTADO_PORCENTAGEM_LIXO_ENTERRA,
    energia_gerador AS ESTADO_PORCENTAGEM_ENERGIA_GERADOR,
    lixo_queima AS ESTADO_PORCENTAGEM_LIXO_QUEIMA,
    sala_diretoria AS ESTADO_PORCENTAGEM_SALA_DIRETORIA,
    agua_rio AS ESTADO_PORCENTAGEM_AGUA_RIO,
    energia_inexistente AS ESTADO_PORCENTAGEM_ENERGIA_INEXISTENTE,
    banda_larga AS ESTADO_PORCENTAGEM_BANDA_LARGA,
    agua_publica AS ESTADO_PORCENTAGEM_AGUA_PUBLICA,
    esgoto_inexistente AS ESTADO_PORCENTAGEM_ESGOTO_INEXISTENTE,
    lixo_coleta_periodica AS ESTADO_PORCENTAGEM_LIXO_COLETA_PERIODICA,
    agua_filtrada AS ESTADO_PORCENTAGEM_AGUA_FILTRADA,
    laboratorio_ciencias AS ESTADO_PORCENTAGEM_LAB_CIENCIAS,
    atendimento_especial AS ESTADO_PORCENTAGEM_ATENDIMENTO_ESPECIAL,
    agua_poco_artesiano AS ESTADO_PORCENTAGEM_AGUA_POCO_ARTESIANO,
    sala_professores AS ESTADO_PORCENTAGEM_SALA_PROFESSORES,
    biblioteca AS ESTADO_PORCENTAGEM_BIBLIOTECA,
    lixo_joga_outra_area AS ESTADO_PORCENTAGEM_LIXO_JOGA_OUTRA_AREA,
    sala_leitura AS ESTADO_PORCENTAGEM_SALA_LEITURA,
    agua_publica AS ESTADO_AGUA_PUBLICA,
  FROM
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_estatisticas_trusted` )
SELECT
  ESCOLA,
  MUNICIPIO,
  IDEB_ANOS_FINAIS,
  ENEM_PORTUGUES,
  ENEM_MATEMATICA,
  ENEM_HUMANAS,
  ENEM_REDACAO,
  ENEM_MEDIA_GERAL,
  ALIMENTACAO,
  AGUA_PUBLICA,
  AGUA_POCO_ARTESIANO,
  ESGOTO_PUBLICO,
  ESGOTO_FOSSA,
  LIXO_COLETA_PERIODICA,
  LIXO_QUEIMA,
  LIXO_RECICLA,
  ENERGIA_PUBLICA,
  ENERGIA_INEXISTENTE,
  COZINHA,
  BIBLIOTECA,
  SANITARIO_DENTRO_PREDIO,
  INTERNET,
  LABORATORIO_INFORMATICA,
  LABORATORIO_CIENCIAS,
  SALA_PROFESSORES,
  IMPRESSORAS,
  RETROPROJETORES,
  TELEVISORES,
  COMPUTADORES_ALUNOS,
  SALAS_EXISTENTES,
  FUNCIONARIOS,
  QUADRA_COBERTA,
  QUADRA_DESCOBERTA,
  SALA_LEITURA,
  REFEITORIO,
  AUDITORIO,
  PATIO_COBERTO,
  SALA_DIRETORIA,
  DEPENDENCIA_ADMINSTRATIVA,
  ATENDIMENTO_ESPECIAL,
  TEMA,
  PORCENTAGEM_SP_2012,
  PORCENTAGEM_SP_2013,
  PORCENTAGEM_SP_2019,
  PORCENTAGEM_SP_2023,
  CRESCIMENTO_PERCENTUAL_SP_2019_2023,
  PORCENTAGEM_BR_2012,
  PORCENTAGEM_BR_2013,
  PORCENTAGEM_BR_2019,
  PORCENTAGEM_BR_2023,
  CRESCIMENTO_PERCENTUAL_BR_2019_2023,
  ESTADO,
  CASE
    WHEN ESTADO = 'SP' THEN 'Sao Paulo'
    WHEN ESTADO = 'RJ' THEN 'Rio de Janeiro'
    WHEN ESTADO = 'DF' THEN 'Distrito Federal'
    WHEN ESTADO = 'ES' THEN 'Espirito Santo'
    WHEN ESTADO = 'BA' THEN 'Bahia'
    WHEN ESTADO = 'RN' THEN 'Rio Grande do Norte'
    WHEN ESTADO = 'RS' THEN 'Rio Grande do Sul'
    WHEN ESTADO = 'PE' THEN 'Pernambuco'
    WHEN ESTADO = 'SC' THEN 'Santa Catarina'
    WHEN ESTADO = 'RR' THEN 'Roraima'
    WHEN ESTADO = 'PR' THEN 'Parana'
    WHEN ESTADO = 'PB' THEN 'Paraiba'
    WHEN ESTADO = 'PI' THEN 'Piaui'
    WHEN ESTADO = 'PA' THEN 'Para'
    WHEN ESTADO = 'AP' THEN 'Amapa'
    WHEN ESTADO = 'GO' THEN 'Goias'
    WHEN ESTADO = 'MA' THEN 'Maranhao'
    WHEN ESTADO = 'SE' THEN 'Sergipe'
    WHEN ESTADO = 'AL' THEN 'Alagoas'
    WHEN ESTADO = 'RO' THEN 'Rondonia'
    WHEN ESTADO = 'MS' THEN 'Mato Grosso do Sul'
    WHEN ESTADO = 'MT' THEN 'Mato Grosso'
    WHEN ESTADO = 'MG' THEN 'Minas Gerais'
    WHEN ESTADO = 'CE' THEN 'Ceara'
    WHEN ESTADO = 'AM' THEN 'Amazonas'
    WHEN ESTADO = 'AC' THEN 'Acre'
    WHEN ESTADO = 'TO' THEN 'Tocantins'
  END AS ESTADO_DESCRITO,
  ESTADO_ENEM_MEDIA_GERAL,
  ESTADO_IDEB_MEDIA_GERAL,
  ESTADO_PORCENTAGEM_ESGOTO_FOSSA,
  ESTADO_PORCENTAGEM_ENERGIA_PUBLICA,
  ESTADO_PORCENTAGEM_AGUA_CACIMBA,
  ESTADO_PORCENTAGEM_AGUA_INEXISTENTE,
  ESTADO_PORCENTAGEM_INTERNET,
  ESTADO_PORCENTAGEM_SANITARIO_DENTRO_PREDIO,
  ESTADO_PORCENTAGEM_LIXO_OUTROS,
  ESTADO_PORCENTAGEM_LABORATORIO_INFORMATICA,
  ESTADO_PORCENTAGEM_DEPENDENCIA_COM_ACESSIBILIDADE,
  ESTADO_PORCENTAGEM_LIXO_RECICLA,
  ESTADO_PORCENTAGEM_SANITARIO_ACESSIBILIDADE,
  ESTADO_PORCENTAGEM_ENERGIA_OUTROS,
  ESTADO_PORCENTAGEM_ESGOTO_PUBLICO,
  ESTADO_PORCENTAGEM_COZINHA,
  ESTADO_PORCENTAGEM_LIXO_ENTERRA,
  ESTADO_PORCENTAGEM_ENERGIA_GERADOR,
  ESTADO_PORCENTAGEM_LIXO_QUEIMA,
  ESTADO_PORCENTAGEM_SALA_DIRETORIA,
  ESTADO_PORCENTAGEM_AGUA_RIO,
  ESTADO_PORCENTAGEM_ENERGIA_INEXISTENTE,
  ESTADO_PORCENTAGEM_LIXO_COLETA_PERIODICA,
  ESTADO_PORCENTAGEM_AGUA_FILTRADA,
  ESTADO_PORCENTAGEM_LAB_CIENCIAS,
  ESTADO_PORCENTAGEM_ATENDIMENTO_ESPECIAL,
  ESTADO_PORCENTAGEM_AGUA_POCO_ARTESIANO,
  ESTADO_PORCENTAGEM_SALA_PROFESSORES,
  ESTADO_PORCENTAGEM_BIBLIOTECA,
  ESTADO_PORCENTAGEM_LIXO_JOGA_OUTRA_AREA,
  ESTADO_PORCENTAGEM_SALA_LEITURA,
  ESTADO_AGUA_PUBLICA,
FROM
  DADOS_INFRA
LEFT JOIN
  CRESC_PERCENT
ON
  DADOS_INFRA.incremento = CRESC_PERCENT.valor_fixo
LEFT JOIN
  MEDIA_ESTADOS
ON
  CRESC_PERCENT.incremento = MEDIA_ESTADOS.valor_fixo