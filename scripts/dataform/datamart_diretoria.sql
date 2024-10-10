config {
    type: "table",
    schema: "REFINED_FLATFILE",
    tags: ['refined', 'processo_full'],
    name: "datamart_diretoria",
    description: "Datamart criado a partir das tabelas da TRUSTED",
    dependencies: [
        'api_escolas_trusted', 'api_estatisticas_trusted', 'api_infraestrutura_trusted'
    ]
}

WITH
  MEDIAS AS (
  SELECT
    es.nome AS ESCOLA,
    es.nome_municipio AS MUNICIPIO,
    es.sigla_uf AS SIGLA_UF,
    es.tipo_localizacao_txt AS TIPO_LOCALIZACAO,
    es.dependencia_administrativa_txt AS DEPENDENCIA_ADMINSTRATIVA,
    es.ano_censo AS ANO_CENSO,
    es.enem_portugues AS MEDIA_ENEM_PORTUGUES,
    es.enem_matematica AS MEDIA_ENEM_MATEMATICA,
    es.enem_humanas AS MEDIA_ENEM_HUMANAS,
    es.enem_naturais AS MEDIA_ENEM_NATURAIS,
    es.enem_redacao AS MEDIA_ENEM_REDACAO,
    es.enem_media_geral MEDIA_ENEM_GERAL,
    es.ideb_af as IDEB_NOTA,
    stat.enem_portugues AS ESTADO_MEDIA_ENEM_PORTUGUES,
    stat.enem_matematica AS ESTADO_MEDIA_ENEM_MATEMATICA,
    stat.enem_humanas AS ESTADO_MEDIA_ENEM_HUMANAS,
    stat.enem_naturais AS ESTADO_MEDIA_ENEM_NATURAIS,
    stat.enem_redacao AS ESTADO_MEDIA_ENEM_REDACAO,
    stat.enem_media_geral AS ESTADO_MEDIA_ENEM_GERAL,
    stat.ideb_af AS ESTADO_IDEB_NOTA,
    es.socio_economico AS SOCIO_ECONOMICO,
    CASE
      WHEN socio_economico = 'Muito Alto' THEN 5
      WHEN socio_economico = 'Alto' THEN 4
      WHEN socio_economico = 'Médio Alto' THEN 3
      WHEN socio_economico = 'Médio' THEN 2
      WHEN socio_economico = 'Médio Baixo' THEN 1
      WHEN socio_economico = 'Baixo' THEN 0
  END
    AS SOCIO_ECONOMICO_NUMERO,
  FROM
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted` es
  JOIN
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_estatisticas_trusted` stat
  ON
    es.sigla_uf = stat.nome_local ),
  PORCENTAGEM_MEDIAS AS (
  WITH
    ESCOLAS AS (
    SELECT
      CASE
        WHEN dependencia_administrativa IN (1, 2, 3) THEN 'Pública'
        WHEN dependencia_administrativa = 4 THEN 'Privada'
    END
      AS TIPO_ESCOLA,
      sigla_uf AS SIGLA_UF,
      COUNTIF(enem_media_geral BETWEEN 400
        AND 500) AS ENTRE_400_500,
      COUNTIF(enem_media_geral BETWEEN 500
        AND 600) AS ENTRE_500_600,
      COUNTIF(enem_media_geral BETWEEN 600
        AND 700) AS ENTRE_600_700,
      COUNTIF(enem_media_geral > 700) AS MAIOR_700
    FROM
      `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted`
    GROUP BY
      TIPO_ESCOLA,
      SIGLA_UF ),
    TOTAL_ESCOLAS AS (
    SELECT
      COUNTIF(enem_media_geral BETWEEN 400
        AND 500) AS escolas_entre_400_500,
      COUNTIF(enem_media_geral BETWEEN 500
        AND 600) AS escolas_entre_500_600,
      COUNTIF(enem_media_geral BETWEEN 600
        AND 700) AS escolas_entre_600_700,
      COUNTIF(enem_media_geral > 700) AS MAIOR_700
    FROM
      `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted` )
  SELECT
    es.TIPO_ESCOLA,
    es.SIGLA_UF,
    es.ENTRE_400_500,
    SAFE_DIVIDE(es.ENTRE_400_500, tot.escolas_entre_400_500) * 100 AS RATIO_400_500,
    es.ENTRE_500_600,
    SAFE_DIVIDE(es.ENTRE_500_600, tot.escolas_entre_500_600) * 100 AS RATIO_500_600,
    es.ENTRE_600_700,
    SAFE_DIVIDE(es.ENTRE_600_700, tot.escolas_entre_600_700) * 100 AS RATIO_600_700,
    es.MAIOR_700,
    SAFE_DIVIDE(es.MAIOR_700, tot.MAIOR_700) * 100 AS RATIO_MAIOR_700
  FROM
    ESCOLAS es,
    TOTAL_ESCOLAS tot ),
  INDICADOR_SOCIO AS (
  SELECT
    CASE
      WHEN socio_economico = 'Muito Alto' THEN 5
      WHEN socio_economico = 'Alto' THEN 4
      WHEN socio_economico = 'Médio Alto' THEN 3
      WHEN socio_economico = 'Médio' THEN 2
      WHEN socio_economico = 'Médio Baixo' THEN 1
      WHEN socio_economico = 'Baixo' THEN 0
      ELSE NULL
  END
    AS socioeconomico_numerico,
    enem_media_geral
  FROM
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted`
  WHERE
    socio_economico IS NOT NULL
    AND enem_media_geral IS NOT NULL ),
  DADOS AS(
  SELECT
    sigla_uf,
    socio_economico
  FROM
    `logical-essence-433414-r5.TRUSTED_FLATFILE.api_escolas_trusted` ),
  INDICADOR AS (
  SELECT
    CORR(socioeconomico_numerico, enem_media_geral) AS CORRELACAO_SOCIOECONOMICA_ENEM,
    sigla_uf AS SIGLA_UF
  FROM
    INDICADOR_SOCIO,
    DADOS
  GROUP BY
    sigla_uf )
SELECT
  ESCOLA,
  MUNICIPIO,
  m.SIGLA_UF,
  TIPO_LOCALIZACAO,
  DEPENDENCIA_ADMINSTRATIVA,
  ANO_CENSO,
  MEDIA_ENEM_PORTUGUES,
  MEDIA_ENEM_MATEMATICA,
  MEDIA_ENEM_HUMANAS,
  MEDIA_ENEM_NATURAIS,
  MEDIA_ENEM_REDACAO,
  MEDIA_ENEM_GERAL,
  IDEB_NOTA,
  ESTADO_MEDIA_ENEM_PORTUGUES,
  ESTADO_MEDIA_ENEM_MATEMATICA,
  ESTADO_MEDIA_ENEM_HUMANAS,
  ESTADO_MEDIA_ENEM_NATURAIS,
  ESTADO_MEDIA_ENEM_REDACAO,
  ESTADO_MEDIA_ENEM_GERAL,
  ESTADO_IDEB_NOTA,
  SOCIO_ECONOMICO,
  SOCIO_ECONOMICO_NUMERO,
  CORRELACAO_SOCIOECONOMICA_ENEM,
  TIPO_ESCOLA,
  ENTRE_400_500,
  RATIO_400_500,
  ENTRE_500_600,
  RATIO_500_600,
  ENTRE_600_700,
  RATIO_600_700,
  MAIOR_700,
  RATIO_MAIOR_700
FROM
  MEDIAS m
JOIN
  PORCENTAGEM_MEDIAS p
ON
  m.SIGLA_UF = p.SIGLA_UF
JOIN
  INDICADOR i
ON
  m.SIGLA_UF = i.SIGLA_UF