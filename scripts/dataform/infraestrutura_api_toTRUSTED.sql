config {
    type: "table",
    schema: "TRUSTED_FLATFILE",
    tags: ['infra_full', 'processo_full'],
    name: "api_infraestrutura_trusted",
    description: "Tabela criada a partir de RAW_FLATFILE.api_infraestrutura",
    bigquery: {
        partitionBy: "DATETIME_TRUNC(partition_time, MONTH)",
        clusterBy: ['id'],
    },
    dependencies: [
        'api_infraestrutura_raw'
    ]
}

WITH latest_partition AS (
    SELECT MAX(partition_time) AS max_partition_time
    FROM `logical-essence-433414-r5.RAW_FLATFILE.api_infraestrutura_raw`
)

select 
  CAST(id AS STRING) as id,
  CAST(secao AS STRING) as secao,
  CAST(tema AS STRING) as tema,
  CAST(entidade AS STRING) as entidade,
  CAST(porcentagem AS FLOAT64) as porcentagem,
  SAFE_CAST((CAST(escolas as FLOAT64)) as INT64 ) as escolas,
  CAST(sigla_uf AS STRING) as sigla_uf,
  CAST(total_escolas AS INT64) as total_escolas,
  CAST(ano AS INT64) as ano,
  CAST(tipo_escolas AS STRING) as tipo_escolas,
  partition_time
  from `logical-essence-433414-r5.RAW_FLATFILE.api_infraestrutura_raw`
  WHERE partition_time = (SELECT max_partition_time FROM latest_partition)


