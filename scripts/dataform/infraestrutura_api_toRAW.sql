config {
    type: "incremental",
    schema: "RAW_FLATFILE",
    tags: ['infra_full', 'processo_full'],
    name: "api_infraestrutura_raw",
    description: "Tabela criada a partir de LAND_FLATFILE.api_infraestrutura",
    bigquery: {
        partitionBy: "DATETIME_TRUNC(partition_time, MONTH)",
        clusterBy: ['id']
    }
}

with cte_infra as (
    select 
    id,
    secao,
    tema,
    entidade,
    porcentagem,
    escolas,
    sigla_uf,
    total_escolas,
    ano,
    tipo_escolas,
    partition_time
    from `logical-essence-433414-r5.LAND_FLATFILE.api_infraestrutura` 
)

select * from cte_infra