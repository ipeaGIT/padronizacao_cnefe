# codigo_uf <- 15
identificar_setores <- function(codigo_uf) {
  con <- duckspatial::ddbs_create_conn("tempdir")
  on.exit(duckdb::dbDisconnect(con), add = TRUE)
  duckspatial::ddbs_load(con, quiet = TRUE)

  n <- DBI::dbExecute(con, "SET threads TO 1;")

  # lendo cnefe como tabela espacial no duckdb
  #
  # o cnefe pode conter linhas duplicadas. na tabela original, esses registros
  # servem pra indicar quando o mesmo endereco/estabelecimento pode possuir
  # finalidades diferentes (e.g. uma linha se refere ao endereço quando usado
  # como domicilio particular, outra ao endereço quando usado como
  # estabelecimento de saude). como no nosso caso essa diferenca nao importa,
  # mantemos apenas registros unicos.

  cnefe <- suppressWarnings(
    ipeadatalake::ler_cnefe(
      2022,
      colunas = c("code_state", "code_address", "lon", "lat", "code_sector"),
      verboso = FALSE
    )
  ) |>
    dplyr::filter(code_state == codigo_uf)

  cnefe <- suppressWarnings(dplyr::collect(cnefe))
  cnefe <- sf::st_as_sf(cnefe, coords = c("lon", "lat"), crs = 4674)

  duckspatial::ddbs_write_vector(con, cnefe, "cnefe", overwrite = TRUE)

  n <- DBI::dbExecute(
    con,
    "CREATE OR REPLACE TABLE cnefe AS SELECT DISTINCT * FROM cnefe"
  )

  # lendo listagem de setores como tabela espacial no duckdb

  setores <- geobr::read_census_tract(
    codigo_uf,
    year = 2022,
    simplified = FALSE
  )
  setores <- setores[, c("code_tract", "area_km2")]

  duckspatial::ddbs_write_vector(con, setores, "setores", overwrite = TRUE)

  # fazendo o join para identificar o setor em que cada observação do cnefe se
  # encontra

  duckspatial::ddbs_join(
    "cnefe",
    "setores",
    conn = con,
    name = "cnefe_com_setores",
    overwrite = TRUE
  )

  n <- DBI::dbExecute(
    con,
    "ALTER TABLE cnefe ADD COLUMN cod_setor_corr VARCHAR;
    ALTER TABLE cnefe ADD COLUMN area_km2 DOUBLE;
    
    UPDATE cnefe
      SET
        cod_setor_corr = FORMAT('{:.0f}', cnefe_com_setores.code_tract),
        area_km2 = cnefe_com_setores.area_km2
      FROM cnefe_com_setores
      WHERE cnefe.code_address = cnefe_com_setores.code_address;"
  )

  # alguns pontos não encontraram correspondência com nenhum setor no join.
  # provavelmente, isso acontece porque eles estão um pouco pra fora dos setores
  # a que eles deveriam pertencer.
  # nesses casos, partimos do pressuposto que o setor está correto, preenchendo
  # a variável de setor corrigido com o setor já cadastrado e buscando a sua
  # área na tabela de setores

  n <- DBI::dbExecute(
    con,
    "UPDATE cnefe
      SET cod_setor_corr = REGEXP_REPLACE(code_sector, '[A-Z]$', '')
      WHERE cod_setor_corr IS NULL;"
  )

  n <- DBI::dbExecute(
    con,
    "UPDATE cnefe
      SET area_km2 = setores.area_km2
      FROM setores
      WHERE
        cnefe.area_km2 IS NULL AND
        CAST(cnefe.cod_setor_corr AS DOUBLE) = setores.code_tract;"
  )

  # ainda existem casos em que area_km2 é nula, mesmo depois do tratamento.
  # isso acontece quando o setor listado no CNEFE não é encontrado na lista
  # completa de setores de 2022. partimos da hipótese que esses setores podem
  # ser, na verdade, setores de 2010, e buscamos a correspondência entre o
  # setor de 2010 e o que seria o de 2022, correto, em uma listagem oficial
  # compilada pelo IBGE (crosswalk; fonte:
  # https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html)
  #
  # detalhe: por vezes, um único setor de 2010 é dividido em vários diferentes
  # setores em 2022. ou seja, não podemos afirmar qual é o setor de 2022 em que
  # o ponto se encontra. para isso, teríamos que fazer um spatial join, mas se
  # o ponto estivesse dentro de algum setor, ele não estaria com a área NA,
  # pois ela teria sido preenchida no spatial join original. nesses casos,
  # portanto, substituímos o setor pelo equivalente de 2022, caso haja, apenas
  # se for uma relação um pra um. caso contrário, deixamos a área e o código do
  # setor como NA

  n_nas <- DBI::dbGetQuery(
    con,
    "SELECT count() FROM cnefe WHERE area_km2 IS NULL"
  )[[1]]

  if (n_nas > 0) {
    n <- DBI::dbExecute(
      con,
      "CREATE OR REPLACE TABLE ends_sem_setor_valido AS
        SELECT code_state, code_address, code_sector, geometry, crs_duckspatial
        FROM cnefe
        WHERE area_km2 IS NULL;"
    )

    ends_sem_setor_valido <- duckspatial::ddbs_read_vector(
      con,
      "ends_sem_setor_valido"
    )

    dists <- sf::st_distance(ends_sem_setor_valido, setores)
    dists <- data.table::setDT(
      tibble::as_tibble(dists, .name_repair = "unique_quiet")
    )
    dists <- data.table::transpose(dists)
    dists <- dists[, lapply(.SD, function(x) which(x == min(x)))]
    dists <- data.table::transpose(dists)

    dists[,
      setor_de_menor_dist := unlist(
        lapply(
          V1,
          function(x) {
            as.character(setores[x, ]$code_tract)
          }
        )
      )
    ]

    dists[,
      area_km2 := unlist(
        lapply(
          V1,
          function(x) {
            setores[x, ]$area_km2
          }
        )
      )
    ]

    dists[, code_address := ends_sem_setor_valido$code_address]

    duckdb::duckdb_register(con, "setor_de_menor_distancia", dists)

    n <- DBI::dbExecute(
      con,
      "UPDATE cnefe
      SET
        cod_setor_corr = setor_de_menor_distancia.setor_de_menor_dist,
        area_km2 = setor_de_menor_distancia.area_km2
      FROM setor_de_menor_distancia
      WHERE cnefe.code_address = setor_de_menor_distancia.code_address;"
    )
  }

  dir_destino <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    "CNEFE/relacao_endereco_setor/2022"
  )

  if (!dir.exists(dir_destino)) {
    dir.create(dir_destino, recursive = TRUE)
  }

  sigla_uf <- enderecobr::padronizar_estados(codigo_uf, formato = "sigla")

  arq_destino <- file.path(
    dir_destino,
    glue::glue("{codigo_uf}_{sigla_uf}.parquet")
  )

  n <- DBI::dbExecute(
    con,
    glue::glue(
      "COPY
        (SELECT code_state, code_address, cod_setor_corr AS cod_setor, area_km2 FROM cnefe)
        TO '{arq_destino}'
        (FORMAT parquet);"
    )
  )

  return(arq_destino)
}
