# codigo_uf <- tar_read(codigo_uf)[[1]]
identificar_setores <- function(codigo_uf) {
  con <- duckspatial::ddbs_create_conn("tempdir")
  on.exit(duckdb::dbDisconnect(con), add = TRUE)
  duckspatial::ddbs_load(con, quiet = TRUE)

  # lendo cnefe como tabela espacial no duckdb

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
        cod_setor_corr = CAST(cnefe_com_setores.code_tract AS VARCHAR),
        area_km2 = cnefe_com_setores.area_km2
      FROM cnefe_com_setores
      WHERE cnefe.code_address = cnefe_com_setores.code_address;"
  )

  # TODO: MELHORAR A FORMATAÇÃO DO COD_SETOR_CORR ACIMA, ta vindo '110001505000063.0', com esse .0 no fim

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
}
