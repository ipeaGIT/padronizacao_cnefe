# arqs_agregados <- tar_read(agregacao)
# versao_dados <- tar_read(versao_dados)
unir_cnefe_agregado <- function(arqs_agregados, versao_dados) {
  tmpdb <- tempfile(fileext = ".duckdb")
  con <- duckdb::dbConnect(duckdb::duckdb(), dbdir = tmpdb)
  on.exit(duckdb::dbDisconnect(con), add = TRUE)

  n <- DBI::dbExecute(
    con,
    glue::glue("SET threads TO {getOption('targets.max_n_threads')};")
  )

  tipos_agr <- unique(basename(dirname(dirname(arqs_agregados))))

  arqs_tipo <- lapply(
    tipos_agr,
    function(tipo) {
      cli::cli_progress_step(glue::glue("Agregando caso {tipo}"))

      arqs <- grepv(glue::glue("\\/{tipo}\\/"), arqs_agregados)

      lista_arqs <- paste(glue::glue("'{arqs}'"), collapse = ", ")

      n <- DBI::dbExecute(
        con,
        glue::glue(
          "CREATE OR REPLACE TABLE agr AS
            SELECT * FROM READ_PARQUET([{lista_arqs}]);"
        )
      )

      dir_agr <- file.path(
        Sys.getenv("PUBLIC_DATA_PATH"),
        "CNEFE/cnefe_padrao_geocodebr/2022",
        versao_dados,
        "dados_agregados"
      )

      if (!dir.exists(dir_agr)) {
        dir.create(dir_agr, recursive = TRUE)
      }

      arq_destino <- file.path(dir_agr, glue::glue("{tipo}.parquet"))

      n <- DBI::dbExecute(
        con,
        glue::glue(
          "COPY
            (SELECT * FROM agr)
            TO '{arq_destino}'
            (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 22);"
        )
      )

      cli::cli_progress_done()

      return(arq_destino)
    }
  )

  arqs_tipo <- unlist(arqs_tipo)

  return(arqs_tipo)
}
