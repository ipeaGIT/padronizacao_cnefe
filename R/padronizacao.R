padronizar_cnefe <- function() {
  colunas_a_manter <- c(
    "code_address",       # identificador
    "code_state",         # estado
    "code_muni",          # municipio
    "cep",                # cep
    "desc_localidade",    # bairro, povoado, vila, etc
    "nom_tipo_seglogr",   # tipo de logradouro
    "nom_titulo_seglogr", # titulo (e.g. general, papa, santa, etc)
    "nom_seglogr",        # logradouro
    "num_adress",         # numero
    "lon",                # longitude
    "lat",                # latituted
    "nv_geo_coord"        # nivel de geocodificacao
  )

  cnefe <- ipeadatalake::ler_cnefe(2022, colunas = colunas_a_manter)

  # se número == 0, setar NA, que vira S/N depois

  cnefe <- mutate(
    cnefe,
    num_adress = ifelse(num_adress == 0, NA_integer_, num_adress)
  )

  # existem casos em que o titulo do logradouro é repetido no nome do
  # logradouro. isso acontece mesmo quando o título do logradouro tem até 3
  # palavras. só podemos juntar o nome com o titulo nos casos em que essa
  # repetição não ocorre

  cnefe <- mutate(
    cnefe,
    nwords_titulo = stringr::str_count(nom_titulo_seglogr, "\\S+")
  )

  cnefe <- data.table::setDT(collect(cnefe))

  cnefe[nwords_titulo == 1, comeco_logr := stringr::word(nom_seglogr, 1, 1)]
  cnefe[nwords_titulo == 2, comeco_logr := stringr::word(nom_seglogr, 1, 2)]
  cnefe[nwords_titulo == 3, comeco_logr := stringr::word(nom_seglogr, 1, 3)]
  cnefe[nom_titulo_seglogr == comeco_logr, juntar := FALSE]
  cnefe[nwords_titulo == 0, juntar := FALSE]
  cnefe[is.na(juntar), juntar := TRUE]

  cnefe[
    ,
    nome_logradouro := ifelse(
      juntar,
      paste(nom_titulo_seglogr, nom_seglogr),
      nom_seglogr
    )
  ]
  cnefe[, c("nom_titulo_seglogr", "nom_seglogr") := NULL]
  cnefe[, c("nwords_titulo", "comeco_logr", "juntar") := NULL]

  cnefe[, estado := enderecobr::padronizar_estados(code_state)]
  cnefe[, code_state := NULL]

  cnefe[, municipio := enderecobr::padronizar_municipios(code_muni)]
  cnefe[, code_muni := NULL]

  cnefe[, cep := enderecobr::padronizar_ceps(cep)]

  cnefe[, numero := enderecobr::padronizar_numeros(num_adress)]
  cnefe[, num_adress := NULL]

  cnefe[
    ,
    `:=`(
      logradouro_completo = paste(nom_tipo_seglogr, nome_logradouro, numero),
      logradouro_sem_numero = paste(nom_tipo_seglogr, nome_logradouro)
    )
  ]

  data.table::setnames(
    cnefe,
    old = c("code_address", "desc_localidade", "nom_tipo_seglogr"),
    new = c("codigo_endereco", "localidade", "tipo_logradouro")
  )

  data.table::setcolorder(
    cnefe,
    c(
      "codigo_endereco", "estado", "municipio", "localidade", "cep",
      "tipo_logradouro", "nome_logradouro", "numero", "logradouro_sem_numero",
      "logradouro_completo", "lon", "lat", "nv_geo_coord"
    )
  )

  schema_cnefe <- arrow::schema(
    codigo_endereco = arrow::int32(),
    estado = arrow::string(),
    municipio = arrow::string(),
    localidade = arrow::string(),
    cep = arrow::string(),
    tipo_logradouro = arrow::string(),
    nome_logradouro = arrow::string(),
    numero = arrow::string(),
    logradouro_sem_numero = arrow::large_utf8(),
    logradouro_completo = arrow::large_utf8(),
    lon = arrow::float64(),
    lat = arrow::float64(),
    nv_geo_coord = arrow::int8()
  )

  cnefe_arrow <- arrow::as_arrow_table(cnefe, schema = schema_cnefe)

  dir_dados <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    "CNEFE/cnefe_padrao_geocodebr"
  )

  dir_ano <- file.path(dir_dados, "2022")
  if (!dir.exists(dir_ano)) dir.create(dir_ano)

  arrow::write_dataset(
    cnefe_arrow,
    path = dir_ano,
    format = "parquet",
    partitioning = "estado",
    hive_style = TRUE
  )

  return(dir_ano)
}
