library(dplyr)
library(arrow)
library(DBI)
library(duckdb)
library(rlang)

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

cnefe[
  ,
  estado := enderecobr::padronizar_estados(code_state, formato = "sigla")
]
cnefe[, code_state := NULL]

cnefe[, municipio := enderecobr::padronizar_municipios(code_muni)]
cnefe[, code_muni := NULL]

cnefe[, cep := enderecobr::padronizar_ceps(cep)]

cnefe[, numero := enderecobr::padronizar_numeros(num_adress)]
cnefe[, num_adress := NULL]
# a <- table(cnefe$numero)
# a <- as.data.frame(a)


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

############### recomeca daqui ----------------------------------------------------------
# filter nv_geo_coord
cnefe <- cnefe[ nv_geo_coord <= 4, ]

###### 66666666666666 (converte 'S/N' para NA)
cnefe[, numero := as.numeric(numero)]

# select columns
cnefe <- cnefe[, .(estado, municipio, logradouro_sem_numero, numero, cep, localidade, lat, lon, tipo_logradouro)]




get_relevant_cols_rafa <- function(case) {
  relevant_cols <- if (case == 1) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep", "localidade")
  } else if (case == 2) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "cep")
  } else if (case == 3) {
    c("estado", "municipio", "logradouro_sem_numero", "numero", "localidade")
  } else if (case == 4) {
    c("estado", "municipio", "logradouro_sem_numero", "numero")
  } else if (case == 44) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (case == 5) {
    c("estado", "municipio", "logradouro_sem_numero", "cep", "localidade")
  } else if (case == 6) {
    c("estado", "municipio", "logradouro_sem_numero", "cep")
  } else if (case == 7) {
    c("estado", "municipio", "logradouro_sem_numero", "localidade")
  } else if (case == 8) {
    c("estado", "municipio", "logradouro_sem_numero")
  } else if (case == 9) {
    c("estado", "municipio", "cep", "localidade")
  } else if (case == 10) {
    c("estado", "municipio", "cep")
  } else if (case == 11) {
    c("estado", "municipio", "localidade")
  } else if (case == 12) {
    c("estado", "municipio")
  }

  return(relevant_cols)
}





# arrow
for( i in 1:12){ # i = 1

  message(i)

  key_cols <- get_relevant_cols_rafa(i)


  if (i %in% 1:4) {
    temp_aggreg <- cnefe[ ! is.na(numero),
                          .(lat= mean(lat), lon = mean(lon)),
                          by = c(key_cols)
                          ]
    }



  if (i %in% 5:12) {
    temp_aggreg <- cnefe[, .(lat= mean(lat), lon = mean(lon)),
                         by = c(key_cols)
                         ]
  }

  # sort rows
  temp_aggreg <- temp_aggreg[order(estado, municipio, logradouro_sem_numero, numero, cep, localidade)]

  # save parque files
  table_name <- paste(key_cols, collapse = "_")
  table_name <- gsub('estado_municipio_logradouro_sem_numero', 'logradouro', table_name)

  schema_cnefe <- arrow::schema(
    estado = arrow::string(),
    municipio = arrow::string(),
    logradouro_sem_numero = arrow::large_utf8(),
    numero = arrow::int32(),
    cep = arrow::string(),
    localidade = arrow::string(),
    lat = arrow::float64(),
    lon = arrow::float64()
  )

  arrow::write_parquet(temp_aggreg, paste0(table_name, '.parquet'))
  }

