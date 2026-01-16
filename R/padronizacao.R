# codigo_uf <- tar_read(codigo_uf)[[1]]
# versao_dados <- tar_read(versao_dados)
padronizar_cnefe <- function(codigo_uf, versao_dados) {
  colunas_a_manter <- c(
    "code_address", # identificador
    "code_state", # estado
    "code_muni", # municipio
    "cep", # cep
    "desc_localidade", # bairro, povoado, vila, etc
    "nom_tipo_seglogr", # tipo de logradouro
    "nom_titulo_seglogr", # titulo (e.g. general, papa, santa, etc)
    "nom_seglogr", # logradouro
    "num_adress", # numero
    "lon", # longitude
    "lat", # latituted
    "nv_geo_coord" # nivel de geocodificacao
  )

  # suprimindo warning:
  #   Potentially unsafe or invalid elements have been discarded from R metadata.
  #   ℹ Type: "externalptr"
  #   → If you trust the source, you can set `options(arrow.unsafe_metadata = TRUE)` to preserve them.

  cnefe <- suppressWarnings(
    ipeadatalake::ler_cnefe(
      2022,
      colunas = colunas_a_manter,
      verboso = FALSE
    )
  ) |>
    dplyr::filter(code_state == codigo_uf)

  # #' mantemos apenas endereços com nv_geo_coord <= 4, OU nv_geo_coord 5 e 6 em
  # #' setores censitarios com area menor ou igual a 0.1km2 (equivalente ao H3 res 9)
  # #' nv_geo_coord 5 representa uma localidade (similar a um bairro) e 6 representa
  # #' um setor censitário (que
  # #' pode ter dimensões gigantescas, principalmente em áreas rurais, mais propensas
  # #' a não ter endereços precisos a nível de rua)

  # cnefe <- cnefe |>
  #   dplyr::filter(
  #     nv_geo_coord <= 4 |
  #       code_tract %in% tracts_aceitaveis
  #   )

  # se numero == 0, setar NA. mantemos como numerico, pois durante o processo de
  # geolocalizacao podemos usa-los para fazer uma interpolacao, e para isso
  # precisamos que seja numerico.

  cnefe <- dplyr::mutate(
    cnefe,
    num_adress = ifelse(num_adress == 0, NA_integer_, num_adress)
  )

  # existem casos em que o titulo do logradouro é repetido no nome do
  # logradouro. isso acontece mesmo quando o título do logradouro tem até 3
  # palavras. só podemos juntar o nome com o titulo nos casos em que essa
  # repetição não ocorre
  #
  # o cnefe pode conter tambem linhas duplicadas. na tabela original, esses
  # registros servem pra indicar quando o mesmo endereco/estabelecimento pode
  # possuir finalidades diferentes (e.g. uma linha se refere ao endereço quando
  # usado como domicilio particular, outra ao endereço quando usado como
  # estabelecimento de saude). como no nosso caso essa diferenca nao importa,
  # mantemos apenas registros unicos.

  cnefe <- unique(cnefe)
  cnefe <- dplyr::mutate(
    cnefe,
    nwords_titulo = stringr::str_count(nom_titulo_seglogr, "\\S+")
  )

  cnefe <- suppressWarnings(dplyr::collect(cnefe))
  cnefe <- data.table::setDT(cnefe)

  cnefe[nwords_titulo == 1, comeco_logr := stringr::word(nom_seglogr, 1, 1)]
  cnefe[nwords_titulo == 2, comeco_logr := stringr::word(nom_seglogr, 1, 2)]
  cnefe[nwords_titulo == 3, comeco_logr := stringr::word(nom_seglogr, 1, 3)]
  cnefe[nom_titulo_seglogr == comeco_logr, juntar := FALSE]
  cnefe[nwords_titulo == 0, juntar := FALSE]
  cnefe[is.na(juntar), juntar := TRUE]

  cnefe[,
    nome_logradouro := ifelse(
      juntar,
      paste(nom_titulo_seglogr, nom_seglogr),
      nom_seglogr
    )
  ]
  cnefe[, c("nom_titulo_seglogr", "nom_seglogr") := NULL]
  cnefe[, c("nwords_titulo", "comeco_logr", "juntar") := NULL]

  # tambem pode acontecer do nome do logradouro incluir o tipo do logradouro.
  # dessa forma, fazemos um procedimento analogo ao feito acima, pra eliminar
  # eventuais redundâncias.
  #
  # quase todas os registros envolvem tipos de logradouro compostos por apenas
  # uma palavra (106~ milhões de observações), o que faz com que a função
  # stringr::word() use muita memória e crashe o R. então usamos uma função
  # auxiliar para calcular comeco_logr em batches nesse caso.
  #
  # no fim dessa sequência, atualizamos o nome do logradouro para remover o tipo
  # que estava embutido

  cnefe[, nwords_tipo := stringr::str_count(nom_tipo_seglogr, "\\S+")]

  #extrair_comeco_logr_uma_palavra(cnefe)
  cnefe[nwords_tipo == 1, comeco_logr := stringr::word(nome_logradouro, 1, 1)]
  cnefe[nwords_tipo == 2, comeco_logr := stringr::word(nome_logradouro, 1, 2)]
  cnefe[nwords_tipo == 3, comeco_logr := stringr::word(nome_logradouro, 1, 3)]
  cnefe[nwords_tipo == 4, comeco_logr := stringr::word(nome_logradouro, 1, 4)]
  cnefe[is.na(comeco_logr), comeco_logr := nome_logradouro]
  cnefe[nom_tipo_seglogr == comeco_logr, juntar := FALSE]
  cnefe[is.na(juntar), juntar := TRUE]

  cnefe[,
    logradouro := ifelse(
      juntar,
      paste(nom_tipo_seglogr, nome_logradouro),
      nome_logradouro
    )
  ]

  cnefe[
    juntar == FALSE,
    nome_logradouro := stringr::str_replace(
      nome_logradouro,
      pattern = paste0("^", nom_tipo_seglogr, " "),
      replacement = ""
    )
  ]

  cnefe[, c("nwords_tipo", "comeco_logr", "juntar") := NULL]

  cnefe[,
    estado := enderecobr::padronizar_estados(code_state, formato = "sigla")
  ]
  cnefe[, code_state := NULL]

  cnefe[, municipio := enderecobr::padronizar_municipios(code_muni)]
  cnefe[, code_muni := NULL]

  cnefe[, cep := enderecobr::padronizar_ceps(cep)]

  cnefe[, numero := num_adress]
  cnefe[, num_adress := NULL]

  data.table::setnames(
    cnefe,
    old = c("desc_localidade", "nom_tipo_seglogr"),
    new = c("localidade", "tipo_logradouro")
  )

  data.table::setcolorder(
    cnefe,
    c(
      "estado",
      "municipio",
      "localidade",
      "cep",
      "tipo_logradouro",
      "nome_logradouro",
      "numero",
      "logradouro",
      "lon",
      "lat",
      "code_address",
      "nv_geo_coord"
    )
  )

  cnefe <- cnefe[order(municipio, logradouro, numero, cep, localidade)]

  # removendo indice do datatable e convertendo pra dataframe, para diminuir o
  # tamanho do objeto final e evitar problemas na leitura do parquet

  data.table::setindex(cnefe, NULL)
  data.table::setDF(cnefe)

  schema_cnefe <- arrow::schema(
    estado = arrow::string(),
    municipio = arrow::string(),
    localidade = arrow::string(),
    cep = arrow::string(),
    tipo_logradouro = arrow::string(),
    nome_logradouro = arrow::string(),
    numero = arrow::int32(),
    logradouro = arrow::large_utf8(),
    lon = arrow::float64(),
    lat = arrow::float64(),
    code_address = arrow::int32(),
    nv_geo_coord = arrow::int8()
  )

  cnefe_arrow <- arrow::as_arrow_table(cnefe, schema = schema_cnefe)

  dir_dados <- file.path(
    Sys.getenv("PUBLIC_DATA_PATH"),
    "CNEFE/cnefe_padrao_geocodebr"
  )

  dir_ano <- file.path(dir_dados, "2022", versao_dados, "microdados")
  if (!dir.exists(dir_ano)) {
    dir.create(dir_ano, recursive = TRUE)
  }

  sigla_uf <- enderecobr::padronizar_estados(codigo_uf, formato = "sigla")

  dir_estado <- file.path(dir_ano, glue::glue("estado={sigla_uf}"))
  if (!dir.exists(dir_estado)) {
    dir.create(dir_estado)
  }

  arq_destino <- file.path(dir_estado, "part-0.parquet")

  arrow::write_parquet(cnefe_arrow, arq_destino)

  return(arq_destino)
}

extrair_comeco_logr_uma_palavra <- function(cnefe) {
  indices <- which(cnefe$nwords_tipo == 1)
  divisao_grupo <- cut(indices, breaks = 10)

  grupos <- split(indices, divisao_grupo)

  resultado <- lapply(
    grupos,
    function(is) stringr::word(cnefe[is]$nome_logradouro, 1, 1)
  )

  resultado <- unlist(resultado)

  cnefe[nwords_tipo == 1, comeco_logr := resultado]

  return(invisible(cnefe[]))
}
