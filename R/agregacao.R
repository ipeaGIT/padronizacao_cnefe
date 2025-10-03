# endereco_cnefe <- tar_read(padronizacao)
# versao_dados <- tar_read(versao_dados)
agregar_cnefe <- function(endereco_cnefe, versao_dados) {

  data.table::setDTthreads(percent = 50)
  sf::sf_use_s2(use_s2 = FALSE)

  cnefe <- arrow::open_dataset(endereco_cnefe) |>
    # dplyr::filter(estado == state_i) |>
    #  dplyr::filter(municipio == 'SEROPEDICA') |>
    dplyr::collect()

  data.table::setDT(cnefe)

  # dir_dados <- file.path(
  #   Sys.getenv("PUBLIC_DATA_PATH"),
  #   "CNEFE/cnefe_padrao_geocodebr"
  # )
  dir_dados <- file.path(
    "C:/Users/r1701707/Desktop/cnefe_pdr",
    "CNEFE/cnefe_padrao_geocodebr"
  )

  dir_agreg <- file.path(dir_dados, "2022", versao_dados, "dados_agregados")
  if (!dir.exists(dir_agreg)) dir.create(dir_agreg, recursive = TRUE)

  # temos 12 possíveis casos de agregação. para cada um desses casos,
  # selecionamos as colunas que devem ser usadas e calculamos a média das
  # coordenadas usando essas colunas como grupos

  for (caso in 1:12) {

    cli::cli_progress_step(glue::glue("Agregando caso {caso}"))

    colunas_agregacao <- selecionar_colunas(caso)

    cnefe_filtrado <- data.table::copy(cnefe)

    # para os casos de 1 a 4, o número é uma informação relevante. não queremos,
    # portanto, considerar endereços sem número, visto que não é garantido, por
    # exemplo, que dois endereços sem número no mesmo logradouro com mesmo cep e
    # bairro sejam de fato o mesmo endereço. podem ser, por exemplo, dois
    # endereços em extremos opostos da rua, mas igualmente sem número. logo,
    # nesses casos removemos endereços sem número

    if (caso <= 4) cnefe_filtrado <- cnefe_filtrado[!is.na(numero)]

    # de forma similar, para os casos de 1 a 8 o logradouro é uma informação
    # relevante. o CNEFE usa o nome "SEM DENOMINACAO" para identificar
    # logradouros sem um nome explícito. no entanto, podem existir vários
    # logradouros sem denominação em um mesmo município/bairro, então precisamos
    # removê-los quando o logradouro é uma variável de identificação relevante

    if (caso <= 8) {
      cnefe_filtrado <- cnefe_filtrado[!grepl("SEM DENOMINACAO", nome_logradouro, fixed = TRUE)]
      cnefe_filtrado <- cnefe_filtrado[!grepl("PROJETAD(A|O)", nome_logradouro, perl = TRUE)]
      cnefe_filtrado <- cnefe_filtrado[!grepl("PARTICULAR", nome_logradouro, fixed = TRUE)]
    }

    # agora fazemos a agregacao, de fato, usando as colunas selecionadas
    # anteriormente como grupos

    cnefe_agregado <- cnefe_filtrado[
      ,
      .(n_casos = .N,
        lon = mean_coord_2step(lon, lat, returned_coord='lon', .95),
        lat = mean_coord_2step(lon, lat, returned_coord='lat', .95),
        desvio_metros = distance_percentile_2step(lon,lat, percentile = .95)
        ),
      by = colunas_agregacao
    ]

    # add 5.8 meters based on the best gps precision of cnefe
    # https://biblioteca.ibge.gov.br/visualizacao/livros/liv102063.pdf
    cnefe_agregado[, desvio_metros := desvio_metros + 6]


    # adicionamos coluna com o endereço completo, escrito por extenso. essa
    # informação é importante para que os usuários do geocodebr saibam o
    # endereço encontrado a partir do input

    adicionar_coluna_de_endereco(cnefe_agregado, colunas_agregacao)

    data.table::setcolorder(
      cnefe_agregado,
      c(colunas_agregacao, "endereco_completo", "n_casos", "lon", "lat")
    )

    # remove data table index
    data.table::setindex(cnefe_agregado, NULL)
    data.table::setDF(cnefe_agregado)

    # convertemos de volta para arrow para definirmos o schema e o tipo de cada
    # coluna

    schema_cnefe <- arrow::schema(
      estado = arrow::string(),
      municipio = arrow::string(),
      localidade = arrow::string(),
      cep = arrow::string(),
      numero = arrow::int32(),
      logradouro = arrow::large_utf8(),
      endereco_completo = arrow::large_utf8(),
      n_casos = arrow::int32(),
      lon = arrow::float64(),
      lat = arrow::float64(),
      desvio_metros = arrow::float()
    )

    schema_arquivo <- schema_cnefe[
      c(colunas_agregacao, "endereco_completo", "n_casos", "lon", "lat", "desvio_metros")
    ]

    cnefe_agregado <- arrow::as_arrow_table(
      cnefe_agregado,
      schema = schema_arquivo
    )

    # cada versão agregada é salva com o nome das colunas usadas na agregação.
    # apenas omitimos a coluna "estado", presente em todas as agregações

    nome_arquivo <- setdiff(colunas_agregacao, "estado")
    nome_arquivo <- paste(nome_arquivo, collapse = "_")
    nome_arquivo <- glue::glue("{nome_arquivo}.parquet")

    endereco_arquivo <- file.path(dir_agreg, nome_arquivo)

    # salva parquet compactado
    arrow::write_parquet(
      x = cnefe_agregado,
      sink = endereco_arquivo,
      compression='zstd',
      compression_level = 22
      )

    cli::cli_progress_done()
  }

  return(dir_agreg)
}

selecionar_colunas <- function(caso) {
  if (caso == 1) {
    c("estado", "municipio", "logradouro", "numero", "cep", "localidade")
  } else if (caso == 2) {
    c("estado", "municipio", "logradouro", "numero", "cep")
  } else if (caso == 3) {
    c("estado", "municipio", "logradouro", "numero", "localidade")
  } else if (caso == 4) {
    c("estado", "municipio", "logradouro", "numero")
  } else if (caso == 5) {
    c("estado", "municipio", "logradouro", "cep", "localidade")
  } else if (caso == 6) {
    c("estado", "municipio", "logradouro", "cep")
  } else if (caso == 7) {
    c("estado", "municipio", "logradouro", "localidade")
  } else if (caso == 8) {
    c("estado", "municipio", "logradouro")
  } else if (caso == 9) {
    c("estado", "municipio", "cep", "localidade")
  } else if (caso == 10) {
    c("estado", "municipio", "cep")
  } else if (caso == 11) {
    c("estado", "municipio", "localidade")
  } else if (caso == 12) {
    c("estado", "municipio")
  }
}

adicionar_coluna_de_endereco <- function(cnefe_agregado, colunas_agregacao) {
  # padrão de endereço completo:
  #   Av. Venceslau Brás, 72 - Botafogo, Rio de Janeiro - RJ, 22290-140
  #
  # então podemos pensar em 3 "campos":
  # logradouro com número - bairro com muni - estado com cep
  #
  # lembrando também que todos os nossos casos incluem ao menos estado e
  # município

  if (all(c("logradouro", "numero") %in% colunas_agregacao)) {
    cnefe_agregado[, .campo_log := paste0(logradouro, ", ", numero, " - ")]
  } else if ("logradouro" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_log := paste0(logradouro, " - ")]
  } else {
    cnefe_agregado[, .campo_log := ""]
  }

  if ("localidade" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_loc := paste0(localidade, ", ", municipio, " - ")]
  } else {
    cnefe_agregado[, .campo_loc := paste0(municipio, " - ")]
  }

  if ("cep" %in% colunas_agregacao) {
    cnefe_agregado[, .campo_est := paste0(estado, ", ", cep)]
  } else {
    cnefe_agregado[, .campo_est := estado]
  }

  cnefe_agregado[
    ,
    endereco_completo := paste0(.campo_log, .campo_loc, .campo_est)
  ]

  cnefe_agregado[, c(".campo_log", ".campo_loc", ".campo_est") := NULL]

  invisible(cnefe_agregado[])
}

distance_percentile <- function(lon_vec, lat_vec, percentile) {

  # lon_vec <- dt$lon
  # lat_vec <- dt$lat

  m <- matrix(c(lon_vec, lat_vec), ncol = 2)

  # get centroid
  centroid <- matrix(c(mean(lon_vec), mean(lat_vec)), ncol = 2)

  dist_m <- raster::pointDistance(centroid, m, lonlat = T) |>
    quantile(percentile, names = FALSE) |>
    round(1)

  return(dist_m)
}

distance_percentile_2step <- function(lon_vec, lat_vec, percentile) {

  # lon_vec <- dt2$lon
  # lat_vec <- dt2$lat

  points_matrix <- matrix(c(lon_vec, lat_vec), ncol = 2)

  # get centroid
  points_centroid <- matrix(c(mean(lon_vec), mean(lat_vec)), ncol = 2)

  dist_vec <- raster::pointDistance(points_centroid, points_matrix, lonlat = T)

  dist_threshol_p95 <- quantile(dist_vec, probs = percentile, names = FALSE)
  dist_index <- dist_vec <= dist_threshol_p95

  lon_p95 <- lon_vec[dist_index]
  lat_p95 <- lat_vec[dist_index]

  points_matrix_p95 <- matrix(c(lon_p95, lat_p95), ncol = 2)
  points_centroid_p95 <- matrix(c(mean(lon_p95), mean(lat_p95)), ncol = 2)

  dist_m <- raster::pointDistance(points_centroid_p95, points_matrix_p95, lonlat = T)
  dist_m <- round(max(dist_m), 1)

  # distancia do raio q cobre todos pontos p95
  return(dist_m)
}


mean_coord_2step <- function(lon_vec, lat_vec, returned_coord, percentile) {

  # lon_vec <- dt2$lon
  # lat_vec <- dt2$lat

  points_matrix <- matrix(c(lon_vec, lat_vec), ncol = 2)

  # get centroid
  points_centroid <- matrix(c(mean(lon_vec), mean(lat_vec)), ncol = 2)

  dist_vec <- raster::pointDistance(points_centroid, points_matrix, lonlat = T)

  dist_threshol_p95 <- quantile(dist_vec, probs = percentile, names = FALSE)
  dist_index <- dist_vec < dist_threshol_p95

  if (returned_coord == 'lat') {
    coord <- lat_vec[dist_index]
    } else {
      coord <- lon_vec[dist_index]
      }

  return(mean(coord))

}
