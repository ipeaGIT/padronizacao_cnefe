# arq_cnefe <- tar_read(padronizacao, branches = 1)
# arq_setores <- tar_read(identificacao_setor, branches = 1)
# versao_dados <- tar_read(versao_dados)
agregar_cnefe <- function(arq_cnefe, arq_setores, versao_dados) {
  data.table::setDTthreads(1)

  cnefe <- data.table::setDT(arrow::read_parquet(arq_cnefe))
  setores <- data.table::setDT(arrow::read_parquet(arq_setores))

  cnefe[
    setores,
    on = c(code_address = "code_address"),
    `:=`(cod_setor = i.cod_setor, area_km2 = i.area_km2)
  ]

  # mantemos apenas endereços com nv_geo_coord <= 4 OU nv_geo_coord 5 e 6 em
  # setores censitarios com area menor ou igual a 0.1 km2 (equivalente a uma
  # celula h3 res 9).
  # nv_geo_coord 5 representa uma localidade (similar a um bairro), e 6
  # representa um setor censitário (que pode ter dimensões gigantescas,
  # principalmente em áreas rurais, mais propensas a não ter endereços precisos
  # a nível de rua).

  cnefe <- cnefe[nv_geo_coord <= 4 | (nv_geo_coord %in% 5:6 & area_km2 <= 0.1)]

  # temos 12 possíveis casos de agregação. para cada um desses casos,
  # selecionamos as colunas que devem ser usadas e calculamos a média das
  # coordenadas usando essas colunas como grupos

  arqs_destino <- vector("character", length = 12)

  for (caso in 1:12) {
    cli::cli_progress_step(glue::glue("Agregando caso {caso}"))

    colunas_agregacao <- selecionar_colunas(caso)

    cnefe_filtrado <- data.table::copy(cnefe)

    # para os casos de 1 a 4, o número é uma informação relevante. não queremos,
    # portanto, considerar endereços sem número, visto que não é garantido, por
    # exemplo, que dois endereços sem número no mesmo logradouro com mesmo cep e
    # bairro sejam de fato o mesmo endereço. podem ser, por exemplo, dois
    # endereços em extremos opostos da rua, mas igualmente sem número. logo,
    # nesses casos, removemos endereços sem número

    if (caso <= 4) {
      cnefe_filtrado <- cnefe_filtrado[!is.na(numero)]
    }

    # de forma similar, para os casos de 1 a 8 o logradouro é uma informação
    # relevante. o CNEFE usa o nome "SEM DENOMINACAO" para identificar
    # logradouros sem um nome explícito. no entanto, podem existir vários
    # logradouros sem denominação em um mesmo município/bairro, então precisamos
    # removê-los quando o logradouro é uma variável de identificação relevante

    if (caso <= 8) {
      cnefe_filtrado <- cnefe_filtrado[
        !grepl("SEM DENOMINACAO", nome_logradouro, fixed = TRUE)
      ]
      cnefe_filtrado <- cnefe_filtrado[
        !grepl("PROJETAD(A|O)", nome_logradouro, perl = TRUE)
      ]
      cnefe_filtrado <- cnefe_filtrado[
        !grepl("PARTICULAR", nome_logradouro, fixed = TRUE)
      ]
    }

    # agora fazemos a agregacao, de fato, usando as colunas selecionadas
    # anteriormente como grupos

    cnefe_agregado <- cnefe_filtrado[,
      .(
        n_casos = .N,
        lon = coord_media_duas_etapas(lon, lat, "lon", 0.95),
        lat = coord_media_duas_etapas(lon, lat, "lat", 0.95),
        desvio_metros = desvio_duas_etapas(lon, lat, 0.95),
        cod_setor = agregar_setor(cod_setor)
      ),
      by = colunas_agregacao
    ]

    # adiciona 5.8 (arredondando, 6) metros ao desvio calculado, baseado no
    # erro minimo do GPS usado pela equipe do CNEFE
    # fonte: https://biblioteca.ibge.gov.br/visualizacao/livros/liv102063.pdf

    cnefe_agregado[, desvio_metros := desvio_metros + 6]

    # adicionamos coluna com o endereço completo, escrito por extenso. essa
    # informação é importante para que os usuários do geocodebr saibam o
    # endereço encontrado a partir do input

    adicionar_coluna_de_endereco(cnefe_agregado, colunas_agregacao)

    data.table::setcolorder(
      cnefe_agregado,
      c(colunas_agregacao, "endereco_completo", "n_casos", "lon", "lat")
    )

    # removendo indice do datatable e convertendo pra dataframe, para diminuir o
    # tamanho do objeto final e evitar problemas na leitura do parquet

    data.table::setindex(cnefe_agregado, NULL)
    data.table::setDF(cnefe_agregado)

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
      desvio_metros = arrow::float(),
      cod_setor = arrow::utf8()
    )

    schema_arquivo <- schema_cnefe[
      c(
        colunas_agregacao,
        "endereco_completo",
        "n_casos",
        "lon",
        "lat",
        "desvio_metros",
        "cod_setor"
      )
    ]

    cnefe_agregado <- arrow::as_arrow_table(
      cnefe_agregado,
      schema = schema_arquivo
    )

    # arquivos são salvos em esquema particionado por estado, em pastas
    # nomeadas a partir do tipo de agregação. para nomear o tipo, usamos os
    # nomes das colunas usadas na agregação, apenas omitindo a coluna
    # "estado", presente em todas as agregações

    nome_agregacao <- setdiff(colunas_agregacao, "estado")
    nome_agregacao <- paste(nome_agregacao, collapse = "_")

    sigla_uf <- stringr::str_extract(arq_cnefe, "estado=[A-Z]{2}")
    sigla_uf <- sub("estado=", "", sigla_uf)

    dir_estado <- file.path(
      Sys.getenv("PUBLIC_DATA_PATH"),
      "CNEFE/cnefe_padrao_geocodebr/2022",
      versao_dados,
      "dados_agregados_particionados",
      nome_agregacao,
      glue::glue("estado={sigla_uf}")
    )

    if (!dir.exists(dir_estado)) {
      dir.create(dir_estado, recursive = TRUE)
    }

    arq_destino <- file.path(dir_estado, "part-0.parquet")

    arrow::write_parquet(
      cnefe_agregado,
      arq_destino,
      compression = "zstd",
      compression_level = 22
    )

    arqs_destino[caso] <- arq_destino

    cli::cli_progress_done()
  }

  return(arqs_destino)
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

  cnefe_agregado[,
    endereco_completo := paste0(.campo_log, .campo_loc, .campo_est)
  ]

  cnefe_agregado[, c(".campo_log", ".campo_loc", ".campo_est") := NULL]

  invisible(cnefe_agregado[])
}


# coordenada media -------------------------------------------------------------

# df <- cnefe_filtrado[municipio == "PORTO VELHO" & logradouro == "AVENIDA PREFEITO CHIQUILITO ERSE" & numero == 5064 & cep == "76820-370" & localidade == "NOVA ESPERANCA"]
# lon <- df$lon
# lat <- df$lat
# coord_desejada <- "lon"
# percentil <- 0.95
coord_media_duas_etapas <- function(lon, lat, coord_desejada, percentil) {
  pontos <- matrix(c(lon, lat), ncol = 2)

  centroide <- matrix(c(mean(lon), mean(lat)), ncol = 2)

  dists <- raster::pointDistance(centroide, pontos, lonlat = TRUE)

  limite_distancia <- quantile(dists, probs = percentil)

  esta_dentro_do_limite <- dists <= limite_distancia

  if (coord_desejada == "lat") {
    media_percentil <- mean(lat[esta_dentro_do_limite])
  } else {
    media_percentil <- mean(lon[esta_dentro_do_limite])
  }

  return(media_percentil)
}

# distance q cobre x% de todos pontos
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

# df <- cnefe_filtrado[municipio == "PORTO VELHO" & logradouro == "AVENIDA PREFEITO CHIQUILITO ERSE" & numero == 5064 & cep == "76820-370" & localidade == "NOVA ESPERANCA"]
# lon <- df$lon
# lat <- df$lat
# coord_desejada <- "lon"
# percentil <- 0.95
desvio_duas_etapas <- function(lon, lat, percentil) {
  # calcula o raio que cobre todos os pontos no percential 95 de distância para
  # o centroide

  pontos <- matrix(c(lon, lat), ncol = 2)

  centroide <- matrix(c(mean(lon), mean(lat)), ncol = 2)

  dists <- raster::pointDistance(centroide, pontos, lonlat = TRUE)

  limite_distancia <- quantile(dists, probs = percentil)

  esta_dentro_do_limite <- dists <= limite_distancia

  # FIXME: ISSO AQUI ESTÁ CORRETO? FALAR COM O RAFA DEPOIS
  # não era pra pegar os pontos que ambos lat e lon estão dentro do percentil
  # 95? do jeito que está hoje em dia, estão sendo criados pontos que não
  # existem na prática

  lon_p95 <- lon[esta_dentro_do_limite]
  lat_p95 <- lon[esta_dentro_do_limite]

  pontos_p95 <- matrix(c(lon_p95, lat_p95), ncol = 2)
  centroide_p95 <- matrix(c(mean(lon_p95), mean(lat_p95)), ncol = 2)

  dists_p95 <- raster::pointDistance(centroide_p95, pontos_p95, lonlat = TRUE)
  dists_p95 <- round(max(dists_p95), 1)

  return(dists_p95)
}


# coordenada media com aproximacao linear do numero ----------------------------

# encontra coodenada media por aproximacao linear
get_aprox_coord <- function(numero_vec, coord) {
  # numero_vec = cnefe_filtrado$numero
  # coord = cnefe_filtrado$lat

  if (length(unique(numero_vec)) < 3) {
    return(mean(coord))
  }

  # create function
  f_coord <- approxfun(x = numero_vec, y = coord, method = "linear")

  # get median number
  mmin <- min(numero_vec)
  mmax <- max(numero_vec)
  mmedia <- median(mmin:mmax)

  # return coordinate
  return(f_coord(mmedia))
}

# coodenada media em duas etapas com aproximacao linear
mean_coord_2step_numero <- function(
  numero_vec,
  lon_vec,
  lat_vec,
  returned_coord,
  percentile
) {
  # lon_vec <- cnefe_filtrado$lon
  # lat_vec <- cnefe_filtrado$lat
  # numero_vec <- cnefe_filtrado$numero

  # se tem um unico ponto, entao retorna ele sem calcular distancia alguma
  if (length(lon_vec) == 1) {
    if (returned_coord == 'lat') {
      return(lat_vec)
    } else {
      return(lon_vec)
    }
  }

  # se todos pontos sao identicos, retorna o 1o
  if (returned_coord == 'lat' & all(lat_vec == lat_vec[1])) {
    return(lat_vec[1])
  }
  if (returned_coord == 'lon' & all(lon_vec == lon_vec[1])) {
    return(lon_vec[1])
  }

  # matrix de todos os pontos
  points_matrix <- matrix(c(lon_vec, lat_vec), ncol = 2)

  # get centroid
  # points_centroid <- matrix(c(mean(lon_vec), mean(lat_vec)), ncol = 2)
  lon_aprox <- get_aprox_coord(numero_vec, lon_vec)
  lat_aprox <- get_aprox_coord(numero_vec, lat_vec)
  points_centroid <- matrix(c(lon_aprox, lat_aprox), ncol = 2)

  # calcula distancias
  dist_vec <- raster::pointDistance(points_centroid, points_matrix, lonlat = T)

  # identifica pontos que estao dentro da distancia p95
  dist_threshol_p95 <- quantile(dist_vec, probs = percentile, names = FALSE)
  dist_index <- dist_vec <= dist_threshol_p95

  if (returned_coord == 'lat') {
    coord <- lat_vec[dist_index]
  } else {
    coord <- lon_vec[dist_index]
  }

  # aprox linear
  numero_vec <- numero_vec[dist_index]
  coord_aprox <- get_aprox_coord(numero_vec, coord)

  return(coord_aprox)
}

# distance q cobre x% de todos pontos em duas etapas com aproximacao linear
distance_percentile_2step_numero <- function(
  numero_vec,
  lon_vec,
  lat_vec,
  percentile
) {
  # lon_vec <- cnefe_filtrado$lon
  # lat_vec <- cnefe_filtrado$lat
  # numero_vec <- cnefe_filtrado$numero

  # se tem um unico ponto, entao retorna ele sem calcular distancia algua
  if (length(lon_vec) == 1) {
    return(0)
  }

  # se todos pontos sao identicos, retorna o 1o
  if (all(lon_vec == lon_vec[1]) & all(lat_vec == lat_vec[1])) {
    return(0)
  }

  # matrix de todos os pontos
  points_matrix <- matrix(c(lon_vec, lat_vec), ncol = 2)

  # get centroid
  # points_centroid <- matrix(c(mean(lon_vec), mean(lat_vec)), ncol = 2)
  lon_aprox <- get_aprox_coord(numero_vec, lon_vec)
  lat_aprox <- get_aprox_coord(numero_vec, lat_vec)
  points_centroid <- matrix(c(lon_aprox, lat_aprox), ncol = 2)

  # calcula as distancias
  dist_vec <- raster::pointDistance(points_centroid, points_matrix, lonlat = T)

  # identifica pontos que estao dentro da distancia p95
  dist_threshol_p95 <- quantile(dist_vec, probs = percentile, names = FALSE)
  dist_index <- dist_vec <= dist_threshol_p95

  lon_p95 <- lon_vec[dist_index]
  lat_p95 <- lat_vec[dist_index]
  num_vec_p95 <- numero_vec[dist_index]

  # matrix de pontos e centroid APENAS para pontos dentro da p95
  points_matrix_p95 <- matrix(c(lon_p95, lat_p95), ncol = 2)

  # points_centroid_p95 <- matrix(c(mean(lon_p95), mean(lat_p95)), ncol = 2)
  lon_p95 <- get_aprox_coord(num_vec_p95, lon_p95)
  lat_p95 <- get_aprox_coord(num_vec_p95, lat_p95)
  points_centroid_p95 <- matrix(c(lon_p95, lat_p95), ncol = 2)

  # calcula as distancias
  dist_m <- raster::pointDistance(
    points_centroid_p95,
    points_matrix_p95,
    lonlat = T
  )

  # distancia do raio q cobre todos pontos p95
  dist_m <- round(max(dist_m), 1)

  return(dist_m)
}


# seleciona setor censitario ---------------------------------------------------

agregar_setor <- function(cod_setor) {
  vetor_setores <- unique(cod_setor)

  if (length(vetor_setores) == 1) {
    return(vetor_setores)
  }

  return(NA_character_)
}

get_N_census_tracts <- function(code_tract_vec) {
  # code_tract_vec <- cnefe_filtrado$code_tract
  code_tract_vec <- unique(code_tract_vec)
  return(length(code_tract_vec))
}
