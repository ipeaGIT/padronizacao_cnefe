# codigo_uf <- 13
# census_tracts_2022 <- tar_read(census_tracts_2022)
# essa funcao identifica o setor censitario ao qual cada endereco pertence
identificar_setores <- function(codigo_uf, census_tracts_2022) {

  con <- duckspatial::ddbs_create_conn("tempdir")
  on.exit(duckdb::dbDisconnect(con), add = TRUE)
  duckspatial::ddbs_load(con, quiet = TRUE)

  #n <- DBI::dbExecute(con, "SET threads TO 1;")

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
      ano = 2022,
      colunas = c("code_state", "code_address", "lon", "lat", "code_sector"),
      verboso = FALSE
    )
  ) |>
    dplyr::filter(code_state == codigo_uf) |>
    unique() # remove pontos duplicados

  # remove the last letter P from code_tract and convert to numeric
  cnefe <- cnefe |>
    dplyr::mutate(code_sector = stringr::str_remove(code_sector, "P$")) |>
    dplyr::mutate(code_sector = as.numeric(code_sector))


  cnefe <- suppressWarnings(dplyr::collect(cnefe))

  data.table::setindex(cnefe, NULL)
  data.table::setDF(cnefe)

  cnefe <- sf::st_as_sf(cnefe, coords = c("lon", "lat"), crs = 4674)

  # # apgar?
  # duckspatial::ddbs_write_vector(con, cnefe, "cnefe", overwrite = TRUE)

  # open census tracts
  setores <- duckspatial::ddbs_open_dataset(census_tracts_2022) |> 
    dplyr::filter(code_state == codigo_uf)

  # setores <- geobr::read_census_tract(
  #   code_tract= codigo_uf,
  #   year = 2022,
  #   simplified = FALSE,
  #   output = "duckdb"
  # )

  duckspatial::ddbs_write_table(con, setores, "setores", overwrite = TRUE)


  # tentativa 1 - simple left join ----------------------------------------------
  # identifica quais setores do cnefe existem na malha de 2022
  code_tracts_de_22 <- setores |>
    dplyr::pull(code_tract)

  cnefe <- cnefe |>
    dplyr::mutate(is_in_2022 = code_sector %in% code_tracts_de_22)

  1- (sum(cnefe$is_in_2022) /nrow(cnefe))
  # 6.5% no DF (53)
  # 22% no PA (15)
  # 7% no ? (14)
  # 6% no ? (13)

  # vamos separar enderecos que possuem setor de 2022 e os que nao tem
  cnefe1_ja_com_setor <- cnefe |> 
    filter(is_in_2022 == TRUE) |>
    sf::st_drop_geometry() |>
    dplyr::select(code_address, code_sector)

  cnefe_ainda_sem_setor <- cnefe |> filter(is_in_2022 == FALSE)

  # cria tabelas vazias das proximas etapas
  cnefe2_com_setor_crosswalk <- data.frame(code_address =NA, code_sector = NA)
  cnefe3_com_setor_join <- data.frame(code_address =NA, code_sector = NA)
  cnefe4_com_setor_dist <- data.frame(code_address =NA, code_sector = NA)


  # tentativa 2 - cross walk com 2010 ----------------------------------------------
  #' muitos setores no cnefe estao na verdade com codigo de 2010
  #' a solucao aqui eh recuperar o codigo de 2022 usandoo cross walk oficial entre
  #' os codigos de 2010 e 2022
  #' compilada pelo IBGE (crosswalk; fonte:
  #' https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html)
  #' versao salva aqui "data_raw/tracts_info.rds"
  #' e aqui https://github.com/ipeaGIT/padronizacao_cnefe/releases/download/v0.5.0/cross_walk_setores_censitarios_2010_2022.parquet

  if (nrow(cnefe_ainda_sem_setor)>0) {

    # recupera info de area do setor (isso vai ser importante no passo de agregacao)
    temp_cross_walk <- readRDS("./data_raw/tracts_info.rds")|>
      dplyr::select(code_tract_2022, code_tract_2010) |>
      unique()

    # detecta casos onde um setor de 2010 virou dois setores de 2022
    # fica apenas com casos de 1 pra 1
    temp_cross_walk <- temp_cross_walk |>
      dplyr::group_by(code_tract_2010) |>
      dplyr::mutate(one_to_n = dplyr::n_distinct(code_tract_2022)) |>
      dplyr::filter(one_to_n == 1) |>
      dplyr::select(-one_to_n)

    # join quando tabela do cnefe tinha um setor com codigo de 2010
    temp_cross <- dplyr::left_join(
      x= cnefe_ainda_sem_setor |> sf::st_drop_geometry() ,
      y = temp_cross_walk,
       by = c('code_sector' = 'code_tract_2010')
    ) |>
      dplyr::filter(!is.na(code_tract_2022)) # dropa casos que nao encontra

    # detecta casos onde um setor de 2010 virou dois setores de 2022 e dropa
    temp_cross <- temp_cross |>
      dplyr::group_by(code_address) |>
      dplyr::mutate(one_to_one = dplyr::n_distinct(code_tract_2022))

    if(isFALSE(all(temp_cross$one_to_one==1))) {
      stop("erro no match de se setores de 2010 pra 2022 one-too-ne")
      }

    cnefe2_com_setor_crosswalk <- temp_cross |>
      dplyr::select(-code_sector) |> # dropa setor original do cnefe para ficarmos com o encontrado
      dplyr::select(code_address, code_sector = code_tract_2022)

    # atualiza cnefe_ainda_sem_setor
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe2_com_setor_crosswalk$code_address)

  }


  # tentativa 3 - join espacial ----------------------------------------------
  # para os casos do cnefe cujo setor nao foi encontrado, vamos fazer join
  # espacial para identificar o setor em que cada observação do cnefe se encontra

  if (nrow(cnefe_ainda_sem_setor)>0) {

    temp_join <- duckspatial::ddbs_join(
      x = cnefe_ainda_sem_setor,
      y = "setores",
      conn = con
      ) |>
      duckspatial::ddbs_collect()

    if (nrow(temp_join)>0) {

      cnefe3_com_setor_join <- temp_join |>
        dplyr::filter(!is.na(code_tract)) |>
        sf::st_drop_geometry() |>
        dplyr::select(code_address, code_sector)
        }

    # atualiza cnefe_ainda_sem_setor
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe3_com_setor_join$code_address)
  }


  setores <- setores |>
    duckspatial::ddbs_collect()

  # tentativa 4 - distancia espacial ----------------------------------------------

  #' ainda podem existir enderecos sem correspondência com nenhum setor de 2002,
  #' porque estes pontos estão um pouco pra fora dos setores de 22
  #' dai pega o setor mais proximo

  if (nrow(cnefe_ainda_sem_setor) > 0) {

    # planar geometry is faster
    sf::sf_use_s2(FALSE)

    point_bffr <- cnefe_ainda_sem_setor |> 
      sf::st_transform(crs = 32723) |> 
      sf::st_buffer(dist = 3000) |> 
      sf::st_transform(crs = 4674)

    # 666666
    setores_closer_by <- sf::st_intersects(setores, point_bffr)

    dists <- sf::st_distance(
      x = cnefe_ainda_sem_setor, 
      y = setores, 
      tolerance = units::as_units(x = 3, "km")
      )
    
    dists <- data.table::setDT(
      tibble::as_tibble(dists, .name_repair = "unique_quiet")
    )
    dists <- data.table::transpose(dists)
    dists <- dists[, lapply(.SD, function(x) which(x == min(x)))]
    dists <- data.table::transpose(dists)


    dists[,
          setor_de_menor_dist := setores[V1, ]$code_tract,
          by = .I
    ]

    # dists[,
    #   setor_de_menor_dist := unlist(
    #     lapply(
    #       V1,
    #       function(x) {
    #         as.character(setores[x, ]$code_tract)
    #       }
    #     )
    #   )
    # ]
  dists[, code_address := cnefe_ainda_sem_setor$code_address]

  cnefe4_com_setor_dist <- dists |>
    dplyr::select(code_address,
                  code_sector = setor_de_menor_dist)

  }


  # junta todas tentativas
  cnefe_setores_corrigidos <- dplyr::bind_rows(
    cnefe1_ja_com_setor,
    cnefe2_com_setor_crosswalk,
    cnefe3_com_setor_join,
    cnefe4_com_setor_dist
    ) |>
    na.omit()

  # isso deveria ser igual
  # checar camila
  if (isFALSE(nrow(cnefe_setores_corrigidos) == nrow(cnefe))) {
    stop("erro na correspondencia de setores")
    }


  df_final <- cnefe_setores_corrigidos |>
    dplyr::mutate(code_state = substring(code_sector, 1,2)) |>
    dplyr::mutate(code_state = as.numeric(code_state)) |>
    dplyr::arrange(code_address)


  dir_destino <- file.path("./data/relacao_endereco_setor/2022")

  if (!dir.exists(dir_destino)) {
    dir.create(dir_destino, recursive = TRUE)
  }

  sigla_uf <- enderecobr::padronizar_estados(codigo_uf, formato = "sigla")

  arq_destino <- file.path(
    dir_destino,
    glue::glue("{codigo_uf}_{sigla_uf}.parquet")
  )


  arrow::write_parquet(df_final, sink = arq_destino)

  return(arq_destino)
}
