# codigo_uf <- 13
# census_tracts_2022 <- tar_read(census_tracts_2022)
# essa funcao identifica o setor censitario ao qual cada endereco pertence
identificar_setores <- function(codigo_uf, census_tracts_2022) {

  # crosswalk de codigo entre setores de 2010 e 2022
  cross_walk <- readRDS("./data_raw/tracts_info.rds")|>
    dplyr::select(code_tract_2022, code_tract_2010) |>
    dplyr::mutate(code_state = substring(code_tract_2022, 1, 2) |> as.numeric()) |> 
    dplyr::filter(code_state == codigo_uf) |> 
    dplyr::select(-code_state) |> 
    unique()


  # detecta casos onde um setor de 2010 virou dois setores de 2022
  cross_walk <- cross_walk |>
      dplyr::group_by(code_tract_2010) |>
      dplyr::mutate(
        one_to_n = dplyr::n_distinct(code_tract_2022),
        all_tracts_22 = paste(code_tract_2022, collapse=", " )
        ) |> 
    dplyr::ungroup()

  # carrega cnefe
  cnefe <- ipeadatalake::ler_cnefe(
      ano = 2022,
      colunas = c("code_state", "code_address", "lon", "lat", "code_sector"),
      verboso = FALSE
    )

  # filtra cnefe uf e converte codigo do setor para numerico
    # o cnefe pode conter linhas duplicadas. na tabela original, esses registros
    # servem pra indicar quando o mesmo endereco/estabelecimento pode possuir
    # finalidades diferentes (e.g. uma linha se refere ao endereço quando usado
    # como domicilio particular, outra ao endereço quando usado como
    # estabelecimento de saude). como no nosso caso essa diferenca nao importa,
    # mantemos apenas registros unicos.
  cnefe <- cnefe |>
    dplyr::filter(code_state == codigo_uf) |>
    unique() |> 
    dplyr::compute() |> 
    dplyr::mutate(code_sector = stringr::str_remove(code_sector, "P$")) |>
    dplyr::mutate(code_sector = as.numeric(code_sector)) #|> 
   #dplyr::collect() 666666666

  # determina casos possiveis de correspendencia entre 2010 e 2022
  cnefe <- cnefe |> 
    dplyr::mutate(in_22 = code_sector %in% unique(cross_walk$code_tract_2022),
                  in_10 = code_sector %in% unique(cross_walk$code_tract_2010)
                ) |> 
    dplyr::mutate(only10 = in_10==T & in_22==F,
                  only22 = in_10==F & in_22==T,
                  both = in_10==T & in_22==T,
                  none = in_10==F & in_22==F
                ) |> 
    dplyr::compute()

  # temp_total <- nrow(cnefe)
  # cnefe |>
  #   dplyr::summarize(
  #     dplyr::across(
  #       c(none, only10, only22, both),
  #       ~ sum(.x) / temp_total
  #     )
  #   ) |> 
  #   dplyr::collect()
  # 
  # nacional
  #  none only10 only22  both
  #  3.47   4.47   37.1  54.9


  # tentativa 1 - simple left join ----------------------------------------------
  # identifica quais setores do cnefe existem na malha de 2022
  cnefe1_ja_com_setor <- cnefe |> 
    dplyr::filter(in_22 == TRUE) |>
    dplyr::select(code_address, code_sector) |> 
    dplyr::mutate(step = 1) |> 
    dplyr::collect()

  # crina
  cnefe_ainda_sem_setor <- cnefe |>
    dplyr::filter_out(code_address %in% cnefe1_ja_com_setor$code_address)

  # cria tabelas vazias das proximas etapas
  cnefe2_com_setor_crosswalk_one2one <- data.frame(code_address=NA, code_sector = NA, step=2)
  cnefe3_com_setor_crosswalk_one2many <- data.frame(code_address=NA, code_sector = NA, step=3)
  cnefe4_com_setor_join <- data.frame(code_address=NA, code_sector = NA, step=4)
  cnefe5_com_setor_dist <- data.frame(code_address=NA, code_sector = NA, step=5)


  # tentativa 2 - cross walk com 2010 one2one ----------------------------------------------

  #' muitos setores no cnefe estao na verdade com codigo de 2010
  #' a solucao aqui eh recuperar o codigo de 2022 usandoo cross walk oficial entre
  #' os codigos de 2010 e 2022
  #' compilada pelo IBGE (crosswalk; fonte:
  #' https://www.ibge.gov.br/geociencias/organizacao-do-territorio/malhas-territoriais/26565-malhas-de-setores-censitarios-divisoes-intramunicipais.html)
  #' versao salva aqui "data_raw/tracts_info.rds"
  #' e aqui https://github.com/ipeaGIT/padronizacao_cnefe/releases/download/v0.5.0/cross_walk_setores_censitarios_2010_2022.parquet

  if (nrow(cnefe_ainda_sem_setor)>0) {
    
    # detecta casos onde um setor de 2010 virou dois setores de 2022
    # fica apenas com casos de 1 pra 1
    cross_walk_one2one <- cross_walk |>
      dplyr::filter(one_to_n == 1) |>
      dplyr::select(-one_to_n, -all_tracts_22)


    # join quando tabela do cnefe tinha um setor com codigo de 2010
    temp_cross <- dplyr::left_join(
      x= cnefe_ainda_sem_setor,
      y = cross_walk_one2one,
       by = c('code_sector' = 'code_tract_2010')
    ) |>
      dplyr::filter(!is.na(code_tract_2022)) # dropa casos que nao encontra

    # checa se ainda teve algum caso de um setor de 2010 virar dois setores de 2022
    temp_cross <- temp_cross |>
      dplyr::group_by(code_address) |>
      dplyr::mutate(one_to_one = dplyr::n_distinct(code_tract_2022))

    if(isFALSE(all(temp_cross$one_to_one==1))) {
      stop("erro no match de se setores de 2010 pra 2022 one-too-ne")
      }

    cnefe2_com_setor_crosswalk_one2one <- temp_cross |>
      dplyr::select(-code_sector) |> # dropa setor original do cnefe para ficarmos com o encontrado
      dplyr::select(code_address, code_sector = code_tract_2022) |> 
      dplyr::collect()

    # atualiza cnefe_ainda_sem_setor
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe2_com_setor_crosswalk_one2one$code_address) |> 
      dplyr::collect()
  }

  # Malha de setores de 22 ----------------------------------------------
  
  # open census tracts
  setores <- duckspatial::ddbs_open_dataset(census_tracts_2022) |> 
    dplyr::filter(code_state == codigo_uf) |> 
    duckspatial::ddbs_collect()

  # adiciona os codigos de setores de 2010
  setores <- dplyr::left_join(
    x = setores,
    y = cross_walk |> dplyr::select(code_tract_2022, code_tract_2010),
    by = c('code_tract'='code_tract_2022')
  )

  # coloca malha de setores no duckdb
  con <- duckspatial::ddbs_create_conn("tempdir")
  on.exit(duckdb::dbDisconnect(con), add = TRUE)
  duckspatial::ddbs_load(con, quiet = TRUE)
  
  duckspatial::ddbs_write_table(con, setores, "setores", overwrite = TRUE)

  # daqui pra frente começam as operações espaciais
  # get cnefe_ainda_sem_setor to sf
  data.table::setindex(cnefe_ainda_sem_setor, NULL)
  data.table::setDF(cnefe_ainda_sem_setor)
  cnefe_ainda_sem_setor <- sf::st_as_sf(cnefe_ainda_sem_setor, coords = c("lon", "lat"), crs = 4674)

  # tentativa 3 - cross walk com 2010 one2many ----------------------------------------------
  # pontos do cnefe ainda sem setor, e cujo setor de 2010 virou mais de um setor em 22
  # vamos pegar o setor mais proximo dentrod o mesmo municipio

  if (nrow(cnefe_ainda_sem_setor)>0) {
    
    # detecta casos onde um setor de 2010 virou dois setores de 2022
    # fica apenas com casos de 1 pra 1
    cross_walk_one2many <- cross_walk |>
      dplyr::filter(one_to_n > 1) |>
      dplyr::select(-one_to_n, -all_tracts_22)

    # pontos do cnefe ainda sem setor, e cujo setor de 2010 virou mais de um setor em 22
    cnefe_one2many <- cnefe_ainda_sem_setor |> 
      dplyr::filter(code_sector %in% cross_walk_one2many$code_tract_2010) |> 
      dplyr::mutate(code_muni = substring(code_sector, 1, 7) |> as.numeric()) |> 
      dplyr::select(code_muni, code_address, code_sector, geometry)
    
    duckspatial::ddbs_write_table(con, cnefe_one2many, "cnefe_one2many", overwrite = TRUE)

    # query
    # 1 restricts candidate polygons to the same 2010 census tract;
    # 2 calculates the point-to-polygon distance;
    # 3 ranks polygons from closest to farthest for each code_address;
    # 4 returns only the closest code_tract.
    q1 <- glue::glue(
      "SELECT
          c.code_address,
          s.code_tract AS code_sector
      FROM cnefe_one2many AS c
      JOIN setores AS s
          ON c.code_sector = s.code_tract_2010
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY c.code_address
          
          ORDER BY ST_Distance(c.geometry, s.geometry)
          ) = 1;") 

    cnefe3_com_setor_crosswalk_one2many <- DBI::dbGetQuery(con, q1)
    
    # checa se ainda teve algum caso de um setor de 2010 virar dois setores de 2022
    if(isFALSE( nrow(cnefe3_com_setor_crosswalk_one2many) == nrow(cnefe_one2many))) {
      stop("erro no match de se one 2010 to many 2022")
      }

    # atualiza cnefe_ainda_sem_setor
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe3_com_setor_crosswalk_one2many$code_address)

    # nrow(cnefe_ainda_sem_setor)
    # 50757
  }


  # tentativa 4 - join espacial ----------------------------------------------
  
  # para os casos do cnefe cujo setor nao foi encontrado, vamos fazer join
  # espacial para identificar o setor em que cada observação do cnefe se encontra

  if (nrow(cnefe_ainda_sem_setor)>0) {

    temp_join <- duckspatial::ddbs_join(
      x = cnefe_ainda_sem_setor,
      y = "setores", 
      join = 'within',
      conn = con
      ) |>
      duckspatial::ddbs_drop_geometry() |> 
      dplyr::select(code_address, code_sector, code_tract) |> 
      dplyr::collect() |> 
      unique()

    if (nrow(temp_join)>0) {

      cnefe4_com_setor_join <- temp_join |>
        dplyr::filter(!is.na(code_tract)) |> # mantem apeas os encontrados
        dplyr::select(-code_sector) |>  # dropa o codigo do setor q estava registrado no cnefe
        dplyr::select(code_address, code_sector = code_tract) 

        }

    # atualiza cnefe_ainda_sem_setor
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe4_com_setor_join$code_address)

  }


  # tentativa 5 - distancia espacial ----------------------------------------------

  #' ainda podem existir enderecos sem correspondência com nenhum setor de 2002,
  #' porque estes pontos estão um pouco pra fora dos setores de 22
  #' dai pega o setor mais proximo

  if (nrow(cnefe_ainda_sem_setor) > 0) {

    # pontos do cnefe ainda sem setor, e cujo setor de 2010 virou mais de um setor em 22
    cnefe_none <- cnefe_ainda_sem_setor |> 
      dplyr::mutate(code_muni = substring(code_sector, 1, 7) |> as.numeric()) |> 
      dplyr::select(code_muni, code_address, code_sector, geometry)
    
    duckspatial::ddbs_write_table(con, cnefe_none, "cnefe_none", overwrite = TRUE)

    
  # query
    # 1 restricts candidate polygons to the same municipality;
    # 2 calculates the point-to-polygon distance;
    # 3 ranks polygons from closest to farthest for each code_address;
    # 4 returns only the closest code_tract.
    q2 <- glue::glue(
      "SELECT
          c.code_address,
          s.code_tract AS code_sector
      FROM cnefe_none AS c
      JOIN setores AS s
          ON c.code_muni = s.code_muni
      QUALIFY ROW_NUMBER() OVER (
          PARTITION BY c.code_address
          ORDER BY ST_Distance(c.geometry, s.geometry)
          ) = 1;") 

    cnefe5_com_setor_dist <- DBI::dbGetQuery(con, q2)
    
    # base de cnefe_ainda_sem_setor deve ter 0 linhas
    cnefe_ainda_sem_setor <- cnefe_ainda_sem_setor |>
      dplyr::filter_out(code_address %in% cnefe5_com_setor_dist$code_address)

    if(nrow(cnefe_ainda_sem_setor) != 0) {stop('erro: ainda tem ponto cnefe sem setor')}
  
  }


  # junta todas tentativas
  cnefe_setores_corrigidos <- dplyr::bind_rows(
    cnefe1_ja_com_setor,
    cnefe2_com_setor_crosswalk_one2one,
    cnefe3_com_setor_crosswalk_one2many,
    cnefe4_com_setor_join,
    cnefe5_com_setor_dist
    ) |>
    na.omit()


  # isso deveria ser igual
  # checar camila
  all_code_addresses <- cnefe |> dplyr::pull(code_address) |> unique()

  if (isFALSE(nrow(cnefe_setores_corrigidos) == nrow(cnefe))) {

    length(unique(cnefe_setores_corrigidos$code_address)) == length(unique(cnefe$code_address))
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
