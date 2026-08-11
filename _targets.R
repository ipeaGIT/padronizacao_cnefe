suppressPackageStartupMessages({
  library(targets)
  library(tarchetypes)
  library(crew)
  library(mirai)
})

ncores <- floor(.9 * parallelly::freeCores()[1])

options(
  "piggyback.verbose" = TRUE,
  "targets.verbose" = TRUE,
  "targets.max_n_threads" = ncores
)

# Set target options: ----------------------------------------------------------
tar_option_set(
  format = "rds",
  memory = "transient",
  garbage_collection = TRUE,
  controller = crew_controller_local(
    workers = ncores
    #,options_local = crew_options_local(log_directory = "./logs/crew_workers")
  ),
  storage = "worker" ,
  retrieval = "worker",
  trust_timestamps = TRUE,
  # error = "null",

  # Packages essentials --------------------------------------------------------
  packages = c('arrow',
               'ipeadatalake',
               'crew',
               'data.table',
               'remotes',
               'dplyr',
               'duckdb',
               'duckspatial',
               'geobr',
               'sfheaders',
               'sf',
               'lwgeom',
               'mirai',
               'parallelly',
               'piggyback',
               'purrr',
               'stringi',
               'stringr',
               'mapview',
               'tarchetypes',
               'visNetwork'
               )
  )

# invisible(lapply(packages, library, character.only = TRUE))


tar_source("./R")

list(
  tar_target(
    name = versao_dados,
    command = "v0.5.0",
    deployment = "main"
    ),

  tar_target(
    name = codigo_uf,
    as.integer(enderecobr::codigos_estados$codigo_estado[1:2]),
    deployment = "main"
  ),

  # download da malha de setores de 2022
  tar_target(
    name = census_tracts_2022,
    command = download_census_tracts(2022),
    format = "file"
  ),

  # identifica o setor censitario ao qual cada endereco pertence
  tar_target(
    name = identificacao_setor,
    command = identificar_setores(codigo_uf, census_tracts_2022),
    pattern = map(codigo_uf),
    format = "file"
  ),

  # padroniza cnefe com enderecobr
  tar_target(
    name = padronizacao,
    command = padronizar_cnefe(codigo_uf, versao_dados),
    pattern = map(codigo_uf),
    format = "file"
  ),

  # agrega cnefe para gerar as varias tabelas do geocodebr para cada uf
  tar_target(
    name = agregacao,
    command = agregar_cnefe(padronizacao, identificacao_setor, versao_dados),
    pattern = map(padronizacao, identificacao_setor),
    format = "file"
  ),

  # junta tabela do estados para ter tabelas nacionais
  tar_target(
    name = uniao_agregados,
    command = unir_cnefe_agregado(agregacao, versao_dados),
    format = "file",
    deployment = "main"
  )
  #,
  ## publica dados no release
  # tar_target(
  #   name = upload,
  #   command = upload_arquivos(uniao_agregados, versao_dados),
  #   deployment = "main"
  # )
)

# check errors
# targets::tar_meta(fields = error, complete_only = TRUE) |> View()


# census_tracts_2022 <- tar_read(census_tracts_2022)
# lapply(
#   X= tar_read(codigo_uf),
#   FUN = function(x){
#
#     message(x)
#     identificar_setores(x, census_tracts_2022)
#
#   }
# )
