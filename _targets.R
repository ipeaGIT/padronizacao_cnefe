options(
  "piggyback.verbose" = TRUE,
  "targets.verbose" = TRUE,
  "targets.max_n_threads" = 12
)

suppressPackageStartupMessages({
  library(targets)
  library(tarchetypes)
  # library(crew)
  # library(mirai)
  # library(data.table)
  # library(dplyr)
  # library(enderecobr)
  # library(piggyback)
  # library(purrr)
  # library(raster)
  # library(sf)
})

control_max_paralelo <- crew::crew_controller_local(
  "max_paralelo",
  workers = getOption("targets.max_n_threads")
)

tar_option_set(
  trust_timestamps = TRUE,
  controller = crew::crew_controller_group(
    control_max_paralelo
  ),
  resources = tar_resources(
    crew = tar_resources_crew(controller = "max_paralelo")
  )
)

source("R/padronizacao.R", encoding = "UTF-8")
source("R/agregacao.R", encoding = "UTF-8")
source("R/upload.R", encoding = "UTF-8")

list(
  tar_target(versao_dados, "v0.4.0"),
  tar_target(codigo_uf, as.integer(enderecobr::codigos_estados$codigo_estado)),

  tar_target(
    padronizacao,
    padronizar_cnefe(state_i = code_uf, versao_dados),
    pattern = map(code_uf),
    format = "file"
  ),

  # seria ideal achar uma forma de paralelizar a agregacao pq eh a etapa mais demorada
  tar_target(
    name = agregacao,
    command = agregar_cnefe(
      endereco_cnefe = padronizacao,
      versao_dados = versao_dados
    ),
    format = "file"
  ),

  tar_target(name = upload, command = upload_arquivos(agregacao, versao_dados))
)
