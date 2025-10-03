options(
  piggyback.verbose = TRUE,
  "targets.verbose" = TRUE
  )

suppressPackageStartupMessages({
  library(targets)
  library(tarchetypes)
  library(crew)
  library(mirai)
  library(data.table)
  library(dplyr)
  library(enderecobr)
  library(piggyback)
  library(purrr)
  library(raster)
  library(sf)
  })

tar_option_set(
  trust_timestamps = TRUE,
  controller = crew_controller_local(workers = 12)
  )

source("R/padronizacao.R", encoding = "UTF-8")
source("R/agregacao.R", encoding = "UTF-8")
source("R/upload.R", encoding = "UTF-8")

list(

  tar_target(name = versao_dados,
             command = "v0.3.0"
             ),

  tar_target(name = code_uf,
             command = c(11L, 12L, 13L, 14L, 15L, 16L, 17L, 21L, 22L, 23L, 24L, 25L,
                         26L, 27L, 28L, 29L, 31L, 32L, 33L, 35L, 41L, 42L, 43L, 50L, 51L,
                         52L, 53L)
             ),

  tar_target(name = padronizacao,
             command = padronizar_cnefe(state_i = code_uf,
                                        versao_dados),
             pattern = map(code_uf),
             format = "file"
  ),

  # seria ideal achar uma forma de paralelizar a agregacao pq eh a etapa mais demorada
  tar_target(name = agregacao,
             command = agregar_cnefe(endereco_cnefe = padronizacao,
                                     versao_dados = versao_dados),
             format = "file"
             ),

  tar_target(name = upload,
             command = upload_arquivos(agregacao, versao_dados))
  )
