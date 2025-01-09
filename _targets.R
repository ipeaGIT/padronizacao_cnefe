options(piggyback.verbose = TRUE)

suppressPackageStartupMessages({
  library(targets)
  library(dplyr)
})

tar_option_set(trust_timestamps = TRUE)

source("R/padronizacao.R", encoding = "UTF-8")
source("R/agregacao.R", encoding = "UTF-8")
source("R/upload.R", encoding = "UTF-8")

list(
  tar_target(versao_dados, "v0.1.0"),
  tar_target(padronizacao, padronizar_cnefe(versao_dados), format = "file"),
  tar_target(agregacao, agregar_cnefe(padronizacao, versao_dados), format = "file"),
  tar_target(upload, upload_arquivos(agregacao, versao_dados))
)
