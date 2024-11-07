options(piggyback.verbose = TRUE)

suppressPackageStartupMessages({
  library(targets)
  library(dplyr)
})

tar_option_set(trust_timestamps = TRUE)

source("R/padronizacao.R", encoding = "UTF-8")
source("R/upload.R", encoding = "UTF-8")

list(
  tar_target(padronizacao, padronizar_cnefe(), format = "file"),
  tar_target(upload, upload_arquivos(padronizacao), format = "url")
)
