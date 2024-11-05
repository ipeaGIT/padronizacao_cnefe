suppressPackageStartupMessages({
  library(targets)
})

source("R/padronizacao.R", encoding = "UTF-8")

list(
  tar_target(padronizacao, padronizar_cnefe())
)
