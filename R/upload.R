# dir_dados <- tar_read(agregacao, branch=1)
# versao_dados <- tar_read(versao_dados)
upload_arquivos <- function(dir_dados, versao_dados) {

  dir_dados <- dir_dados[1]
  arquivos_cnefe <- list.files(dir_dados, full.names = TRUE)

  # tenta criar um release pra fazer upload do cnefe padronizado. se release já
  # existe a função retorna um warning. no caso, fazemos um "upgrade" de warning
  # pra erro

  tryCatch(
    piggyback::pb_release_create(
      "ipeaGIT/padronizacao_cnefe",
      tag = versao_dados,
      body = paste0("CNEFE padronizado ", versao_dados)
    ),
    warning = function(cnd) erro_release_existente(versao_dados)
  )

  # o github tem um pequeno lagzinho pra identificar que o release foi criado,
  # então nós damos um sleep de 2 segundos aqui só pra garantir que a chamada
  # abaixo da pb_upload() não dá ruim.
  # issue relacionado: https://github.com/ropensci/piggyback/issues/101
  Sys.sleep(2)

  purrr::walk(
    arquivos_cnefe,
    function(arq) {
      piggyback::pb_upload(
        arq,
        repo = "ipeaGIT/padronizacao_cnefe",
        tag = versao_dados
      )
    }
  )

  endereco_release <- paste0(
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/",
    versao_dados
  )

  return(endereco_release)
}

erro_release_existente <- function(versao_dados) {
  cli::cli_abort(
    c(
      "O release {.val {versao_dados}} j\u00e1 existe.",
      "i" = "Por favor, use uma nova tag ou apague o release existente."
    ),
    call = rlang::caller_env(n = 5)
  )
}
