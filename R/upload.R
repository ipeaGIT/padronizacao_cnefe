# dir_dados <- tar_read(padronizacao)
upload_arquivos <- function(dir_dados) {
  arquivos_cnefe <- list.files(dir_dados, full.names = TRUE)

  tmpdir <- tempfile("cnefe_padronizado_zipado")
  dir.create(tmpdir)

  arquivos_zipados <- purrr::map_chr(
    arquivos_cnefe,
    function(arq) {
      nome_dir <- basename(arq)

      zip::zipr(
        file.path(tmpdir, paste0(nome_dir, ".zip")),
        files = arq
      )
    }
  )

  nome_tag <- "v0.1.0"

  # tenta criar um release pra fazer upload do cnefe padronizado. se release já
  # existe a função retorna um warning. no caso, fazemos um "upgrade" de warning
  # pra erro

  tryCatch(
    piggyback::pb_release_create(
      "ipeaGIT/padronizacao_cnefe",
      tag = nome_tag,
      body = paste0("CNEFE padronizado ", nome_tag)
    ),
    warning = function(cnd) erro_release_existente(nome_tag)
  )

  # o github tem um pequeno lagzinho pra identificar que o release foi criado,
  # então nós damos um sleep de 2 segundos aqui só pra garantir que a chamada
  # abaixo da pb_upload() não dá ruim.
  # issue relacionado: https://github.com/ropensci/piggyback/issues/101
  Sys.sleep(2)

  purrr::walk(
    arquivos_zipados,
    function(arq) {
      piggyback::pb_upload(
        arq,
        repo = "ipeaGIT/padronizacao_cnefe",
        tag = nome_tag
      )
    }
  )

  endereco_release <- paste0(
    "https://github.com/ipeaGIT/padronizacao_cnefe/releases/",
    nome_tag
  )

  return(endereco_release)
}

erro_release_existente <- function(nome_tag) {
  cli::cli_abort(
    c(
      "O release {.val {nome_tag}} j\u00e1 existe.",
      "i" = "Por favor, use uma nova tag ou apague o release existente."
    ),
    call = rlang::caller_env(n = 5)
  )
}
