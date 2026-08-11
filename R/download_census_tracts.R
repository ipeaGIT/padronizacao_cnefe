# year <- tar_read(years)[1]
download_census_tracts <- function(year) {
  cli::cli_inform("Baixando setores censitários do ano de {.val {year}}")

  census_tracts <- NULL

  simp <- ifelse(year==2022, FALSE, TRUE)

  while (is.null(census_tracts)) {
    census_tracts <- geobr::read_census_tract(
      code_tract = "all",
      year = year,
      simplified = simp,
      output = "duckdb",
      showProgress = TRUE
      # showProgress = getOption("TARGETS_SHOW_PROGRESS")
    )
  }

  # if (year == 2022) {
  #   census_tracts <- dplyr::mutate(
  #     census_tracts,
  #     code_tract = as.character(code_tract)
  #   )
  # }

  census_tracts <- census_tracts |> 
    dplyr::select(code_tract, code_state, code_muni, geometry)


  cli::cli_inform("Corrigindo eventuais problemas topológicos")

  census_tracts <- census_tracts |> 
    duckspatial::ddbs_make_valid()
    # duckspatial::ddbs_collect()
    # sf::st_make_valid(census_tracts)
  
  # save file locally
  dir.create("./data/", showWarnings = F)
  file_path <- paste0("./data/census_tracts_", year, ".parquet")
  
  duckspatial::ddbs_write_dataset(
    data = census_tracts, 
    path = file_path, 
    crs = "EPSG:4674", 
    overwrite = T, 
    quiet = TRUE
  )
  
  return(file_path)
}
