data.table::setDTthreads(percent = 100)

cnefe_agregado2 <- cnefe_agregado[
  ,
  .(lon = mean(lon),
    lat = mean(lat),
    peso = .N
  ),
  by = colunas_agregacao
]


mage <- subset(cnefe_agregado, estado=='RJ' & municipio == 'MAGE')

get_concave_area <- function(lat_vec, lon_vec){

  # lat_vec <- mage$lat[1:100]
  # lon_vec <- mage$lon[1:100]

  # convert to points
  temp_matrix <- matrix( c(lat_vec, lon_vec), ncol = 2, byrow = FALSE)
  temp_sf <- sfheaders::sf_point(temp_matrix, keep = T)
  # temp_sf <- sfheaders::sf_point(temp_matrix[1,], keep = T)
  sf::st_crs(temp_sf) <- 4674


  # points buffer of 5.84 meters based on the best gps precision of cnefe
  # https://biblioteca.ibge.gov.br/visualizacao/livros/liv102063.pdf
  raio <- 5.84

  n_points <- nrow(temp_sf)
  single_point_area <- pi * raio^2

  if ( n_points == 1) {
    return(single_point_area)
  }


  if (n_points %in% 2:4) {

    buff <- sf::st_buffer(x = temp_sf, dist = raio)
    buff <- sf::st_union(buff)
    # plot(buff)

    # back to points
    temp_points <- sfheaders::sfc_cast(buff, "POINT")
    # plot(temp_points)

    # sample 10%
    sample_rows <- sample(1:length(temp_points), size = round(0.1 * length(temp_points)))
    sample_points <- temp_points[sample_rows, ]
    sample_points <- sf::st_as_sf(sample_points, 'POINT')
    # plot(sample_points)

    poly <- concaveman::concaveman(points = sample_points)
    # mapview(poly) + sample_points
    }

  if (n_points >= 5) {
    poly <- concaveman::concaveman(points = temp_sf)
    # mapview(poly) + temp_sf

  }

  # get area
  poly <- sf::st_make_valid(poly)
  area_m2 <- sf::st_area(poly)
  area_m2 <- as.numeric(area_m2)
  # area_m2 <- round(area_m2, digits = 0)
  # plot(poly, add=T)

  return(area_m2)

}


profvis::profvis({
get_concave_area(lat_vec, lon_vec)
})

system.time(
  mage2 <- mage[1:100
    ,
    .(lon = mean(lon),
      lat = mean(lat),
      peso = .N,
      area_m2 = get_concave_area(lat, lon)
    ),
    by = colunas_agregacao
  ]
)

head(mage2)
