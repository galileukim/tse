# set-up ------------------------------------------------------------------
lapply(
  c(
    "tidyverse",
    "R.utils",
    "data.table",
    "stringi",
    "foreach",
    "here"
  ),
  require,
  character.only = T
)

set.seed(1789)

# conversion ibge-tse
identifiers <- fread(
  here("files/cod_ibge_tse.csv"),
  colClasses = c("cod_tse" = "character", "cod_ibge_7" = "character")
) %>% 
  select(
    starts_with("cod")
  ) %>% 
  transmute(
    cod_tse,
    cod_ibge_6 = str_sub(cod_ibge_7, 1, 6)
  )

`%<>%` <- magrittr::`%<>%`