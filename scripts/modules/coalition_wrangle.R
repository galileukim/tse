
# coalitions --------------------------------------------------------------
# fix coligacao 2004-2010
coligacao <- fread(
  here("data/raw/election/coalition.csv"),
  integer64 = "character"
)

# fix special characters
coligacao <- coligacao %>% 
  mutate_if(
    is.character,
    funs(
      stringi::stri_trans_general(., "latin-ascii")
    )
  ) %>% 
  mutate_if(
    is.character,
    str_to_lower
  )

# fix codigo tse
coligacao <- coligacao %>% 
  mutate(
    SIGLA_UE = case_when(
      str_detect(SIGLA_UE, "[a-z]+") ~ SIGLA_UE,
      T ~ str_pad(SIGLA_UE, 5, "left", "0")
    )  
  )

# fix colnames
names(coalition) <- names(coalition) %>% 
  str_to_lower

# fix ne and nulo
coligacao[coligacao =="#ne#"|coligacao == "#nulo#"|coligacao == ""] <- NA

# select and rename
coligacao <- coligacao %>%
  transmute(
    election_year = ano_eleicao,
    state = sigla_uf,
    election_type = descricao_eleicao,
    cod_tse = sigla_ue,
    round = num_turno,
    position_code = codigo_cargo,
    party = sigla_partido,
    coalition = str_replace_all(composicao_coligacao, "-", "") %>%
      str_replace_all(., "\\d+", "") %>% 
      str_replace(., "^\\/\\s", ""),
    coalition_name = nome_coligacao,
    coalition_type = tipo_legenda
  ) %>%
  mutate_if(
    is.character,
    str_to_lower
  )

# fix coalition type
coligacao <- coligacao %>% 
  mutate(
    coalition_type = if_else(
      str_detect(coalition_type, "isolado"), 
      "partido isolado",
      coalition_type
    )
  )

# fix partido isolado coalition: attribute party to coalition
coligacao <- coligacao %>% 
  mutate(
    coalition = if_else(
      coalition_type == "partido isolado",
      party,
      coalition
    )
  )

# remove duplicate
# lose 10k = 1.1%
coligacao <- coligacao %>% 
  distinct(
    election_type,
    election_year,
    cod_tse,
    position_code,
    round,
    party,
    .keep_all = T
  )

# group vars
group_var <- names(coligacao)[!(names(coligacao) %in% c("party", "coalition"))]

# fix fed/state elections
# years 2006 and 2010
coligacao_state <- coligacao %>%
  filter(
    election_year %in% c(2006, 2010)
  ) %>%
  group_by_at(
    vars(one_of(group_var))
  ) %>%
  mutate(
    coalition = stri_c(party, collapse = " / ")
  ) %>% 
  ungroup()

# rejoin and replace coalition
coligacao <- coligacao %>%
  filter(
    !election_year %in% c(2006, 2010)
  ) %>% 
  bind_rows(
    coligacao_state
  )

# fix presidential coalitions
missing_coalition <- tibble(
  election_year = 2006,
  election_type = "eleicoes 2006",
  state = "br",
  cod_tse = state,
  coalition_type = "coligacao",
  position_code = rep(1, 5),
  round = c(rep(seq(2), 2), 1),
  party = c(rep("psdb", 2), rep("pt", 2), "psol"),
  coalition = c(rep("psdb/pfl/pps", 2), rep("psol/pcb/pstu", 2), "pt/prb/pc do b"),
  coalition_name = c(rep("coligacao por um brasil decente", 2), rep("frente de esquerda", 2), "a forca do povo")
) %>%
  bind_rows(
    tibble(
      election_year = 2010,
      state = "br",
      cod_tse = state,
      coalition_type = "coligacao",
      election_type = "eleicoes 2010",
      round = rep(seq(2), 2),
      position_code = 1,
      party = c(rep("psdb", 2), rep("pt", 2)),
      coalition = c(rep("psdb/dem/pps/pmn/pt do b/ptb", 2), rep("pt/pmdb/pdt/pc do b/psb/pr/prb/psc/ptc/ptn", 2)),
      coalition_name = c(rep("o brasil pode mais", 2), rep("para o brasil seguir mudando", 2))
    )
  )

# state cols
states <- coligacao %>%
  distinct(
    cod_tse
  ) %>%
  filter(
    str_count(cod_tse) <= 2,
    cod_tse != "br"
  ) %>%
  mutate(
    state = "br"
  )

missing_coalition <- missing_coalition %>%
  bind_rows(
    coligacao %>% 
      filter(position_code <= 2)
  ) %>% 
  left_join(
    states,
    by = c('state')
  ) %>% 
  mutate(
    cod_tse = cod_tse.y
  ) %>%
  select(
    -starts_with("cod_tse.")
  ) %>% 
  mutate(
    state = cod_tse
  )

# bind
coligacao <- rbindlist(
  list(
    coligacao %>% 
      filter(position_code > 2),
    missing_coalition
  ),
  fill = T
)

# write out
coligacao %>% 
  fwrite(
    here("data/wrangle/coalition.csv")
  )
