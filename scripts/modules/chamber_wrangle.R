
# chamber -----------------------------------------------------------------
# elections
election <- list.files(
  here("data/wrangle"),
  pattern = "^election.csv",
  full.names = T
) %>%
  fread(
    nThread = parallel::detectCores() - 1,
    select = c(
      "cpf_candidate",
      "elec_title",
      "candidate_name",
      "cod_ibge_6",
      "cod_tse",
      "round",
      "mun_name",
      "election_year",
      "birthyear",
      "candidate_status",
      "party",
      "campaign",
      "edu",
      "gender",
      "edu_desc",
      "coalition",
      "coalition_name",
      "position",
      "position_code",
      "position_previous",
      "occupation",
      "election_type",
      "outcome",
      "outcome_previous",
      "incumbent",
      "elected",
      "reelected",
      "ideology",
      "vote"
    )
  ) %>%
  filter(
    election_year %in% seq(2000, 2016, 4)
  )

# retain municipalities with only one type of election
# rule out problem of supplementary elections
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    n = n_distinct(election_type)
  ) %>%
  ungroup() %>% 
  filter(n == 1) %>%
  select(-n)

# extract municipalities with no second round
election <- election %>%
  group_by(
    cod_ibge_6,
    election_year
  ) %>%
  mutate(
    round_two = !any(round == 2)
  ) %>%
  ungroup() %>% 
  filter(
    round_two
  ) %>% 
  select(
    -starts_with("round")
  ) %>%
  ungroup

# eliminate defective entries with no elected officials
election <- election %>%
  group_by(
    cod_tse,
    election_type,
    position
  ) %>%
  filter(
    any(str_detect(outcome, "^eleito|media"))
  ) %>%
  ungroup()

# add municipal votes for mayor election
election <- election %>% 
  group_by(
    cod_ibge_6,
    election_year
  ) %>% 
  mutate(
    total_vote_mayor = sum(vote[position == "prefeito"], na.rm = T),
    total_vote_legislative = sum(vote[position == "vereador"], na.rm = T)
  ) %>% 
  ungroup()

# victory margin
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  arrange(
    desc(vote),
    desc(position_code),
    .by_group = T
  ) %>%
  mutate(
    vote_margin =
      (vote[row_number() == 1] - vote[row_number() == 2])/
      vote[row_number() == 2]
  ) %>%
  arrange(
    election_year,
    cod_ibge_6,
    position_code
  ) %>%
  ungroup()

# retain elected officials
election <- election %>%
  filter(
    elected == 1
  )

# coalition size and age
election <- election %>%
  mutate(
    coalition_size = 1 + str_count(coalition, "/"),
    age = election_year - as.integer(birthyear)
  )

# mayor coalition
# fix sao miguel do tapuio
# strangely there are some municipalities without mayor: 8 in total
# prob. because I eliminated supplementary election wins
election <- election %>%
  filter(
    cod_ibge_6 != 221040
  ) %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    mayor = sum(position == "prefeito", na.rm = T)
  ) %>%
  filter(
    mayor == 1
  ) %>%
  mutate(
    mayor_party = party[position == "prefeito"],
    mayor_coalition = coalition[position == "prefeito"]
  ) %>%
  select(-mayor) %>%
  ungroup()

# mayor party vars
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    mayor_party_share = sum(
      party == mayor_party & position == "vereador"
    )/sum(position == "vereador"),
    mayor_party_margin = mayor_party_share - 0.5,
    mayor_party_majority = if_else(
      mayor_party_share > 0.5, 1, 0
    ),
    mayor_party_edu = mean(
      edu[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_campaign = mean(
      campaign[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_age = mean(
      age[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_vote = sum(
      vote[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_ideology_mean = mean(
      ideology[position == "vereador" & party == mayor_party],
      na.rm = T
    ),
    mayor_party_ideology_sd = sd(
      ideology[position == "vereador" & party == mayor_party],
      na.rm = T
    )
  ) %>%
  ungroup()

# mayor coalition vars
# note: coalition denotes that the vereador's party is present
# in mayor's electoral coalition
# note: warning signs displayed due to missing info for 80 candidates.
# choice: drop them
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    coalition_member = if_else(
      str_detect(mayor_coalition, paste0("\\b", party, "\\b")) & position == "vereador",
      1, 0
    ),
    coalition_share = sum(
      coalition_member == 1 & position == "vereador")/
      sum(position == "vereador"),
    coalition_margin = coalition_share - 0.5,
    coalition_majority = if_else(
      coalition_share > 0.5, 1, 0
    ),
    coalition_edu = mean(
      edu[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_campaign = mean(
      campaign[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_age = mean(
      age[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_vote = sum(
      vote[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_party_ideology_mean = mean(
      ideology[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    coalition_party_ideology_sd = sd(
      ideology[position == "vereador" & coalition_member == 1],
      na.rm = T
    ),
    chamber_size = sum(position == "vereador")
  ) %>%
  ungroup()

# opposition structure
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    opposition_share = sum(
      coalition_member != 1 & position == "vereador")/
      sum(position == "vereador"),
    opposition_margin = opposition_share - 0.5,
    opposition_majority = if_else(
      opposition_share > 0.5, 1, 0
    ),
    opposition_edu = mean(
      edu[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_campaign = mean(
      campaign[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_age = mean(
      age[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_vote = sum(
      vote[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_party_ideology_mean = mean(
      ideology[position == "vereador" & coalition_member != 1],
      na.rm = T
    ),
    opposition_party_ideology_sd = sd(
      ideology[position == "vereador" & coalition_member != 1],
      na.rm = T
    )
  ) %>%
  ungroup()

# effective number of parties
election <- election %>%
  group_by(
    election_year,
    cod_ibge_6,
    party
  ) %>%
  mutate(
    party_share = sum(position == "vereador"),
    party_share = party_share/chamber_size,
    first_value = if_else(row_number() == 1, 1, 0)
  ) %>%
  ungroup() %>%
  group_by(
    election_year,
    cod_ibge_6
  ) %>%
  mutate(
    effective_parties = 1/sum(party_share[first_value == 1]^2)
  ) %>%
  ungroup() %>%
  select(
    -first_value,
    -party_share
  )

# retain elected mayors (hiring principals)
# subset
election <- election %>%
  filter(
    position == "prefeito"
  ) %>%
  mutate(
    cod_ibge_6 = as.character(cod_ibge_6)
  ) %>%
  select(
    cod_ibge_6,
    candidate_name,
    cpf_candidate,
    elec_title,
    election_year,
    mun_name,
    mayor_age = age,
    birthyear,
    mayor_edu = edu,
    mayor_male = gender,
    mayor_edu_desc = edu_desc,
    mayor_party = party,
    mayor_campaign = campaign,
    mayor_coalition = coalition,
    mayor_occupation = occupation,
    mayor_coalition_size = coalition_size,
    mayor_incumbent = incumbent,
    mayor_reelected = reelected,
    starts_with("mayor_party_"),
    starts_with("coalition"),
    starts_with("chamber_"),
    starts_with("party_"),
    starts_with("outcome"),
    contains("ideology"),
    starts_with("position"),
    total_vote_mayor,
    total_vote_legislative,
    effective_parties
  ) %>% 
  mutate(
    mayor_male = case_when(
      mayor_male == 2 ~ 1,
      mayor_male == 4 ~ 0,
      T ~ 2
    ),
    mayor_male = ifelse(
      mayor_male == 2, NA, mayor_male
    )
  )

# times in office
election <- election %>% 
  arrange(
    cpf_candidate,
    elec_title,
    candidate_name,
    election_year
  ) %>% 
  group_by(
    cpf_candidate,
    elec_title,
    candidate_name
  ) %>% 
  mutate(
    time_office = row_number() - 1
  ) %>% 
  ungroup()

# reorder dataframe
election <- election %>%
  arrange(
    cod_ibge_6,
    position_code,
    election_year
  ) %>% 
  select(
    order(colnames(.))
  ) %>%
  select(
    cod_ibge_6,
    election_year,
    everything()
  )

# write-out
election %>%
  fwrite(
    here("data/wrangle/election_chamber.csv")
  )
