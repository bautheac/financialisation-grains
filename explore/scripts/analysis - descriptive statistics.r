



library(finRes); library(magrittr); library(doParallel)
# source("explore/scripts/functions - shared.r")

# start cluster ####
cluster <- makeCluster(detectCores() - 1L); registerDoParallel(cluster)
# globals ####
data("tickers_futures", "tickers_cftc", package = "BBGsymbols"); data("exchanges", package = "fewISOs")
storethat <- "/media/storage/Dropbox/code/R/Projects/thesis/data/storethat.sqlite"
periods <- tibble::tibble(period = c(rep("past", 2L), rep("financialization", 2L)), bound = rep(c("start", "end"), 2L),
                          date = c("1997-07-01", "2003-12-31", "2004-01-01", "2008-09-14"))
fields <- tibble::tibble(field = c("FUT_AGGTE_OPEN_INT", "FUT_AGGTE_VOL"), name = c("aggregate open interest", "aggregate volume"))
widths <- tibble::tibble(frequency = c("day", "week", "month"), width = c(252L, 52L, 12L))
# data ####
## load ####
start <- "1996-01-01"; end <- "2008-09-14"
`commodity futures tickers` <- c("KWA Comdty", "W A Comdty", "C A Comdty", "S A Comdty")
`commodity term structure data` <- pullit::pull_futures_market(source = "storethat", type = "term structure", active_contract_tickers = `commodity futures tickers`,
                                                               start = start, end = end, TS_positions = 1L, roll_type = "A", roll_days = 0L, roll_months = 0L,
                                                               roll_adjustment = "N", file = storethat)
`commodity aggregate data` <- pullit::pull_futures_market(source = "storethat", type = "aggregate", active_contract_tickers = `commodity futures tickers`,
                                                          start = start, end = end, file = storethat)
`commodity spot data` <- pullit::pull_futures_market(source = "storethat", type = "spot", active_contract_tickers = `commodity futures tickers`,
                                                     start = start, end = end, file = storethat)
`commodity CFTC data` <- pullit::pull_futures_CFTC(source = "storethat", active_contract_tickers = `commodity futures tickers`, start = start, end = end, file = storethat)
## munging ####
CFTC <- dplyr::left_join(`commodity CFTC data`@data, dplyr::select(tickers_cftc, -c(name, `active contract ticker`)), by = "ticker") %>% 
  dplyr::filter(format == "legacy", underlying == "futures only", unit == "contracts", participant %in% c("commercial", "non-commercial", "total"), 
                position %in% c("long", "short", "net")) %>%  dplyr::select(ticker = `active contract ticker`, participant, position, date, value) %>% 
  dplyr::mutate(date = RQuantLib::advance(dates = date, n = 3L, timeUnit = 0L, calendar = "UnitedStates/NYSE"))

# commodity futures ####
`term structure` <- dplyr::left_join(`commodity term structure data`@data, `commodity term structure data`@term_structure_tickers, by = "ticker") %>% 
  dplyr::select(ticker = `active contract ticker`, field, date, value) %>% dplyr::filter(field == "PX_LAST") %>% dplyr::mutate(field = "futures price")
spot <- dplyr::filter(`commodity spot data`@data, field == "PX_LAST") %>% dplyr::mutate(field = "spot price") %>% dplyr::mutate(value = value * 100L)
basis <- dplyr::bind_rows(`term structure`, spot) %>% dplyr::group_by(ticker) %>% tidyr::spread(field, value) %>% dplyr::mutate(basis = `futures price` - `spot price`) %>%
  dplyr::select(ticker, date, basis) %>% tidyr::gather(field, value, -c(ticker, date)) %>% dplyr::ungroup()
aggregate <- dplyr::left_join(`commodity aggregate data`@data, fields, by = "field") %>% dplyr::select(ticker, field = name, date, value)
`commodity futures data` <- dplyr::bind_rows(`term structure`, spot, basis, aggregate)

## market variables ####
### by year ####
years <- foreach(y = unique(`commodity futures data`$ticker), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  data <- dplyr::filter(`commodity futures data`, ticker == y) %>% dplyr::select(ticker, field, date, value) %>% 
    dplyr::mutate(year = lubridate::year(date))

  # levels 
  levels <- dplyr::group_by(data, ticker, field, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = NA, type = "levels") %>%
    dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
  # returns 
  ## days
  days <- dplyr::group_by(data, ticker, field) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::group_by(ticker, field, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = "day", type = "returns") %>%
    dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
  ## other frequencies
  others <- lapply(c("week", "month"), function(x){
    dplyr::mutate(data, unit = do.call(what = x, args = list(date))) %>% dplyr::group_by(ticker, field, year, unit) %>% 
      dplyr::filter(dplyr::row_number() == n()) %>% dplyr::group_by(ticker, field) %>% dplyr::select(-unit) %>%
      dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
      dplyr::group_by(ticker, field, year) %>% 
      dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                       mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                       sd = sd(value, na.rm = T)) %>% 
      dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = x, type = "returns") %>%
      dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
  }) %>% dplyr::bind_rows()
  
  dplyr::bind_rows(list(levels, days, others))
}

#### by subperiod ####
subperiods <- foreach(y = unique(`commodity futures data`$ticker), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  lapply(unique(periods$period), function(z){
    
    start <- dplyr::filter(periods, period == z, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
    end <- dplyr::filter(periods, period == z, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
    
    data <- dplyr::filter(`commodity futures data`, ticker == y, date >= as.Date(start), date <= as.Date(end)) %>% 
      dplyr::select(ticker, field, date, value) %>% dplyr::mutate(year = lubridate::year(date))
    
    # levels 
    levels <- dplyr::group_by(data, ticker, field) %>% 
      dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                       mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                       sd = sd(value, na.rm = T)) %>% 
      dplyr::ungroup() %>% dplyr::mutate(period = z, frequency = NA, type = "levels", year = NA) %>%
      dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
    # returns 
    ## days
    days <- dplyr::group_by(data, ticker, field) %>% 
      dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
      dplyr::group_by(ticker, field) %>% 
      dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                       mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                       sd = sd(value, na.rm = T)) %>% 
      dplyr::ungroup() %>% dplyr::mutate(period = z, frequency = "day", type = "returns", year = NA) %>%
      dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
    ## other frequencies
    others <- lapply(c("week", "month"), function(x){
      dplyr::mutate(data, year = lubridate::year(date), unit = do.call(what = x, args = list(date))) %>% dplyr::group_by(ticker, field, year, unit) %>% 
        dplyr::filter(dplyr::row_number() == n()) %>% dplyr::group_by(ticker, field) %>% dplyr::select(-c(year, unit)) %>%
        dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
        dplyr::group_by(ticker, field) %>% 
        dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                         mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                         sd = sd(value, na.rm = T)) %>% 
        dplyr::ungroup() %>% dplyr::mutate(period = z, frequency = x, type = "returns", year = NA) %>%
        dplyr::select(type, frequency, ticker, field, period, year, min, max, mean, sd)
    }) %>% dplyr::bind_rows()
    
    dplyr::bind_rows(list(levels, days, others))
  }) %>% dplyr::bind_rows()
}

`market variables` <- dplyr::bind_rows(list(years, subperiods))


## CFTC variables ####
### contracts ####
CFTC <- dplyr::left_join(`commodity CFTC data`@data, dplyr::select(tickers_cftc, -c(name, `active contract ticker`)), by = "ticker") %>% 
  dplyr::filter(format == "legacy", underlying == "futures only", unit == "contracts", participant %in% c("commercial", "non-commercial", "total"), 
                position %in% c("long", "short", "net")) %>%  dplyr::select(ticker = `active contract ticker`, participant, position, date, value) %>% 
  dplyr::mutate(date = RQuantLib::advance(dates = date, n = 3L, timeUnit = 0L, calendar = "UnitedStates/NYSE"))
#### by year ####
combinations <- dplyr::distinct(CFTC, ticker, participant, position) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; participant <- combinations[y, "participant"]; position <- combinations[y, "position"]
  
  data <- dplyr::filter(CFTC, ticker == !! ticker, participant == !! participant, position == !! position) %>% 
    dplyr::mutate(year = lubridate::year(date)) %>% dplyr::select(ticker, participant, position, date, year, value)
  
  # levels 
  levels <- dplyr::group_by(data, ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = NA, type = "levels") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  # returns 
  ## weeks
  days <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::group_by(ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = "week", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  months <- dplyr::mutate(data, month = lubridate::month(date)) %>% dplyr::group_by(ticker, participant, position, year, month) %>% 
    dplyr::filter(dplyr::row_number() == n()) %>% dplyr::ungroup() %>% dplyr::select(-month) %>%
    dplyr::group_by(ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::group_by(ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = "month", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  dplyr::bind_rows(list(levels, days, months))
}

#### by subperiod ####
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows() %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; participant <- combinations[y, "participant"]
  position <- combinations[y, "position"]; period <- combinations[y, "period"]
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  data <- dplyr::filter(CFTC, ticker == !! ticker, participant == !! participant, position == !! position,
                        date >= as.Date(start), date <= as.Date(end)) %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, participant, position, date, year, value)
  
  # levels 
  levels <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = NA, type = "levels") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  # returns 
  ## weeks
  days <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = "week", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  months <- dplyr::mutate(data, month = lubridate::month(date)) %>% dplyr::group_by(ticker, participant, position, year, month) %>% 
    dplyr::filter(dplyr::row_number() == n()) %>% dplyr::ungroup() %>% dplyr::select(-c(year, month)) %>%
    dplyr::group_by(ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = "month", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  dplyr::bind_rows(list(levels, days, months))
}

contracts <- dplyr::bind_rows(years, subperiods) %>% dplyr::mutate(unit = "contracts") %>%
  dplyr::select(type, frequency, ticker, unit, participant, position, period, year, min, max, mean, sd) %>%
  dplyr::arrange(type, frequency, ticker, unit, participant, position, period, year)

### traders ####
CFTC <- dplyr::left_join(`commodity CFTC data`@data, dplyr::select(tickers_cftc, -c(name, `active contract ticker`)), by = "ticker") %>% 
  dplyr::filter(format == "legacy", underlying == "futures only", unit == "traders", participant %in% c("commercial", "non-commercial", "total"), 
                position %in% c("long", "short", "net")) %>%  dplyr::select(ticker = `active contract ticker`, participant, position, date, value) %>% 
  dplyr::mutate(date = RQuantLib::advance(dates = date, n = 3L, timeUnit = 0L, calendar = "UnitedStates/NYSE"))
#### by year ####
combinations <- dplyr::distinct(CFTC, ticker, participant, position) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; participant <- combinations[y, "participant"]; position <- combinations[y, "position"]
  
  data <- dplyr::filter(CFTC, ticker == !! ticker, participant == !! participant, position == !! position) %>% 
    dplyr::mutate(year = lubridate::year(date)) %>% dplyr::select(ticker, participant, position, date, year, value)
  
  # levels 
  levels <- dplyr::group_by(data, ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = NA, type = "levels") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  # returns 
  ## weeks
  days <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::group_by(ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = "week", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  months <- dplyr::mutate(data, month = lubridate::month(date)) %>% dplyr::group_by(ticker, participant, position, year, month) %>% 
    dplyr::filter(dplyr::row_number() == n()) %>% dplyr::ungroup() %>% dplyr::select(-month) %>%
    dplyr::group_by(ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::group_by(ticker, participant, position, year) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = "year", frequency = "month", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  dplyr::bind_rows(list(levels, days, months))
}
#### by subperiod ####
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows() %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; participant <- combinations[y, "participant"]
  position <- combinations[y, "position"]; period <- combinations[y, "period"]
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  data <- dplyr::filter(CFTC, ticker == !! ticker, participant == !! participant, position == !! position,
                        date >= as.Date(start), date <= as.Date(end)) %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, participant, position, date, year, value)
  
  # levels 
  levels <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = NA, type = "levels") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  # returns 
  ## weeks
  days <- dplyr::group_by(data, ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = "week", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  months <- dplyr::mutate(data, month = lubridate::month(date)) %>% dplyr::group_by(ticker, participant, position, year, month) %>% 
    dplyr::filter(dplyr::row_number() == n()) %>% dplyr::ungroup() %>% dplyr::select(-c(year, month)) %>%
    dplyr::group_by(ticker, participant, position) %>% 
    dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>%
    dplyr::summarise(min = min(value, na.rm = T), max = max(value, na.rm = T), 
                     mean = list(tryCatch({ t.test(value, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })), 
                     sd = sd(value, na.rm = T)) %>% 
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, frequency = "month", type = "returns") %>%
    dplyr::select(type, frequency, ticker, participant, position, period, year, min, max, mean, sd)
  
  dplyr::bind_rows(list(levels, days, months))
}

traders <- dplyr::bind_rows(years, subperiods) %>% dplyr::mutate(unit = "traders") %>%
  dplyr::select(type, frequency, ticker, unit, participant, position, period, year, min, max, mean, sd) %>%
  dplyr::arrange(type, frequency, ticker, unit, participant, position, period, year)

`CFTC variables` <- dplyr::bind_rows(contracts, traders)


## conditional correlations ####
### by year ####
years <- foreach(y = unique(`commodity futures data`$ticker), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  data <- dplyr::filter(`commodity futures data`, ticker == y, field %in% c("futures price", "spot price")) %>% dplyr::select(ticker, field, date, value)
  width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
  levels <- tidyr::spread(data, field, value) %>% 
    dplyr::mutate(rho = zoo::rollapplyr(., width, function(x) cor(as.numeric(x[, "futures price"]), as.numeric(x[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
    dplyr::mutate(type = "levels", frequency = "day", year = lubridate::year(date)) %>% dplyr::select(type, frequency, ticker, date, rho, year)
  days <- dplyr::group_by(data, field) %>% dplyr::mutate(value = (value / dplyr::lag(value, 1L)) - 1L) %>% dplyr::ungroup() %>% tidyr::spread(field, value) %>%
    dplyr::mutate(rho = zoo::rollapplyr(., width, function(x) cor(as.numeric(x[, "futures price"]), as.numeric(x[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
    dplyr::mutate(type = "returns", frequency = "day", year = lubridate::year(date)) %>% dplyr::select(type, frequency, ticker, date, rho, year)
  others <- lapply(c("week", "month"), function(x){
    width <- dplyr::filter(widths, frequency == x) %>% dplyr::select(width) %>% purrr::flatten_int()
    dplyr::mutate(data, year = lubridate::year(date), unit = do.call(what = x, args = list(date))) %>% dplyr::group_by(ticker, field, year, unit) %>% 
      dplyr::filter(dplyr::row_number() == n()) %>% dplyr::group_by(ticker, field) %>% dplyr::select(-unit) %>%
      dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>% tidyr::spread(field, value) %>%
      dplyr::mutate(rho = zoo::rollapplyr(., width, function(z) cor(as.numeric(z[, "futures price"]), as.numeric(z[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
      dplyr::mutate(type = "returns", frequency = x) %>% dplyr::select(type, frequency, ticker, date, rho, year)
  }) %>% dplyr::bind_rows()
  data <- dplyr::bind_rows(list(levels, days, others)) %>% dplyr::filter(! is.na(rho))
  
  dplyr::group_by(data, type, frequency, ticker, year) %>% 
    dplyr::summarise(min = min(rho, na.rm = T), max = max(rho, na.rm = T), mean = list(tryCatch({ t.test(rho, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })),
                     sd = sd(rho, na.rm = T)) %>%
    dplyr::ungroup() %>% dplyr::mutate(period = NA) %>% dplyr::select(type, frequency, ticker, period, year, min, max, mean, sd)
}

### by subperiod ####
combinations <- expand.grid(unique(`commodity futures data`$ticker), unique(periods$period), stringsAsFactors = F) %>% setNames(c("commodity", "period")) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; period <- combinations[y, "period"]
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, date >= as.Date(start), date <= as.Date(end), field %in% c("futures price", "spot price")) %>% 
    dplyr::select(ticker, field, date, value)
  width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
  levels <- tidyr::spread(data, field, value) %>% 
    dplyr::mutate(rho = zoo::rollapplyr(., width, function(x) cor(as.numeric(x[, "futures price"]), as.numeric(x[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
    dplyr::mutate(type = "levels", frequency = "day") %>% dplyr::select(type, frequency, ticker, date, rho)
  days <- dplyr::group_by(data, field) %>% dplyr::mutate(value = (value / dplyr::lag(value, 1L)) - 1L) %>% dplyr::ungroup() %>% tidyr::spread(field, value) %>%
    dplyr::mutate(rho = zoo::rollapplyr(., width, function(x) cor(as.numeric(x[, "futures price"]), as.numeric(x[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
    dplyr::mutate(type = "returns", frequency = "day") %>% dplyr::select(type, frequency, ticker, date, rho)
  others <- lapply(c("week", "month"), function(x){
    width <- dplyr::filter(widths, frequency == x) %>% dplyr::select(width) %>% purrr::flatten_int()
    dplyr::mutate(data, year = lubridate::year(date), unit = do.call(what = x, args = list(date))) %>% dplyr::group_by(ticker, field, year, unit) %>% 
      dplyr::filter(dplyr::row_number() == n()) %>% dplyr::group_by(ticker, field) %>% dplyr::select(-c(year, unit)) %>%
      dplyr::mutate(value = (value / dplyr::lag(value, 1L) - 1L), value = ifelse(is.infinite(value), NA, value)) %>% tidyr::spread(field, value) %>%
      dplyr::mutate(rho = zoo::rollapplyr(., width, function(z) cor(as.numeric(z[, "futures price"]), as.numeric(z[, "spot price"]), use = "pairwise.complete.obs"), fill = NA, by.column = F)) %>%
      dplyr::mutate(type = "returns", frequency = x) %>% dplyr::select(type, frequency, ticker, date, rho)
  }) %>% dplyr::bind_rows()
  data <- dplyr::bind_rows(list(levels, days, others)) %>% dplyr::filter(! is.na(rho))
  
  dplyr::group_by(data, type, frequency, ticker) %>% 
    dplyr::summarise(min = min(rho, na.rm = T), max = max(rho, na.rm = T), mean = list(tryCatch({ t.test(rho, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })),
                     sd = sd(rho, na.rm = T)) %>%
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA) %>% dplyr::select(type, frequency, ticker, period, year, min, max, mean, sd)
}

`conditional correlations` <- dplyr::bind_rows(list(years, subperiods))

`market variables` <- dplyr::bind_rows(list(`market variables`, `conditional correlations`))



## volatilities ####
### fields ####
fields <- tibble::tibble(name = forcats::as_factor(c("Open", "High", "Low", "Close")), symbol = c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST"))
### data ####
`term structure` <- dplyr::left_join(`commodity term structure data`@data, `commodity term structure data`@term_structure_tickers, by = "ticker") %>% 
  dplyr::select(ticker = `active contract ticker`, field, date, value) %>% dplyr::filter(field %in% c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST")) %>% 
  dplyr::mutate(variable = "futures price")
spot <- dplyr::filter(`commodity spot data`@data, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")) %>% dplyr::mutate(variable = "spot price") 
basis <- dplyr::bind_rows(dplyr::filter(`term structure`, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")), spot) %>% dplyr::group_by(ticker, field) %>% 
  tidyr::spread(variable, value) %>% dplyr::mutate(basis = `futures price` - `spot price`) %>% dplyr::select(ticker, field, date, basis) %>% 
  tidyr::gather(variable, value, -c(ticker, field, date)) %>% dplyr::ungroup()
`commodity futures data` <- dplyr::bind_rows(`term structure`, basis) %>% dplyr::left_join(fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, variable, field = name, date, value)
### estimators ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
### by year ####
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable) %>% dplyr::select(field, date, value) %>% 
    dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis") {
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
  volatilities <- dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = 252L)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(year = lubridate::year(date))
  
  dplyr::group_by(volatilities, year) %>% 
    dplyr::summarise(min = min(volatility, na.rm = T), max = max(volatility, na.rm = T), 
                     mean = list(tryCatch({ t.test(volatility, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })),
                     sd = sd(volatility, na.rm = T)) %>%
    dplyr::ungroup() %>% dplyr::mutate(period = NA, estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(variable, ticker, estimator, period, year, min, max, mean, sd)
}

### by subperiod ####
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(field, date, value) %>% dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis") {
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
  volatilities <- dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = 252L)) %>%
    dplyr::filter(! is.na(volatility))
  
  dplyr::summarise(volatilities, min = min(volatility, na.rm = T), max = max(volatility, na.rm = T), 
                   mean = list(tryCatch({ t.test(volatility, alternative = "two.sided", mu = 0, na.rm = T) }, error = function(e) { NA })),
                   sd = sd(volatility, na.rm = T)) %>%
    dplyr::ungroup() %>% dplyr::mutate(period = !! period, year = NA, estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(variable, ticker, estimator, period, year, min, max, mean, sd)
}

volatilities <- dplyr::bind_rows(years, subperiods)


statistics <- tibble::tibble(analysis = c("market variables", "CFTC variables", "volatilities"), results = list(`market variables`, `CFTC variables`, volatilities))

parallel::stopCluster(cluster)
saveRDS(statistics, file = "explore/results/descriptive statistics.rds")