



library(finRes); library(magrittr); library(doParallel)
# source("explore/scripts/functions - shared.r")

# start cluster ####
cluster <- makeCluster(detectCores()); registerDoParallel(cluster)
# globals ####
data("tickers_futures", "tickers_cftc", package = "BBGsymbols"); data("exchanges", package = "fewISOs")
storethat <- "/media/storage/Dropbox/code/R/Projects/thesis/data/storethat.sqlite"
periods <- tibble::tibble(period = c(rep("past", 2L), rep("financialization", 2L)), bound = rep(c("start", "end"), 2L),
                          date = c("1997-07-01", "2003-12-31", "2004-01-01", "2008-09-14"))
fields <- tibble::tibble(name = forcats::as_factor(c("Open", "High", "Low", "Close", "OI", "volume")), 
                         symbol = c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "FUT_AGGTE_OPEN_INT", "FUT_AGGTE_VOL"))
widths <- tibble::tibble(frequency = c("day", "week", "month"), width = c(252L, 52L, 12L))
# data ####
## loading ####
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
CFTC <- dplyr::left_join(`commodity CFTC data`@data, dplyr::select(tickers_cftc, -c(name, `active contract ticker`)), by = "ticker") %>% 
  dplyr::filter(format == "legacy", underlying == "futures only", unit == "contracts", participant %in% c("commercial", "non-commercial", "total"), 
                position %in% c("long", "short", "net")) %>%  dplyr::select(`active contract ticker`, participant, position, date, value) %>% 
  dplyr::mutate(date = RQuantLib::advance(dates = date, n = 3L, timeUnit = 0L, calendar = "UnitedStates/NYSE"))
## munging ####
`term structure` <- dplyr::left_join(`commodity term structure data`@data, `commodity term structure data`@term_structure_tickers, by = "ticker") %>% 
  dplyr::select(ticker = `active contract ticker`, field, date, value) %>% dplyr::filter(field %in% c("PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST")) %>% 
  dplyr::mutate(variable = "futures price")
spot <- dplyr::filter(`commodity spot data`@data, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")) %>% dplyr::mutate(variable = "spot price") 
basis <- dplyr::bind_rows(dplyr::filter(`term structure`, field %in% c("PX_HIGH", "PX_LOW", "PX_LAST")), spot) %>% dplyr::group_by(ticker, field) %>% 
  tidyr::spread(variable, value) %>% dplyr::mutate(basis = `futures price` - `spot price`) %>% dplyr::select(ticker, field, date, basis) %>% 
  tidyr::gather(variable, value, -c(ticker, field, date)) %>% dplyr::ungroup()
`commodity futures data` <- dplyr::bind_rows(`term structure`, basis) %>% dplyr::left_join(fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, variable, field = name, date, value) %>% dplyr::arrange(ticker, variable, field, date)

# market variables ####
width <- dplyr::filter(widths, frequency == "day") %>% dplyr::select(width) %>% purrr::flatten_int()
## volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable) %>% dplyr::select(field, date, value) %>% 
    dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}
## munging ####
contemporaneous <- dplyr::left_join(`commodity aggregate data`@data, fields, by = c("field" = "symbol")) %>% dplyr::select(ticker, field = name, date, value) %>%
  dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character))
combinations <- dplyr::distinct(contemporaneous, ticker, field)
lagged <- lapply(1L:nrow(combinations), function(x) { ticker <- combinations[x, "ticker"]; field <- combinations[x, "field"]
  dplyr::filter(contemporaneous, ticker == !! ticker, field == !! field) %>% dplyr::mutate(value = dplyr::lag(value, 1L), field = paste("lagged", field))
}) %>% dplyr::bind_rows()
aggregate <- dplyr::bind_rows(contemporaneous, lagged) %>% dplyr::arrange(ticker, field, date)
## 1-factor models ####
### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(aggregate$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)

  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]

  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field == !! regressor) %>% tidyr::spread(field, value)

  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% dplyr::select(ticker, date, year, dplyr::everything()) %>%
    dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[5L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}
### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(aggregate$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)

  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  regressor <- combinations[y, "regressor"]; period <- combinations[y, "period"]

  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()

  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(date), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field == !! regressor, date >= as.Date(date), date <= as.Date(end)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[4L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}

`1-factor models` <- dplyr::bind_rows(years, subperiods)
## 2-factor models ####
regressors <- as.data.frame(t(combn(unique(aggregate$field), 2L))) %>% setNames(c("regressor 1", "regressor 2"))
### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(regressors), function(x) dplyr::mutate(combinations, `regressor 1` = regressors[x, "regressor 1"], `regressor 2` = regressors[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% dplyr::select(ticker, date, year, dplyr::everything()) %>%
    dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}
### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(regressors), function(x) dplyr::mutate(combinations, `regressor 1` = regressors[x, "regressor 1"], `regressor 2` = regressors[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`), date >= as.Date(start), date <= as.Date(end)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}

`2-factor models` <- dplyr::bind_rows(years, subperiods)
## 4-factor models ####
regressors <- as.data.frame(t(combn(unique(aggregate$field), 4L))) %>% setNames(c("regressor 1", "regressor 2", "regressor 3", "regressor 4"))
### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(regressors), function(x) 
  dplyr::mutate(combinations, `regressor 1` = regressors[x, "regressor 1"], `regressor 2` = regressors[x, "regressor 2"], 
                `regressor 3` = regressors[x, "regressor 3"], `regressor 4` = regressors[x, "regressor 4"])) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% dplyr::select(ticker, date, year, dplyr::everything()) %>% 
    dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "` + `", names(data)[8L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, 
                  `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], `regressor 3` = names(data)[7L], `regressor 4` = names(data)[8L], 
                  period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}
### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(regressors), function(x) 
  dplyr::mutate(combinations, `regressor 1` = regressors[x, "regressor 1"], `regressor 2` = regressors[x, "regressor 2"], 
                `regressor 3` = regressors[x, "regressor 3"], `regressor 4` = regressors[x, "regressor 4"])) %>% dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]
  estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(aggregate, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`), date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, 
                  `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], `regressor 3` = names(data)[6L], `regressor 4` = names(data)[7L], 
                  period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}

`4-factor models` <- dplyr::bind_rows(years, subperiods)

`market variables` <- dplyr::bind_rows(`1-factor models`, `2-factor models`, `4-factor models`)


# CFTC variables ####
width <- dplyr::filter(widths, frequency == "week") %>% dplyr::select(width) %>% purrr::flatten_int()
## contemparaneous ####
### regressors ####
Working <- dplyr::filter(CFTC, participant == "commercial", position %in% c("long", "short")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(regime = ifelse(short > long, 1L, 0L), total = long + short) %>% 
  dplyr::select(`active contract ticker`, date, regime, total) %>% dplyr::ungroup()
Working <- dplyr::filter(CFTC, participant == "non-commercial", position %in% c("long", "short")) %>% dplyr::select(-participant) %>%
  dplyr::left_join(Working, by = c("active contract ticker", "date")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(`Working's T` = (ifelse(regime == 1L, short, long) / total)) %>%
  dplyr::select(`active contract ticker`, date, `Working's T`) %>% tidyr::gather(field, value, -c(`active contract ticker`, date)) %>%
  dplyr::ungroup()
regressors <- dplyr::bind_rows(dplyr::filter(CFTC, position == "net") %>% dplyr::select(`active contract ticker`, field = participant, date, value), Working) %>%
  dplyr::rename(ticker = `active contract ticker`)
contemporaneous <- dplyr::left_join(dplyr::filter(`commodity aggregate data`@data, field == "FUT_AGGTE_VOL"), fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, field = name, date, value) %>% dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character)) %>% dplyr::filter(date %in% unique(regressors$date))
regressors <- dplyr::bind_rows(regressors, contemporaneous) %>% dplyr::arrange(ticker, field, date)
### volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable, date %in% unique(regressors$date)) %>% 
    dplyr::select(field, date, value) %>% dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}

### 1-factor models ####
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(regressors$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility ~ `", names(data)[5L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[5L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(regressors$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  regressor <- combinations[y, "regressor"]; period <- combinations[y, "period"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(date), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor, date >= as.Date(date), date <= as.Date(end)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility ~ `", names(data)[4L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[4L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}

`1-factor models` <- dplyr::bind_rows(years, subperiods)
### 2-factor models ####
variables <- as.data.frame(t(combn(unique(regressors$field), 2L))) %>% setNames(c("regressor 1", "regressor 2"))
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]
  estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`), date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}

`2-factor models` <- dplyr::bind_rows(years, subperiods)
### 4-factor models ####
variables <- as.data.frame(t(combn(unique(regressors$field), 4L))) %>% setNames(c("regressor 1", "regressor 2", "regressor 3", "regressor 4"))
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) 
  dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"], 
                `regressor 3` = variables[x, "regressor 3"], `regressor 4` = variables[x, "regressor 4"])) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "` + `", names(data)[8L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], 
                  `regressor 3` = names(data)[7L], `regressor 4` = names(data)[8L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) 
  dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"], 
                `regressor 3` = variables[x, "regressor 3"], `regressor 4` = variables[x, "regressor 4"])) %>% dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]
  estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`), date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], 
                  `regressor 3` = names(data)[6L], `regressor 4` = names(data)[7L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}

`4-factor models` <- dplyr::bind_rows(years, subperiods)

`contemporaneous results` <- dplyr::bind_rows(`1-factor models`, `2-factor models`, `4-factor models`)

## lagged ####
### regressors ####
Working <- dplyr::filter(CFTC, participant == "commercial", position %in% c("long", "short")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(regime = ifelse(short > long, 1L, 0L), total = long + short) %>% 
  dplyr::select(`active contract ticker`, date, regime, total) %>% dplyr::ungroup()
Working <- dplyr::filter(CFTC, participant == "non-commercial", position %in% c("long", "short")) %>% dplyr::select(-participant) %>%
  dplyr::left_join(Working, by = c("active contract ticker", "date")) %>% dplyr::group_by(`active contract ticker`) %>% 
  tidyr::spread(position, value) %>% dplyr::mutate(`Working's T` = (ifelse(regime == 1L, short, long) / total)) %>%
  dplyr::select(`active contract ticker`, date, `Working's T`) %>% tidyr::gather(field, value, -c(`active contract ticker`, date)) %>%
  dplyr::ungroup()
regressors <- dplyr::bind_rows(dplyr::filter(CFTC, position == "net") %>% dplyr::select(`active contract ticker`, field = participant, date, value), Working) %>%
  dplyr::rename(ticker = `active contract ticker`)
contemporaneous <- dplyr::left_join(dplyr::filter(`commodity aggregate data`@data, field == "FUT_AGGTE_VOL"), fields, by = c("field" = "symbol")) %>% 
  dplyr::select(ticker, field = name, date, value) %>% dplyr::mutate_at(dplyr::vars(field), dplyr::funs(as.character)) %>% dplyr::filter(date %in% unique(regressors$date))
regressors <- dplyr::bind_rows(regressors, contemporaneous) %>% dplyr::arrange(ticker, field, date)
regressors <- dplyr::group_by(regressors, ticker, field) %>% dplyr::mutate(value = dplyr::lag(value, 1L)) %>% dplyr::ungroup() %>%
  dplyr::group_by(ticker) %>% dplyr::mutate(field = paste("lagged", field)) %>% dplyr::ungroup()
### volatilities ####
futures <- tibble::tibble(variable = rep("futures price", 6L), estimator = c("close", "garman.klass", "parkinson", "rogers.satchell", "gk.yz", "yang.zhang"))
basis <- tibble::tibble(variable = rep("basis", 2L), estimator = c("close", "parkinson"))
estimators <- dplyr::bind_rows(futures, basis)
combinations <- lapply(unique(`commodity futures data`$ticker), function(x) dplyr::mutate(estimators, commodity = x)) %>% dplyr::bind_rows() %>% as.data.frame()
volatilities <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "commodity"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  
  data <- dplyr::filter(`commodity futures data`, ticker == !! ticker, variable == !! variable, date %in% unique(regressors$date)) %>% 
    dplyr::select(field, date, value) %>% dplyr::arrange(field, date) %>% tidyr::spread(field, value) %>% dplyr::filter(complete.cases(.))
  if (variable == "basis"){
    data <- switch(estimator, "parkinson" = dplyr::mutate(data, Open = NA, Close = NA) %>% dplyr::select(date, Open, High, Low, Close),
                   "close" = dplyr::select(data, date, Close), data)
  }
  
  dplyr::mutate(data, volatility = TTR::volatility(dplyr::select(data, -date), n = width, calc = estimator, N = width)) %>%
    dplyr::filter(! is.na(volatility)) %>% dplyr::mutate(estimator = !! estimator, ticker = !! ticker, variable = !! variable) %>% 
    dplyr::select(ticker, variable, estimator, date, volatility)
}

### 1-factor models ####
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(regressors$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]; regressor <- combinations[y, "regressor"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility ~ `", names(data)[5L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[5L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(unique(regressors$field), function(x) dplyr::mutate(combinations, regressor = x)) %>% dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  regressor <- combinations[y, "regressor"]; period <- combinations[y, "period"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(date), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field == !! regressor, date >= as.Date(date), date <= as.Date(end)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility ~ `", names(data)[4L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, regressor = names(data)[4L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, regressor, period, year, model)
}

`1-factor models` <- dplyr::bind_rows(years, subperiods)
### 2-factor models ####
variables <- as.data.frame(t(combn(unique(regressors$field), 2L))) %>% setNames(c("regressor 1", "regressor 2"))
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"])) %>% 
  dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]
  estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`), date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, period, year, model)
}

`2-factor models` <- dplyr::bind_rows(years, subperiods)
### 4-factor models ####
variables <- as.data.frame(t(combn(unique(regressors$field), 4L))) %>% setNames(c("regressor 1", "regressor 2", "regressor 3", "regressor 4"))
#### years ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) 
  dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"], 
                `regressor 3` = variables[x, "regressor 3"], `regressor 4` = variables[x, "regressor 4"])) %>% 
  dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
years <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]; estimator <- combinations[y, "estimator"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator) %>% dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`)) %>% tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::mutate(year = lubridate::year(date)) %>% 
    dplyr::select(ticker, date, year, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "` + `", names(data)[8L], "`"))
  dplyr::group_by(data, year) %>% dplyr::do({data <- .; tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA }))}) %>% 
    dplyr::ungroup() %>%
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, 
                  `regressor 1` = names(data)[5L], `regressor 2` = names(data)[6L], `regressor 3` = names(data)[7L], `regressor 4` = names(data)[8L], 
                  period = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}
#### subperiods ####
combinations <- dplyr::distinct(volatilities, ticker, variable, estimator)
combinations <- lapply(1L:nrow(variables), function(x) 
  dplyr::mutate(combinations, `regressor 1` = variables[x, "regressor 1"], `regressor 2` = variables[x, "regressor 2"], 
                `regressor 3` = variables[x, "regressor 3"], `regressor 4` = variables[x, "regressor 4"])) %>% 
  dplyr::bind_rows()
combinations <- lapply(unique(periods$period), function(x) dplyr::mutate(combinations, period = x)) %>% dplyr::bind_rows()
combinations <- dplyr::mutate_all(combinations, dplyr::funs(as.character)) %>% as.data.frame()
subperiods <- foreach(y = 1L:nrow(combinations), .combine = dplyr::bind_rows) %dopar% {
  library(magrittr); library(lubridate)
  
  ticker <- combinations[y, "ticker"]; variable <- combinations[y, "variable"]
  estimator <- combinations[y, "estimator"]; period <- combinations[y, "period"]
  `regressor 1` <- combinations[y, "regressor 1"]; `regressor 2` <- combinations[y, "regressor 2"]
  `regressor 3` <- combinations[y, "regressor 3"]; `regressor 4` <- combinations[y, "regressor 4"]
  
  start <- dplyr::filter(periods, period == !! period, bound == "start") %>% dplyr::select(date) %>% purrr::flatten_chr()
  end <- dplyr::filter(periods, period == !! period, bound == "end") %>% dplyr::select(date) %>% purrr::flatten_chr()
  
  volatility <- dplyr::filter(volatilities, ticker == !! ticker, variable == !! variable, estimator == !! estimator, date >= as.Date(start), date <= as.Date(end)) %>% 
    dplyr::select(date, volatility)
  X <- dplyr::filter(regressors, ticker == !! ticker, field %in% c(`regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`), date >= as.Date(start), date <= as.Date(end)) %>% 
    tidyr::spread(field, value)
  
  data <- dplyr::left_join(volatility, X, by = "date") %>% dplyr::select(ticker, date, dplyr::everything()) %>% dplyr::arrange(ticker, date)
  
  formula <- as.formula(paste0("volatility  ~ `", names(data)[4L], "` + `", names(data)[5L], "` + `", names(data)[6L], "` + `", names(data)[7L], "`"))
  tibble::tibble(model = tryCatch({ list(lm(formula, data = data)) }, error = function(e) { NA })) %>% 
    dplyr::mutate(ticker = !! ticker, variable = !! variable, estimator = !! estimator, 
                  `regressor 1` = names(data)[4L], `regressor 2` = names(data)[5L], `regressor 3` = names(data)[6L], `regressor 4` = names(data)[7L], 
                  period = !! period, year = NA) %>%
    dplyr::select(ticker, variable, estimator, `regressor 1`, `regressor 2`, `regressor 3`, `regressor 4`, period, year, model)
}

`4-factor models` <- dplyr::bind_rows(years, subperiods)

`lagged results` <- dplyr::bind_rows(`1-factor models`, `2-factor models`, `4-factor models`)


`CFTC variables` <- dplyr::bind_rows(`contemporaneous results`, `lagged results`)
regressions <- tibble::tibble(analysis = c("market variables", "CFTC variables"), results = list(`market variables`, `CFTC variables`))

parallel::stopCluster(cluster); gc()
saveRDS(regressions, file = "explore/results/regressions.rds")
