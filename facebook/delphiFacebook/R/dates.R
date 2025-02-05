# Time zone to use throughout package.
tz_to <- "America/Los_Angeles"
wave6_mod_date <- lubridate::ymd("2021-01-06", tz=tz_to)

#' Get the date of the first day of the previous month.
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date
#' 
#' @export
start_of_prev_full_month <- function(date) {
  return(floor_date(date, "month") - months(1))
}

#' Get the date of the last day of the previous month.
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate ceiling_date days
#' 
#' @export
end_of_prev_full_month <- function(date) {
  first_day_of_next_month <- ceiling_date(date, "month")
  
  if (first_day_of_next_month != date) {
    first_day_of_next_month <- floor_date(date, "month")
  }
  
  return(first_day_of_next_month - days(1))
}

#' Get the date range specifying the previous month.
#'
#' @param date Date of interest
#' 
#' @return list of two Dates
#' 
#' @export
get_range_prev_full_month <- function(date = Sys.Date()) {
  eom <- end_of_prev_full_month(date)
  
  if (eom == date) {
    som <- start_of_prev_full_month(date + months(1))
  } else {
    som <- start_of_prev_full_month(date)
  }
  
  return(list(som, eom))
}

#' Get the date of the first day of the previous epiweek.
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate floor_date weeks
#' 
#' @export
start_of_prev_full_week <- function(date) {
  return(floor_epiweek(date) - weeks(1))
}

#' Get the date of the last day of the previous epiweek.
#'
#' @param date Date of interest
#' 
#' @return Date
#' 
#' @importFrom lubridate days
#' 
#' @export
end_of_prev_full_week <- function(date) {
  first_day_of_next_week <- ceiling_epiweek(date)
  
  if (first_day_of_next_week != date) {
    first_day_of_next_week <- floor_epiweek(date)
  }
  
  return(first_day_of_next_week - days(1))
}

#' Get the date range specifying the previous week.
#'
#' @param date Date of interest
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate weeks
#' 
#' @export
get_range_prev_full_week <- function(date = Sys.Date()) {
  eow <- end_of_prev_full_week(date)
  
  if (eow == date) {
    sow <- start_of_prev_full_week(date + weeks(1))
  } else {
    sow <- start_of_prev_full_week(date)
  }
  
  return(list(sow, eow))
}

#' Get the date range specifying the previous full time period.
#'
#' @param date Date of interest
#' @param weekly_or_monthly_flag string "week" or "month" indicating desired
#' time period to aggregate over
#' 
#' @return list of two Dates
#' 
#' @importFrom lubridate ymd_hms as_date
#' 
#' @export
get_range_prev_full_period <- function(date = Sys.Date(), weekly_or_monthly_flag = c("month", "week")) {
  weekly_or_monthly_flag <- match.arg(weekly_or_monthly_flag)

  if (weekly_or_monthly_flag == "month") {
    # Get start and end of previous full month.
    date_period_range = get_range_prev_full_month(date)
  } else if (weekly_or_monthly_flag == "week") {
    # Get start and end of previous full epiweek.
    date_period_range = get_range_prev_full_week(date)
  }
  
  date_period_range[[1]] =  ymd_hms(
    sprintf("%s 00:00:00", as_date(date_period_range[[1]])), tz = tz_to
  )
  date_period_range[[2]] =  ymd_hms(
    sprintf("%s 23:59:59", as_date(date_period_range[[2]])), tz = tz_to
  )
  
  return(date_period_range)
}

#' Get date of the first day of the epiweek `x` falls in.
#' 
#' @param date date
#' 
#' @return date
#' 
#' @importFrom lubridate epiweek days
#' 
#' @export
floor_epiweek <- function(date) { 
  for (prior_days in 1:7) {
    if (epiweek(date) != epiweek(date - days(prior_days))) { break }
  }
  return(date - days(prior_days - 1))
}

#' Get date of the first day of the epiweek following the epiweek `x` falls in.
#' 
#' @param date date
#' 
#' @return date
#' 
#' @importFrom lubridate days
#' 
#' @export
ceiling_epiweek <- function(date) {
  return(floor_epiweek(date + days(7)))
}
