% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/variables.R
\name{code_activities}
\alias{code_activities}
\title{Activities outside the home}
\usage{
code_activities(input_data, wave)
}
\arguments{
\item{input_data}{input data frame of raw survey data}

\item{wave}{integer indicating survey version}
}
\value{
data frame augmented with `a_work_outside_home_1d`, `a_shop_1d`,
  `a_restaurant_1d`, `a_spent_time_1d`, `a_large_event_1d`,
  `a_public_transit_1d`
}
\description{
Activities outside the home
}
