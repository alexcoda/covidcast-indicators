## Functions for performing the aggregations in an efficient way.

#' Produce all desired aggregations.
#'
#' Writes the outputs directly to CSVs in the directory specified by `params`.
#' Produces output using all available data between `params$start_date` and
#' `params$end_date`, inclusive.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#' @param params Named list of configuration parameters.
#'
#' @return none
#'
#' @import data.table
#' @importFrom dplyr full_join %>% select all_of
#' @importFrom purrr reduce
#'
#' @export
produce_aggregates <- function(df, aggregations, cw_list, params) {
  msg_plain("Producing contingency aggregates...")
  ## For the date range lookups we do on df, use a data.table key. This puts the
  ## table in sorted order so data.table can use a binary search to find
  ## matching dates, rather than a linear scan, and is important for very large
  ## input files.
  df <- as.data.table(df) %>% filter(!is.na(weight))
  setkeyv(df, "start_dt")

  # Keep only obs in desired date range.
  df <- df[start_dt >= params$start_time & start_dt <= params$end_time]

  output <- post_process_aggs(df, aggregations, cw_list)
  df <- output[[1]]
  aggregations <- output[[2]]

  ## Keep only columns used in indicators, plus supporting columns.
  group_vars <- unique( unlist(aggregations$group_by) )
  df <- select(df,
               all_of(unique(aggregations$metric)),
               all_of(unique(aggregations$var_weight)),
               all_of( group_vars[group_vars != "geo_id"] ),
               .data$zip5,
               .data$start_dt
  )

  agg_groups <- unique(aggregations[c("group_by", "geo_level")])

  # For each unique combination of group_vars and geo level, run aggregation process once
  # and calculate all desired aggregations on the grouping. Rename columns. Save
  # to individual files
  for (group_ind in seq_along(agg_groups$group_by)) {

    agg_group <- agg_groups$group_by[group_ind][[1]]
    geo_level <- agg_groups$geo_level[group_ind]
    geo_crosswalk <- cw_list[[geo_level]]

    # Subset aggregations to keep only those grouping by the current agg_group
    # and with the current geo_level. `setequal` ignores differences in
    # ordering and only looks at unique elements.
    these_aggs <- aggregations[mapply(aggregations$group_by,
                                      FUN=function(x) {setequal(x, agg_group)
                                      }) & aggregations$geo_level == geo_level, ]

    df_out <- summarize_aggs(df, geo_crosswalk, these_aggs, params)

    if ( nrow(df_out) != 0 ) {
      # To drop other response columns ("val", "sample_size", "se",
      # "effective_sample_size", "represented"), add here.
      drop_vars <- c("effective_sample_size")
      drop_cols <- which(Reduce("|", lapply(
        drop_vars, function(prefix) {
          startsWith(names(df_out), prefix) 
        }))
      )
      df_out <- df_out %>% select(-drop_cols)
      
      write_contingency_tables(df_out, params, geo_level, agg_group)
    }
  }
}

#' Process aggregations to make formatting more consistent
#'
#' Parse grouping variables for geolevel, and save to new column for easy
#' access. If none, assume national. If `metric` is a multiple choice item,
#' include it in list of grouping variables so levels are included in output
#' CSV. Alphabetize grouping variables; columns are saved to output CSV in this
#' order.
#'
#' @param df Data frame of individual response data.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables to aggregate
#'   over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param cw_list Named list of geographic crosswalks, each of which maps a zip5
#'   to a geographic level such as county or state. Aggregates will be produced
#'   for each geographic level.
#'
#' @return list of data frame of individual response data and user-set data
#' frame of desired aggregations
#'
#' @export
post_process_aggs <- function(df, aggregations, cw_list) {
  aggregations$geo_level <- NA
  for (agg_ind in seq_along(aggregations$group_by)) {
    # Find geo_level, if any, present in provided group_by vars
    geo_level <- intersect(aggregations$group_by[agg_ind][[1]], names(cw_list))

    # Add implied geo_level to each group_by. Order alphabetically. Replace
    # geo_level with generic "geo_id" var. Remove duplicate grouping vars.
    if (length(geo_level) > 1) {
      stop('more than one geo type provided for a single aggregation')

    } else if (length(geo_level) == 0) {
      # Presume national grouping
      geo_level <- "nation"
      aggregations$group_by[agg_ind][[1]] <-
        sort(append(aggregations$group_by[agg_ind][[1]], "geo_id"))

    } else {
      aggregations$group_by[agg_ind][[1]][
        aggregations$group_by[agg_ind][[1]] == geo_level] <- "geo_id"
      aggregations$group_by[agg_ind][[1]] <-
        sort(unique(aggregations$group_by[agg_ind][[1]]))
    }

    aggregations$geo_level[agg_ind] <- geo_level
  }

  # Remove aggregations using unavailable variables.
  group_vars <- unique( unlist(aggregations$group_by) )
  metric_cols <- unique(aggregations$metric)
  
  cols_check_available <- unique(c(group_vars[group_vars != "geo_id"], metric_cols))
  available <- cols_check_available %in% names(df)
  cols_not_available <- cols_check_available[ !available ]
  for (col_var in cols_not_available) {
    # Remove from aggregations
    aggregations <- aggregations[aggregations$metric != col_var &
                                   !mapply(aggregations$group_by,
                                           FUN=function(x) {col_var %in% x}), ]
    msg_plain(paste0(
        col_var, " is not defined. Removing all aggregations that use it. ",
        nrow(aggregations), " remaining")
    )
  }

  return(list(df, aggregations))
}

#' Perform calculations across all groupby levels for all aggregations.
#'
#' @param df a data frame of survey responses
#' @param crosswalk_data An aggregation, such as zip => county or zip => state,
#'   as a data frame with a "zip5" column to join against.
#' @param aggregations Data frame with columns `name`, `var_weight`, `metric`,
#'   `group_by`, `compute_fn`, `post_fn`. Each row represents one aggregate
#'   to report. `name` is the aggregate's base column name; `var_weight` is the
#'   column to use for its weights; `metric` is the column of `df` containing the
#'   response value. `group_by` is a list of variables used to perform the
#'   aggregations over. `compute_fn` is the function that computes
#'   the aggregate response given many rows of data. `post_fn` is applied to the
#'   aggregate data after megacounty aggregation, and can perform any final
#'   calculations necessary.
#' @param params a named list with entries "s_weight", "s_mix_coef",
#'   "num_filter"
#'   
#' @return named list where each element is the val, se, n, n effective, or
#'   represented population for a given aggregation
#'
#' @importFrom dplyr inner_join bind_rows
#' @importFrom parallel mclapply
#' @importFrom stats complete.cases
#'
#' @export
summarize_aggs <- function(df, crosswalk_data, aggregations, params) {
  if (nrow(df) == 0) {
    return( data.frame() )
  }
  
  ## We do batches of just one set of groupby vars at a time, since we have
  ## to select rows based on this.
  assert( length(unique(aggregations$group_by)) == 1 )
  
  if ( length(unique(aggregations$name)) < nrow(aggregations) ) {
    stop("all aggregations using the same set of grouping variables must have unique names")
  }
  
  ## dplyr complains about joining a data.table, saying it is likely to be
  ## inefficient; profiling shows the cost to be negligible, so shut it up
  df <- suppressWarnings(inner_join(df, crosswalk_data, by = "zip5"))
  
  group_vars <- aggregations$group_by[[1]]
  
  if ( !all(group_vars %in% names(df)) ) {
    msg_plain(
      sprintf(
        "not all of grouping columns %s available in data; skipping aggregation",
        paste(group_vars, collapse=", ")
      ))
    return( data.frame( ))
  }
  
  # TODO: necessary? do post-filtering anyway for safety.
  ## Find all unique groups and associated frequencies, saved in column `Freq`.
  unique_groups_counts <- as.data.frame(
    table(df[, group_vars, with=FALSE], exclude=NULL, dnn=group_vars), 
    stringsAsFactors=FALSE
  )
  
  # Drop groups with less than threshold sample size.
  unique_groups_counts <- filter(unique_groups_counts, .data$Freq >= params$num_filter)
  if ( nrow(unique_groups_counts) == 0 ) {
    return( data.frame() )
  }
  
  # TODO: necessary?
  ## Set an index on the groupby var columns so that the groupby step can be
  ## faster; data.table stores the sort order of the column and
  ## uses a binary search to find matching values, rather than a linear scan.
  setindexv(df, group_vars)
  
  df_out <- df[, summarize_aggregations_group(.SD, aggregations, params), 
               by=group_vars, 
               .SDcols=c("weight", "weight_in_location", unique(aggregations$metric))]
  
  return( df_out )
}

#' Produce estimates for all indicators in a specific target group.
#'
#' @param group_df Data frame containing weight fields and all metric fields.
#' @param aggregations Aggregations to report. See `produce_aggregates()`.
#' @param params Named list of configuration options.
#'
#' @return named list where each element is the val, se, n, n effective, or
#'   represented population for a given aggregation
#'
#' @importFrom dplyr %>%
summarize_aggregations_group <- function(group_df, aggregations, params) {
  ## Prepare outputs.
  df_out <- list()
  
  for (row in seq_along(aggregations$id)) {
    agg_name <- aggregations$name[row]
    metric <- aggregations$metric[row]
    var_weight <- aggregations$var_weight[row]
    compute_fn <- aggregations$compute_fn[[row]]
    post_fn <- aggregations$post_fn[[row]]
    
    agg_df <- group_df[!is.na(group_df[[var_weight]]) & !is.na(group_df[[metric]]), ]
    
    if (nrow(agg_df) > 0) {
      s_mix_coef <- params$s_mix_coef
      mixing <- mix_weights(agg_df[[var_weight]] * agg_df$weight_in_location,
                            s_mix_coef, params$s_weight)
      
      sample_size <- sum(agg_df$weight_in_location)
      total_represented <- sum(agg_df[[var_weight]] * agg_df$weight_in_location)
      
      ## TODO: See issue #764
      new_row <- compute_fn(
        response = agg_df[[metric]],
        weight = if (aggregations$skip_mixing[row]) { mixing$normalized_preweights } else { mixing$weights },
        sample_size = sample_size,
        total_represented = total_represented)
      
      new_row <- post_fn(data.frame(new_row))
      new_row <- apply_privacy_censoring(new_row, params)

      # Defaults
      df_out[[paste("val", agg_name, sep="_")]] <- NA
      df_out[[paste("se", agg_name, sep="_")]] <- NA
      df_out[[paste("sample_size", agg_name, sep="_")]] <- NA
      df_out[[paste("effective_sample_size", agg_name, sep="_")]] <- NA
      df_out[[paste("represented", agg_name, sep="_")]] <- NA
            
      # Keep only aggregations where the main value, `val`, and sample size are present.
      if ( nrow(new_row) > 0 ) {
        new_row <- as.list(new_row)
        if ( !is.na(new_row$val) && !is.na(new_row$sample_size) ) {
          df_out[[paste("val", agg_name, sep="_")]] <- new_row$val
          df_out[[paste("se", agg_name, sep="_")]] <- new_row$se
          df_out[[paste("sample_size", agg_name, sep="_")]] <- sample_size
          df_out[[paste("effective_sample_size", agg_name, sep="_")]] <- new_row$effective_sample_size
          df_out[[paste("represented", agg_name, sep="_")]] <- new_row$represented
        }
      }
    }
  }
  
  return(df_out)
}
