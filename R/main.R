#' Create a connection to a PostgreSQL database
#'
#' This function creates a connection to a PostgreSQL database and returns it as
#' a list object with three functions - \code{query}, \code{insert}, and
#' \code{disconnect}. These functions can be called to execute SQL queries,
#' insert data into tables and disconnect from the database respectively.
#'
#' @param host A character string indicating the host name or IP address of the
#' server on which the database is running. Default is “localhost".
#' @param port A character string indicating the port number on which to connect
#' to the database. Default is “5432".
#' @param dbname A character string indicating the name of the PostgreSQL
#' database to connect to.
#' @param user A character string indicating the user name to use when
#' connecting to the database.
#' @param password A character string indicating the password to use when
#' connecting to the database.
#'
#' @return A list object containing the following elements:
#' \itemize{
#' \item \code{query}: A function used to execute SQL queries.
#' \item \code{insert}: A function used to insert data into tables.
#' \item \code{disconnect}: A function used to disconnect from the database.
#'}
#'
#' @import RPostgreSQL
#'
#' @export
connectDb <- function(
    host="localhost", port="5432",
    dbname="platform", user="postgres",
    password="password"
) {
  db <- list()

  con <- dbConnect(
    PostgreSQL(), host=host, port=port,
    dbname=dbname, user=user, password=password
  )

  db$query <- function(statement) {
    dbGetQuery(con, statement)
  }

  db$insert <- function(data, table, append=TRUE, row.names=FALSE) {
    dbWriteTable(con, name=table, data, append, row.names)
  }

  db$disconnect <- function() {
    dbDisconnect(con)
  }

  db
}

#' Save query results as RDS file
#'
#' This function saves query results as an RDS file.
#' The name of the RDS file is generated using the CRC32 hash algorithm on the
#' query statement, unless a name is provided.
#' The file is saved in the specified directory unless the directory does not
#' exist or the file already exists, in which case the function will print out
#' an appropriate message.
#'
#' @param data A list object returned by the \code{\link{query}} function.
#' @param name An optional character string used as the name of the output file.
#' If not provided, the file name will be generated using the CRC32 hash
#' algorithm on the query statement.
#' @param dir An optional character string indicating the directory in which to
#' save the file. Default is “rds”.
#' @param force A logical value indicating whether to overwrite an existing file.
#' Default is FALSE.
#'
#' @return This function has no explicit return value.
#' It saves the RDS file to the specified directory.
#'
#' @examples
#' \dontrun{
#' # Save query results to an RDS file
#' vaxr::save(my_query, "my_query_results")
#' }
#'
#' @importFrom digest digest
#'
#' @export
save <- function(
  data,
  name=NULL, dir="~/shared/rds", force=FALSE
) {
  if (is.null(name)) {
    name <- digest(data$query, algo = "crc32")
  }
  file_path <- paste0(dir, '/', name, '.rds')

  if (force || !file.exists(file_path)) {
    saveRDS(data, file_path)
    print(paste(file_path, "saved!"))
  } else {
    print(paste(file_path, "already exists!"))
  }
}

#' Execute a SQL query using PostgreSQL
#'
#' This function executes a supplied SQL query using PostgreSQL and
#' creates a list containing the query result, a sample of the result,
#' the query statement, the query description, and the execution time.
#'
#' @param statement A character string representing a SQL query statement.
#' @param desc An optional character string describing the query.
#' Default is an empty string.
#' @param sample_size An optional integer indicating the number of rows to
#' include in the sample. Default is 5.
#' @param db An optional PostgreSQL database connection object.
#' If not provided, the function will connect to the default database.
#'
#' @return A list containing following elements:
#' \itemize{
#' \item \code{data}: The query result as a data frame.
#' \item \code{sample}: A sample of the query result.
#' \item \code{query}: The original query statement.
#' \item \code{desc}: A description of the query.
#' \item \code{time}: A vector with two elements indicating the start and
#' end time of query execution.
#' }
#'
#' @examples
#' \dontrun{
#' # Execute a SQL query
#' # my_query <- vaxr::query("SELECT * FROM mytable", "all_records”)
#' }
#'
#' @importFrom utils head
#'
#' @export
query <- function(
  statement,
  desc="", sample_size=5,
  db=NULL
) {
  now <- Sys.time()

  if (is.null(db)) {
    db <- connectDb()
  }

  data <- db$query(statement)
  db$disconnect()

  # -----
  out <- list()
  out$data <- data
  out$sample <- head(data, sample_size)
  out$query <- statement
  out$desc <- desc
  out$time <- c(now, Sys.time())  # execution time
  out
}


#' Insert data into a PostgreSQL database table
#'
#' This function inserts data into a PostgreSQL database table.
#' The function takes a data frame, table name, and optional database connection
#' object as input. If no database connection object is provided, the function
#' will create a new connection object. The function also accepts optional
#' arguments for appending data to the existing table and preserving row names.
#'
#' @param data A data frame containing the data to be inserted into the database.
#' @param table A character string indicating the name of the PostgreSQL database
#' table to insert data into.
#' @param db An optional PostgreSQL database connection object. If not provided,
#' the function will connect to the default database.
#' @param append A logical value indicating whether to append the data to an
#' existing table or create a new table. Default is TRUE.
#' @param row.names A logical value indicating whether to preserve row names.
#' Default is FALSE.
#'
#' @return This function has no explicit return value.
#'
#' @examples
#' \dontrun{
#' # Insert data into a table
#' vaxr::upload(my_data, "my_table")
#' }
#'
#' @export
upload <- function(
  data, table,
  db=NULL, append=TRUE, row.names=FALSE
) {
  if (is.null(db)) {
    db <- connectDb()
  }

  db$insert(data, table, append, row.names)
  db$disconnect()
}
