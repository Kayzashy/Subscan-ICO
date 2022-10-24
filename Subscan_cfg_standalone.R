remove(list=ls())

library(httr)
#library(tidyverse)
library(stringr)
library(dplyr)

ch_saft <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\ch_saft.csv") %>% mutate(Csv = "ch_saft.csv") %>% select(Name,Address,Amount,Csv)
ch_saft_2 <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\ch_saft_2.csv") %>% mutate(Csv = "ch_saft_2.csv") %>% select(Name,Address,Amount,Csv)
second_batch_cnf <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\2nd_batch_cnf_linear_vesting.csv") %>% mutate(Csv = "2nd_batch_cnf_linear_vesting.csv") %>% select(Name,Address,Amount,Csv)
cnf_direct_transfers <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\cnf_direct_transfers.csv") %>% mutate(Csv = "cnf_direct_transfers.csv") %>% select(Name,Address,Amount,Csv)
cnf_vesting_3 <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\cnf_vesting_3.csv") %>% mutate(Csv = "cnf_vesting_3.csv") %>% select(Name,Address,Amount,Csv)
team_transfer_anida <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\team_transfer_anida.csv") %>% mutate(Csv = "team_transfer_anida.csv") %>% select(Name,Address,Amount,Csv)
tinlake_sale_in <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\tinlake_sale_in.csv") %>% mutate(Csv = "tinlake_sale_in.csv") %>% select(Name,Address,Amount,Csv)

# second_batch_cnf_out <- read.csv("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Input\\2nd_batch_cnf_linear_vesting_out.csv")
# a <- second_batch_cnf_out %>% select(address,amount) %>% rename("Address" = address,"Amount" = amount)
# b <- second_batch_cnf %>% left_join(a,by = "Address")

rbind_all <- rbind(ch_saft,
                   ch_saft_2,
                   second_batch_cnf,
                   cnf_direct_transfers,
                   cnf_vesting_3,
                   team_transfer_anida,
                   tinlake_sale_in) %>% mutate(Name = trimws(Name), Address = trimws(Address))

# add_2 <- rbind_all %>% group_by(Name,Address) %>% summarise(N = n()) %>% arrange(-N)
# 
# rbind_all %>% filter(Address %in% add_2) %>% arrange(Address)

headers = c(
  'Content-Type' = 'application/json',
  'X-API-Key: dd5f2fa6f0184a309a97fd3ba41d3af0'
)


xpage = 0
addresses = trimws(rbind_all$Address)

#addresses = addresses[1:10]

all_data <- data.frame()

now <- Sys.time()

for (address in addresses) {#address= '4gLJTxk7gBCBmSYfywUQovpUzgTEPhmKRBP9XkTUZnCbEWRD' addresses[2]

body_fake <- paste0("{\n    \"row\": ",1,",\n    \"page\": ",paste(xpage),",\n    \"address\":\"",trimws(address),"\"\n    \n}")

res_fake <- VERB("POST", url = "https://centrifuge-standalone-history.api.subscan.io/api/scan/transfers",body = body_fake,  add_headers(headers))
  
a_fake <- content(res_fake, 'text')

x <- str_locate(a_fake,"count")
y <- str_locate(a_fake,"transfers")
xrow <- as.numeric(substring(a_fake,x[2]+3,y[1]-3))  
#xrow
#xrow=100
Sys.sleep(0.2)

if (xrow > 100) {

pages <- rank(seq(from = 0, to = xrow, by = 100))-1

if ((xrow - pages[length(pages)]*100)==0) {
 pages = pages[-length(pages)]
}

pages

for (p in pages){#p = pages[3]
body <- paste0("{\n    \"row\": ",paste(100),",\n    \"page\": ",paste(p),",\n    \"address\":\"",trimws(address),"\"\n    \n}")

res <- VERB("POST", url = "https://centrifuge-standalone-history.api.subscan.io/api/scan/transfers",body = body,  add_headers(headers))

a <- content(res, 'text')

success_raw <- cbind(str_locate_all(a,"success\"")[[1]][,2] + 2, str_locate_all(a,"hash\"")[[1]][,1]-3)
success <- as.logical(substring(a,first=success_raw[,1],last=success_raw[,2]))

block_time_raw_1 <- cbind(str_locate_all(a,"block_timestamp\"")[[1]][,2]+2,str_locate_all(a,"module\"")[[1]][,1]-3)
block_time_raw_2 <- substring(a,first=block_time_raw_1[,1],last=block_time_raw_1[,2])
block_time <- as.Date(as.POSIXct(as.numeric(block_time_raw_2), origin="1970-01-01"))

block_num_raw <- cbind(str_locate_all(a,"block_num\"")[[1]][,2]+2,str_locate_all(a,"block_timestamp\"")[[1]][,1]-3)
block_num <- substring(a,first=block_num_raw[,1],last=block_num_raw[,2])

hash_raw <- cbind(str_locate_all(a,"hash\"")[[1]][,2]+3,str_locate_all(a,"block_num\"")[[1]][,1]-4)
hash <- substring(a,first=hash_raw[,1],last=hash_raw[,2])

from_raw <- cbind(str_locate_all(a,"from\"")[[1]][,2]+3,str_locate_all(a,"to\":")[[1]][,1]-4)
from <- substring(a,first=from_raw[,1],last=from_raw[,2])

to_raw <- cbind(str_locate_all(a,"to\":")[[1]][,2]+2,str_locate_all(a,"extrinsic_index\"")[[1]][,1]-4)
to <- substring(a,first=to_raw[,1],last=to_raw[,2])

value_raw <- cbind(str_locate_all(a,"amount\"")[[1]][,2]+3,str_locate_all(a,"amount_v2\"")[[1]][,1]-4)
value <- as.numeric(substring(a,first=value_raw[,1],last=value_raw[,2]))

df_raw <- data.frame(Address = address,Block_time = block_time ,Block_num = block_num, Hash = hash, From = from, To = to, Value = value)
df <- df_raw %>% mutate(Value = case_when(address == to ~ Value,TRUE ~ - Value),
                        transfer = case_when(address == to ~ "trasfer_in",TRUE ~ "transfer_out"))

f_success <-  which(!success)

if (length(f_success) > 0) {
  df <- df[-f_success,]
} 

all_data <- rbind(all_data,df)

Sys.sleep(0.3)

}
  
} else {
  
body <- paste0("{\n    \"row\": ",paste(xrow),",\n    \"page\": ",paste(xpage),",\n    \"address\":\"",trimws(address),"\"\n    \n}")

res <- VERB("POST", url = "https://centrifuge-standalone-history.api.subscan.io/api/scan/transfers",body = body,  add_headers(headers))

a <- content(res, 'text')
}

Sys.sleep(0.3)

success_raw <- cbind(str_locate_all(a,"success\"")[[1]][,2] + 2, str_locate_all(a,"hash\"")[[1]][,1]-3)
success <- as.logical(substring(a,first=success_raw[,1],last=success_raw[,2]))

block_time_raw_1 <- cbind(str_locate_all(a,"block_timestamp\"")[[1]][,2]+2,str_locate_all(a,"module\"")[[1]][,1]-3)
block_time_raw_2 <- substring(a,first=block_time_raw_1[,1],last=block_time_raw_1[,2])
block_time <- as.Date(as.POSIXct(as.numeric(block_time_raw_2), origin="1970-01-01"))

block_num_raw <- cbind(str_locate_all(a,"block_num\"")[[1]][,2]+2,str_locate_all(a,"block_timestamp\"")[[1]][,1]-3)
block_num <- substring(a,first=block_num_raw[,1],last=block_num_raw[,2])

hash_raw <- cbind(str_locate_all(a,"hash\"")[[1]][,2]+3,str_locate_all(a,"block_num\"")[[1]][,1]-4)
hash <- substring(a,first=hash_raw[,1],last=hash_raw[,2])

from_raw <- cbind(str_locate_all(a,"from\"")[[1]][,2]+3,str_locate_all(a,"to\":")[[1]][,1]-4)
from <- substring(a,first=from_raw[,1],last=from_raw[,2])

to_raw <- cbind(str_locate_all(a,"to\":")[[1]][,2]+2,str_locate_all(a,"extrinsic_index\"")[[1]][,1]-4)
to <- substring(a,first=to_raw[,1],last=to_raw[,2])

value_raw <- cbind(str_locate_all(a,"amount\"")[[1]][,2]+3,str_locate_all(a,"amount_v2\"")[[1]][,1]-4)
value <- as.numeric(substring(a,first=value_raw[,1],last=value_raw[,2]))

df_raw <- data.frame(Address = address,Block_time = block_time ,Block_num = block_num, Hash = hash, From = from, To = to, Value = value)
df <- df_raw %>% mutate(Value = case_when(address == to ~ Value,TRUE ~ - Value),
                        transfer = case_when(address == to ~ "trasfer_in",TRUE ~ "transfer_out"))

f_success <-  which(!success)

if (length(f_success) > 0) {
  df <- df[-f_success,]
} 

all_data <- rbind(all_data,df)
Sys.sleep(0.3)
}

time_passed <- Sys.time() - now
time_passed

View(all_data)

df <- rbind_all %>% select(Name,Amount,Address,Csv) %>% right_join(all_data, by = "Address")

df %>% filter(Address == '4esVzAWrFiALQr8bDpboTdH8mAW5TuZWqMPAU3QdwCy76RFe')

length(rbind_all$Address)
length(unique(all_data$Address))

write.csv(df,"C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg_standalone.csv")
saveRDS(df,"C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg_standalone.RData")




