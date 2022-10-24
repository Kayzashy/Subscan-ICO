remove(list=ls())

library(dplyr)


t_cfg <- readRDS("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg.RData")
t_cfg_s <- readRDS("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg_standalone.RData")

t_cfg_step2 <- readRDS("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg_step2.RData") %>% distinct()
t_cfg_s_step2 <- readRDS("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\transfers_cfg_standalone_step2.RData") %>% distinct()

transfers_raw <- rbind(t_cfg,t_cfg_s) %>% arrange(Csv,Address,Name,Block_time) %>% distinct()
transfer_raw_step2 <- rbind(t_cfg_step2,t_cfg_s_step2) %>% arrange(Address,Block_time)

s_cfg_s <- readRDS("C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\staking_cfg_standalone.RData")


transfers_in <- transfers_raw %>% filter(transfer == "trasfer_in") %>%
  mutate(Address = trimws(Address)) %>% 
  select(-Amount) %>%
  distinct() %>% 
  group_by(Name,Address) %>% 
  summarise(Transfers_in = sum(Value))

transfers_out <- transfers_raw %>% filter(transfer == "transfer_out") %>%
  mutate(Address = trimws(Address)) %>% 
  select(-Amount) %>%
  distinct() %>% 
  group_by(Name,Address) %>% 
  summarise(Transfers_out = sum(Value))


private_deals <- transfers_raw %>% mutate(Address = trimws(Address)) %>% 
  select(Name,Address,Amount) %>% 
  distinct() %>% 
  group_by(Name,Address) %>%
  summarise(Private_deal = sum(Amount)/10^18)

staking_rewards <- s_cfg_s %>% mutate(Address = trimws(Address)) %>%
  select(Name,Address,Value) %>%
  distinct() %>%
  group_by(Name,Address) %>%
  summarise(Staking_rewards = sum(Value))

pd_balance <- private_deals %>% 
  left_join(staking_rewards, by = c("Name","Address")) %>%
  left_join(transfers_in, by = c("Name","Address")) %>%
  left_join(transfers_out, by = c("Name","Address")) %>%
  arrange(Transfers_out) %>%
  mutate(Actual_balance = Transfers_in + Transfers_out + Staking_rewards) %>%
  mutate(Diff = Private_deal - Actual_balance) %>% arrange(-Diff)

ch_saft_names <- transfers_raw %>% filter(Csv == 'ch_saft.csv') %>% pull(Name)



a <- pd_balance %>% filter(Name %in% unique(ch_saft_names)) %>% arrange(Name,Address)

b <- transfers_raw %>% filter(Name == 'FinTech Collective DeFi Fund I')

###
destinations <- data.frame(To = c('4brTc3oXsaQb8Z2sKqz4FN5NJtCPAENX6quZaoZrfxtKWqd7',
                                  '4eS7ZryvxRbnCtYP1sJMAwKwVKUujqBCJw2SgGgXjVWAHvs7',
                                  '4dCsyMo3ehGN5ePH2KpwZqStfDTBYaVewkWLtTXbT7wFKXjr',
                                  '4beFA2VcN3FKZ68RtyDLzBTrJYVWRzjYmmc5x5QHvba2q8xt',
                                  "4dJdUGMwKLFmQqHD1zyjpeHUgKWN6meZCUURw1KSmCaefJgg"),
                           Destination = c('Cex',
                                           'Cex',
                                           "Cex",
                                           "Cex",
                                           "Cex"))


transfers_raw %>% filter(From %in% destinations$To)

### Removing all the transfers from the same person

multi_addresses_names <- transfers_raw %>% 
  distinct(Name,Address) %>% 
  group_by(Name) %>%  
  summarise(N = n()) %>% 
  arrange(-N) %>% 
  filter(N>1) %>% 
  pull(Name)

multi_addresses <- list()

for (i in multi_addresses_names) {#i = multi_addresses_names[1]
  a <- transfers_raw %>% filter(Name == i) %>% distinct(Address) %>% pull()
  #b <- c(i,a)
  multi_addresses[[i]] = a
}

hashes <- c()

for (i in multi_addresses_names){#i = multi_addresses_names[1]
  
  a <- multi_addresses[[i]]  
  b <- transfers_raw %>% filter(Name == i) %>% select("From","To") 
  
  d <- transfers_raw %>% filter(Name == i)  
  
  e <- d[which(rowSums(t(apply(b, 1,'%in%', multi_addresses[[i]]))) > 1),] %>% pull(Hash)
  
  f <- paste0("",e,"")
  
  hashes <- c(hashes,f)
  
}  

hashes_2 <- hashes[hashes != ""]

transfers <- transfers_raw %>% filter(!(Hash %in% hashes_2))

top_n <- transfers %>% filter(transfer == 'transfer_out') %>% group_by(To) %>% summarise(N = n()) %>% arrange(-N)

top_value <- transfers %>% filter(transfer == 'transfer_out') %>% group_by(To) %>% summarise(Value = -sum(Value)) %>% arrange(-Value)

top_n_addresses <- as.character(paste0("",top_n$To,""))

df_top <- data.frame(To = c(), n = c())

for (i in top_n_addresses){
  a <- transfers %>% filter(To == i, transfer == 'transfer_out')
  
  a <- data.frame(To=i,n=length(unique(a$From))) 
  df_top <- rbind(df_top,a)
}

df_top %>% arrange(-n) 

### transfer out


destinations <- data.frame(To = c("4dpEcgqFp8UL6eA3b7hhtdj7qftHRZE7g1uadHyuw1WSNSgH",
                                  "4cQ3MSd7cBrvhGtAwYUo2DxbX1bZooMjrWTKCjNfwURcfQd5",
                                  '4dpEcgqFor2TJw9uWSjx2JpjkNmTic2UjJAK1j9fRtcTUoRu'),
                           Destination = c("modlchnbrdge",
                                           'Tinlake - 1754 Factory, LLC (Bling Series 1)',
                                           "Ethereum Bridge"))



transfers_destination_time <- transfers_raw %>% left_join(destinations,by = "To") %>% 
  filter(transfer == 'transfer_out',Csv %in% c("ch_saft.csv","ch_saft_2.csv"),!is.na(Destination)) %>% 
  group_by(Name,Address,Block_time,From,To,Destination) %>% summarise(Value = sum(Value)) %>% arrange(Name,Address,Block_time)

transfers_destination <- transfers_destination_time %>% group_by(Name,From,To,Destination) %>% 
  summarise(Value = sum(Value))

### transfer in

destinations_1 <- data.frame(From = c("4dpEcgqFp8UL6eA3b7hhtdj7qftHRZE7g1uadHyuw1WSNSgH",
                                  "4cQ3MSd7cBrvhGtAwYUo2DxbX1bZooMjrWTKCjNfwURcfQd5",
                                  '4dpEcgqFor2TJw9uWSjx2JpjkNmTic2UjJAK1j9fRtcTUoRu'),
                           From_name = c("modlchnbrdge",
                                           'Tinlake - 1754 Factory, LLC (Bling Series 1)',
                                           "Ethereum Bridge"))

destinations_2 <- data.frame(From = c('4brTc3oXsaQb8Z2sKqz4FN5NJtCPAENX6quZaoZrfxtKWqd7',
                                    '4eS7ZryvxRbnCtYP1sJMAwKwVKUujqBCJw2SgGgXjVWAHvs7',
                                    '4dCsyMo3ehGN5ePH2KpwZqStfDTBYaVewkWLtTXbT7wFKXjr',
                                    '4beFA2VcN3FKZ68RtyDLzBTrJYVWRzjYmmc5x5QHvba2q8xt',
                                    "4dJdUGMwKLFmQqHD1zyjpeHUgKWN6meZCUURw1KSmCaefJgg"),
                           From_name = c('Cex',
                                         'Cex',
                                         "Cex",
                                         "Cex",
                                         "Cex"))

destinations <- rbind(destinations_1,destinations_2)

transfers_destination_time_in <- transfers %>% left_join(destinations,by = "From") %>% 
  filter(transfer == 'trasfer_in',Csv %in% c("ch_saft.csv","ch_saft_2.csv"),!is.na(From_name)) %>% 
  group_by(Name,Block_time,From,To,From_name) %>% summarise(Value = sum(Value))



### Let's investigate for some hypothetical cex address

top_n <- transfer_raw_step2 %>% filter(transfer == 'transfer_out') %>% group_by(To) %>% summarise(N = n()) %>% arrange(-N)

top_value <- transfer_raw_step2 %>% filter(transfer == 'transfer_out') %>% group_by(To) %>% summarise(Value = -sum(Value)) %>% arrange(-Value)

top_n_addresses <- as.character(paste0("",top_n$To,""))

df_top <- data.frame(To = c(), n = c())

for (i in top_n_addresses){
a <- transfer_raw_step2 %>% filter(To == i, transfer == 'transfer_out')

a <- data.frame(To=i,n=length(unique(a$From))) 
df_top <- rbind(df_top,a)
}

top_all <- top_n %>% left_join(top_value, by= "To") %>% left_join(df_top,by="To") 

top_all_filter <- top_all %>% filter(N>=10,n>=4,Value>=10000)


### trasfer out second step

destinations <- data.frame(To = c('4brTc3oXsaQb8Z2sKqz4FN5NJtCPAENX6quZaoZrfxtKWqd7',
                                  '4eS7ZryvxRbnCtYP1sJMAwKwVKUujqBCJw2SgGgXjVWAHvs7',
                                  '4dCsyMo3ehGN5ePH2KpwZqStfDTBYaVewkWLtTXbT7wFKXjr',
                                  '4beFA2VcN3FKZ68RtyDLzBTrJYVWRzjYmmc5x5QHvba2q8xt',
                                  "4dJdUGMwKLFmQqHD1zyjpeHUgKWN6meZCUURw1KSmCaefJgg"),
                           Destination = c('Cex',
                                           'Cex',
                                           "Cex",
                                           "Cex",
                                           "Cex"))


transfer_step2 <- transfer_raw_step2 %>% left_join(destinations,by="To") %>% 
  filter(!is.na(Destination),transfer == 'transfer_out') %>% 
  select(Block_time,Block_num,From,To,Value,Destination) %>% rename("Block_num_2" = Block_num,"To" = From,"To_2" = To, "Value_2" = Value)


step_1 <- transfers %>% filter(transfer == 'transfer_out',Csv %in% c("ch_saft.csv","ch_saft_2.csv")) %>% 
  group_by(Name,Block_time,From,To) %>% summarise(Value = sum(Value))

step_2 <- transfer_step2 %>% group_by(Block_time,To,To_2,Destination) %>% summarise(Value_2 = sum(Value_2))

transfers_all_time <- step_1 %>% left_join(step_2, by = c("Block_time","To")) %>% filter(!is.na(Destination)) %>% arrange(Name,Block_time,From)


step_1 <- transfers_all_time %>% ungroup() %>%
  select(Name,From,To,Block_time,Value) %>% distinct() %>%
  group_by(Name,From,To) %>% summarise(Value = sum(Value))

step_2 <- transfers_all_time %>% ungroup() %>% 
  select(To,To_2,Destination,Block_time,Value_2) %>% 
  distinct() %>% 
  group_by(To,To_2,Destination) %>% summarise(Value_2 = sum(Value_2))

transfers_all <- step_1 %>% left_join(step_2, by = c("To")) %>% 
  arrange(Name,From) %>% 
  rename("Destination_2" = Destination) %>%
  mutate(Destination = case_when(Destination_2 == 'Cex' ~ 'Deposit Address',
                                 To == '4dpEcgqFor2TJw9uWSjx2JpjkNmTic2UjJAK1j9fRtcTUoRu' ~ "Ethereum Bridge",
                                 To == '4cQ3MSd7cBrvhGtAwYUo2DxbX1bZooMjrWTKCjNfwURcfQd5' ~ 'Tinlake - 1754 Factory, LLC (Bling Series 1)')) %>%
  select("Name","From","To","Destination","Value","To_2","Destination_2","Value_2","Destination" )

### bind all

ch_saft <- rbind(transfers_destination,transfers_all) %>% arrange(Name,From) 


ch_saft_addresses <- transfers %>% filter(Csv %in% c("ch_saft.csv","ch_saft_2.csv")) %>% select(Name,Address) %>% distinct()


ch_saft_all <- pd_balance %>% filter(Address %in% ch_saft_addresses$Address) %>% select(Name,Address,Private_deal) %>% left_join(ch_saft,by = c("Name","Address"="From")) %>% arrange(Name,Address) 

write.csv(ch_saft_all,"C:\\Users\\Alex\\Desktop\\Crypto Jobs\\CFG ICO\\Output\\ch_saft_all.csv")
#saveRDS(top_all,"C:\\Users\\aless\\Desktop\\CFG ICO\\Output\\top_all.RData")
