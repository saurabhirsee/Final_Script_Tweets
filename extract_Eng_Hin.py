import tweepy
import pandas as pd
import numpy as np
import regex

filename4 = "Eng_Hin_SP.csv"

file_in_use = filename4
keywords = pd.read_csv('toExtract/'+file_in_use)

creds = pd.read_csv('creds.csv')
cred_no = 0

def cleanTxt(text):
    text = regex.sub('@[A-Za-z0-9_]+', '',text) # Remove @mentions
    text = regex.sub('#[A-Za-z0-9_]+','',text) # Remove the '#' symbol
    text = regex.sub('RT[\s]','',text) # Remove RT
    text = regex.sub('https?://\S+', '', text) # Remove hyper link
    text = regex.sub('[":;“”)(]', ' ', text) # Remove the other special characters
    text = regex.sub('&amp', 'and', text) 
    text = regex.sub('\n',' ', text) 
    return text

def deEmojify(text):
    regrex_pattern = regex.compile(pattern = "["
                           u"\U0001F600-\U0001F64F"  # emoticons
                           u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                           u"\U0001F680-\U0001F6FF"  # transport & map symbols
                           u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                           u"\U00002702-\U000027B0"
                           u"\U000024C2-\U0001F251"
                           "]+", flags=regex.UNICODE)
    return regrex_pattern.sub(r'',text)

def changeToLower(text):
    return text.lower()

def extraction(search_word, ext_count, cred_no):
    consumerKey = creds.loc[cred_no]['consumerKey']
    consumerSecret = creds.loc[cred_no]['consumerSecret']
    accessToken = creds.loc[cred_no]['accessToken']
    accessTokenSecret = creds.loc[cred_no]['accessTokenSecret']
    
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    authenticate.set_access_token(accessToken,accessTokenSecret)
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    
    tweets = tweepy.Cursor(api.search, q=search_word+' -filter:retweets', 
                       result_type="recent", lang="en", tweet_mode="extended").items(ext_count)
    df = pd.DataFrame( [x.full_text for x in tweets], columns=['Tweets'])
    
    df['Tweets'] = df['Tweets'].apply(cleanTxt)
    df['Tweets'] = df['Tweets'].apply(deEmojify)
    df['Tweets'] = df['Tweets'].apply(changeToLower)
    
    return df

# Script Written by Saurabh

master = pd.DataFrame({"keys": [], "word1": [], "word2": [], "Tweets": []})

def updateMaster(masterfile, df, keys, word1, word2, master):
    for i in range(0,df.shape[0]):
        master = master.append({"keys":keys, "word1":word1, "word2":word2,
                               "Tweets":df.loc[i]['Tweets']}, 
                               ignore_index=True)
    
    master.to_csv('masterData/'+masterfile, index=False)
    
    return df.shape[0], master

masterfile4 = "master_Eng_Hin.csv"

masterfile = masterfile4

extraction_count = 100

for i in range(0,keywords.shape[0]):
    keys = keywords.loc[i]['keys']
    word1 = keywords.loc[i]['word1']
    word2 = keywords.loc[i]['word2']
    
    inc_count, master = updateMaster(masterfile, 
                             extraction(keys, extraction_count, cred_no),
                             keys,
                             word1,
                             word2, 
                             master)
    
    cred_no = (cred_no+1)%141
    
    print('Added', inc_count, 'samples of', keys, 'using cred no.', cred_no)