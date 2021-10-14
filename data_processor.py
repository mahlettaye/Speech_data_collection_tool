import pandas as pd
import re
import os   

class DataProcessor():
    
    def __init__(self, filename):
        self.filename=filename
    
    def read_data (filename):
        try:
            data=pd.read_csv(filename)
        except FileNotFoundError as e:
            print (e)
        return data
    
    def data_selector(df):
        """To sub set dataframe"""
        df=df [['headline','article']]
        return df

    def normalize_char_level_missmatch(input_token):
        """Function that accepts tokens and lablize token to 
        remove character level missmatch """
        rep1=re.sub('[ሃኅኃሐሓኻ]','ሀ',input_token)
        rep2=re.sub('[ሑኁዅ]','ሁ',rep1)
        rep3=re.sub('[ኂሒኺ]','ሂ',rep2)
        rep4=re.sub('[ኌሔዄ]','ሄ',rep3)
        rep5=re.sub('[ሕኅ]','ህ',rep4)
        rep6=re.sub('[ኆሖኾ]','ሆ',rep5)
        rep7=re.sub('[ሠ]','ሰ',rep6)
        rep8=re.sub('[ሡ]','ሱ',rep7)
        rep9=re.sub('[ሢ]','ሲ',rep8)
        rep10=re.sub('[ሣ]','ሳ',rep9)
        rep11=re.sub('[ሤ]','ሴ',rep10)
        rep12=re.sub('[ሥ]','ስ',rep11)
        rep13=re.sub('[ሦ]','ሶ',rep12)
        rep14=re.sub('[ዓኣዐ]','አ',rep13)
        rep15=re.sub('[ዑ]','ኡ',rep14)
        rep16=re.sub('[ዒ]','ኢ',rep15)
        rep17=re.sub('[ዔ]','ኤ',rep16)
        rep18=re.sub('[ዕ]','እ',rep17)
        rep19=re.sub('[ዖ]','ኦ',rep18)
        rep20=re.sub('[ጸ]','ፀ',rep19)
        rep21=re.sub('[ጹ]','ፁ',rep20)
        rep22=re.sub('[ጺ]','ፂ',rep21)
        rep23=re.sub('[ጻ]','ፃ',rep22)
        rep24=re.sub('[ጼ]','ፄ',rep23)
        rep25=re.sub('[ጽ]','ፅ',rep24)
        rep26=re.sub('[ጾ]','ፆ',rep25)
        #Normalizing words with Labialized Amharic characters such as በልቱዋል or  በልቱአል to  በልቷል  
        rep27=re.sub('(ሉ[ዋአ])','ሏ',rep26)
        rep28=re.sub('(ሙ[ዋአ])','ሟ',rep27)
        rep29=re.sub('(ቱ[ዋአ])','ቷ',rep28)
        rep30=re.sub('(ሩ[ዋአ])','ሯ',rep29)
        rep31=re.sub('(ሱ[ዋአ])','ሷ',rep30)
        rep32=re.sub('(ሹ[ዋአ])','ሿ',rep31)
        rep33=re.sub('(ቁ[ዋአ])','ቋ',rep32)
        rep34=re.sub('(ቡ[ዋአ])','ቧ',rep33)
        rep35=re.sub('(ቹ[ዋአ])','ቿ',rep34)
        rep36=re.sub('(ሁ[ዋአ])','ኋ',rep35)
        rep37=re.sub('(ኑ[ዋአ])','ኗ',rep36)
        rep38=re.sub('(ኙ[ዋአ])','ኟ',rep37)
        rep39=re.sub('(ኩ[ዋአ])','ኳ',rep38)
        rep40=re.sub('(ዙ[ዋአ])','ዟ',rep39)
        rep41=re.sub('(ጉ[ዋአ])','ጓ',rep40)
        rep42=re.sub('(ደ[ዋአ])','ዷ',rep41)
        rep43=re.sub('(ጡ[ዋአ])','ጧ',rep42)
        rep44=re.sub('(ጩ[ዋአ])','ጯ',rep43)
        rep45=re.sub('(ጹ[ዋአ])','ጿ',rep44)
        rep46=re.sub('(ፉ[ዋአ])','ፏ',rep45)
        rep47=re.sub('[ቊ]','ቁ',rep46) #ቁ can be written as ቊ
        rep48=re.sub('[ኵ]','ኩ',rep47) #ኩ can be also written as ኵ  
        
        return rep48

    def remove_punc_and_special_chars(text): 
  
        normalized_text = re.sub('[\–\►\!\@\#\$\%\^\«\»\&\*\(\)\…\[\]\{\}\;\“\”\›\’\‘\"\'\:\,\.\‹\/\<\>\?\\\\|\`\´\~\-\=\+\፡\።\፤\;\፦\፥\፧\፨\፠\፣]', '',text) 
        return normalized_text

    def normalize_and_append_to_outfile(_sentence,counter,file_name):  
        count=counter
        try:
            with open(file_name,'a+',encoding='utf8') as processed_text:
                for sen in _sentence:
                    tokens=re.compile('\s+').split(sen) #word level
                    normalized_sentence=''
                    if len(tokens)<=7:
                        print("Below minimum words in sentence")
                    else:
                        for token in tokens:                                           
                            normalized_token=DataProcessor.normalize_char_level_missmatch(token)                 
                            processed=DataProcessor.remove_punc_and_special_chars(normalized_token)                 
                            normalized_sentence+=processed+' ' ##merge normalized tokens to a sentence
                        processed_text.write(normalized_sentence+"(sentence "+str(count)+")"+'\n') #append normalized sentence to a file
                        count=count+1
                processed_text.close()
        except FileNotFoundError as e:
            print(e)
        return count 

    def normalize_and_append_headline_to_outfile(_sentence,counter,file_name):
        try:        
            with open(file_name,'a+',encoding='utf8') as processed_text:
                tokens=re.compile('\s+').split(_sentence) #word level
                normalized_sentence=''
                if len(tokens)<=7:
                        print("Below minimum words in sentence")
                else:
                    for token in tokens:                                           
                        normalized_token=DataProcessor.normalize_char_level_missmatch(token)
                        processed=DataProcessor.remove_punc_and_special_chars(normalized_token)                 
                        normalized_sentence+=processed+' ' ##merge normalized tokens to a sentence
                    processed_text.write(normalized_sentence+"(sentence "+str(counter)+")"+'\n') #append normalized sentence to a file
                    count=counter+1
                    
                    processed_text.close()
        except FileNotFoundError as e:
            print (e)

    def preprocess_data (self):
        data=DataProcessor.read_data(self.filename)
        df=data[['headline','article']]
        file_name="Clean_Amharic.txt"
        counter=1
        try: 
            for data in df['article']:
                sentences=re.compile('[!?።\፡\፡]+').split(data)
                counter=DataProcessor.normalize_and_append_to_outfile(sentences,counter,file_name)
        except Exception as e:
            print (e)
        try:
            for data in df['headline']:
                sentence= str(data)
                DataProcessor.normalize_and_append_headline_to_outfile(sentence,counter,file_name)
                counter=counter+1
        except Exception as e:
            print (e)
            

if __name__=="__main__":
    processor_obj= DataProcessor("Amharic News Dataset.csv")
    processor_obj.preprocess_data()
    