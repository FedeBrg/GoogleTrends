import pandas as pd
from pytrends.request import TrendReq

pytrends = TrendReq(hl='en-US', tz=360)


directory = "."
filename = "diarios.csv"
kw_file = pd.read_csv(directory+filename)
kw_list = kw_file["Keywords"].values.tolist()

print(kw_list)


def gtrends_overtime(full_list, key_ref, save_name="", directory="", category=0, time='all', loc=''):
    #iterate every 4 item in list plus a keyword reference as the relative comparison
    i = 0
    while i < len(kw_list):
        l = kw_list[i:(i+4)]
        l.append(key_ref)
        print(l)
        pytrends.build_payload(l, timeframe=time, geo=loc)
        df_time = pytrends.interest_over_time()
        df_time.reset_index(inplace=True)
        df_time_name = "gtrends_overtime"+str(save_name)+str((i+4)//4)+".csv"
        df_time.to_csv(directory+df_time_name, index = False)
        i += 4

gtrends_overtime(kw_list, 'Ámbito', "_argentina_", directory, category=0, time='all', loc='AR')


def gtrends_region(kw_list, key_ref, save_name="", directory="",
                   category=0, time='all', loc='', res='COUNTRY'):
    #iterate every 4 item in list plus a keyword reference as the relative comparison
    i = 0
    while i < len(kw_list):
        l = kw_list[i:(i+4)]
        l.append(key_ref)
        pytrends.build_payload(l, cat=category, timeframe=time, geo=loc, gprop='')
        # resolution can be set to 'REGION' when loc is set to specific country, e.g. 'US'
        df_region = pytrends.interest_by_region(resolution=res, inc_low_vol=True, inc_geo_code=False)
        df_region.reset_index(inplace=True)
        df_region_name = "gtrends_region"+str(save_name)+str((i+4)//4)+".csv"
        df_region.to_csv(df_region_name, index = False)
        i += 4

# gtrends_region(kw_list, 'Noodles', "_worldwide_", directory, category=71, time='all', loc='', res='COUNTRY')

# gtrends_region(kw_list, 'Noodles', "_US_", directory, category=71, time='all', loc='US', res='REGION')

# gtrends_region(kw_list, 'Noodles', "_ID_", directory, category=71, time='all', loc='ID', res='REGION')

def combine_wbase(directory, base_name, n_file, filename):
    df1 = pd.read_csv(directory+base_name+str(1)+".csv")
    for i in range(n_file-1):
        df2 = pd.read_csv(directory+base_name+str(i+2)+".csv")
        df1 = pd.concat([df1, df2], axis=1, sort=False)
    df_name = filename
    # Saving the merged file or you can simply return the dataframe
    df1.to_csv(df_name, index = False)
    #return df


combined = combine_wbase(directory, "gtrends_overtime_argentina_", 3, "gtrends_overtime_worlwide_merged.csv")
combined = pd.read_csv("gtrends_overtime_worlwide_merged.csv")
print(combined)


def partial(df, n_file):
    for i in range(n_file-1):
        df = df.drop(columns="isPartial."+str(i+1)+"")
    if df.isPartial.tail(1).bool() == True:
        df = df.drop(df.isPartial.tail(1).index, axis=0)
    df = df.drop(columns="isPartial")
    return df

combined = partial(combined,3)
combined

def normalise(df, n_file, key_ref, col='date'):
    li = []
    # Checking the relative popularity between comparisons
    for i in range(n_file-1):    
        df = df.drop(columns=col+"."+str(i+1)+"")
        # Appending the list if relative popularity of the keyword reference is different
        if df[key_ref+"."+str(i+1)+""][0] == df[key_ref][0]:
            pass
        else:
            li.append(i+1)
    
    # Normalizing relative popularity when the relative popularity of the keyword reference is different         
    for l in li:
        k = df.columns.get_loc(key_ref+"."+str(l)+"")
        for n in range(len(df.index)):
            # Computing relative popularity by normalizing according to the reference
            if df.iloc[n,(k)] > 0:
                for m in range(5):
                    df.iloc[n,(k-4+m)] = (df.iloc[n,(k-4+m)] * (df[key_ref][n]/df.iloc[n,(k)]))
            else:
                for m in range(5):
                    df.iloc[n,(k-4+m)] = (df.iloc[n,(k-4+m)] * (df[key_ref][n]/0.01))
    return df



normalised = normalise(combined, n_file=3, key_ref="Ámbito", col='date')
normalised


def tidy(df, n_file, key_ref, kw_file, col='date'):
    for i in range(n_file-1):    
        df = df.drop(columns=key_ref+"."+str(i+1)+"")
    df=pd.melt(df,id_vars=[col],var_name='Keywords', value_name='RelativePopularity')
    df = df.merge(kw_file, on="Keywords")
    return df



overtime = tidy(normalised, 3, "Ámbito", kw_file, col='date')
print(overtime)

overtime.to_csv("gtrends_diarios_overtime.csv", index = False)

